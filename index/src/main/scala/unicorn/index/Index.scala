/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn.index

import unicorn.json._
import unicorn.util._

/**
 * For a composite index of multiple columns, sort order is only
 * meaningful for fixed-width columns (except the last one, which
 * can be of variable length) in case of range search.
 * Otherwise, it can only be used for equality queries.
 */
object IndexSortOrder extends Enumeration {
  type IndexSortOrder = Value

  /**
   * The ascending sort order naturally utilizes the
   * fact that BigTable row keys are sorted ascending.
   */
  val Ascending = Value

  /**
   * We flip every bit of values as the row keys of index table.
   */
  val Descending = Value

  /**
   * Hashed indexes compute a hash of the value of a field
   * and index the hashed value. These indexes permit only
   * equality queries. Internally, this is based on MD5 hash
   * function, which computes a 16-byte value. Therefore, this
   * is mostly useful for long byte strings/BLOB. If a column
   * in a composite index is hashed, the whole index will be hashed.
   * Hashed index doesn't support unique constraint.
   */
  val Hashed = Value

  /**
   * Text indexes are for full text search by keywords.
   */
  val Text = Value
}

import IndexSortOrder._

/** A column in an index */
case class IndexColumn(qualifier: Array[Byte], order: IndexSortOrder = Ascending) {
  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[IndexColumn]) return false

    val that = o.asInstanceOf[IndexColumn]
    if (this.qualifier.size != that.qualifier.size ||
        this.order != that.order) return false

    compareByteArray(qualifier, that.qualifier) == 0
  }

  override def hashCode: Int = {
    var hash = 7
    qualifier.foreach { i => hash = 31 * hash + i }
    hash = 31 * hash + order.id
    hash
  }

  override def toString: String = {
    val hex = bytes2Hex(qualifier)
    s"""($hex, $order)"""
  }
}

/**
 * Secondary index. Index columns must belong to the same column family.
 * In HBase and Accumulo, the row key is sorted.
 * The secondary index implementation uses this fact and reverts
 * the key and value in the index table.
 * If columns has more than one elements, this is a composite index.
 * The order of columns is important as it determine the row key
 * of index table. Only the leading columns of row key can be used for
 * index scan in case that partial index columns are used in query.
 * If unique is true, the column cannot have duplicated values.
 * The index row key may have (stackable) prefix (e.g. tenant id).
 *
 * @author Haifeng Li
 */
case class Index(name: String, family: String, columns: Seq[IndexColumn], unique: Boolean = false, prefix: Seq[IndexRowKeyPrefix] = Seq.empty) {
  require(columns.size > 0)

  val isHashIndex = columns.exists(_.order == Hashed)
  val isTextIndex = columns.exists(_.order == Text)

  val coveredColumns = columns.map { column => new ByteArray(column.qualifier) }.toSet

  val indexTableName = IndexTableNamePrefix + name

  /**
   * Returns true if the index covers some of given columns.
   */
  def cover(family: String, columns: ByteArray*): Boolean = {
    this.family == family && !(coveredColumns & columns.toSet).isEmpty
  }

  /**
   * If the index doesn't cover any columns to update, return empty set.
   * Otherwise, returns the covered columns of this index.
   */
  def findCoveredColumns(family: String, columns: ByteArray*): Set[ByteArray] = {
    if (isTextIndex) coveredColumns & columns.toSet
    else if (cover(family, columns: _*)) coveredColumns
    else Set.empty
  }

  /** Returns true if both indices cover the same column set (and same order) */
  def coverSameColumns(that: Index): Boolean = {
    columns == that.columns
  }

  /** Returns the prefixed index table row key */
  def prefixedIndexRowKey(indexTableRowKey: ByteArray, baseTableRowKey: ByteArray) = {
    prefix.foldRight(indexTableRowKey){ (prefix, value) => prefix(this, baseTableRowKey).bytes ++ value.bytes }
  }

  def toJson: JsValue = {
    JsObject(
      "name" -> name,
      "family" -> family,
      "columns" -> columns.map { column =>
        JsObject(
          "qualifier" -> JsBinary(column.qualifier),
          "order" -> column.order.toString
        )
      },
      "unique" -> unique,
      "prefix" -> prefix.map(_.toString)
    )
  }
}

object Index {
  def apply(js: JsValue) = {
    val name: String = js.name
    val family: String = js.family
    val columns = js.columns match {
      case JsArray(columns) => columns.map { column =>
        val qualifier: Array[Byte] = column.qualifier
        val order: String = column.order
        IndexColumn(qualifier, IndexSortOrder.withName(order))
      }
      case _ => throw new IllegalStateException("columns is not JsArray")
    }

    val unique: Boolean = js.unique

    val tenantIdPrefixPattern = """tenant\((\d+)\)""".r
    val indexIdPrefixPattern = """index\((\d+)\)""".r
    val prefixArray = js.prefix.asInstanceOf[JsArray]
    val prefix = prefixArray.map ( _ match {
      case JsString(tenantIdPrefixPattern(size)) => new TenantIdPrefix(size.toInt)
      case JsString(indexIdPrefixPattern(id)) => new TenantIdPrefix(id.toInt)
      case _ => throw new IllegalArgumentException("Unsupported index prefix")
    }).toSeq

    new Index(name, family, columns, unique, prefix)
  }
}