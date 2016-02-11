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
import unicorn.bigtable.ColumnFamily
import unicorn.util._

/** For a composite index of multiple columns, sort order is only
  * meaningful for fixed-width columns (except the last one, which
  * can be of variable length) in case of range search.
  * Otherwise, it can only be used for equality queries.
  * It is the user's responsibility to make sure only fixed-width
  * columns are used in a composite index.
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
}

/** Index type. Because of the sparse natural of BigTable, all indices are sparse.
  * That is, we omit references to rows that do not include the indexed field.
  * The work around for users to set JsUndefined to the missing field.
  */
object IndexType extends Enumeration {
  type IndexType = Value

  /** A regular index allowing duplicates, not hashed, not text. */
  val Default = Value

  /** Unique index/constraint, where the column cannot have duplicated values. */
  val Unique = Value

  /** Hashed indexes compute a hash of the value
    * and index the hashed value. These indexes permit only
    * equality queries. Internally, this is based on MD5 hash
    * function, which computes a 16-byte value. Therefore, this
    * is mostly useful for long byte strings/BLOB.
    * Hashed index doesn't support unique constraint.
    */
  val Hashed = Value

  /** Text indexes are for full text search by keywords. */
  val Text = Value
}

import IndexSortOrder.IndexSortOrder
import IndexType.IndexType

/** A column in an index */
case class IndexColumn(qualifier: ByteArray, order: IndexSortOrder = IndexSortOrder.Ascending) {
  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[IndexColumn]) return false

    val that = o.asInstanceOf[IndexColumn]
    this.order == that.order && this.qualifier != that.qualifier
  }

  override def hashCode: Int = {
    31 * qualifier.hashCode + order.id
  }

  override def toString: String = {
    val hex = bytes2Hex(qualifier)
    s"""($hex, $order)"""
  }
}

/** Secondary index. Index columns must belong to the same column family.
  * In HBase and Accumulo, the row key is sorted.
  * The secondary index implementation uses this fact and reverts
  * the key and value in the index table.
  * If columns has more than one elements, this is a composite index.
  * The order of columns is important as it determine the row key
  * of index table. Only the leading columns of row key can be used for
  * index scan in case that partial index columns are used in query.
  * The index row key may have (stackable) prefix (e.g. tenant id).
  * An index is used in the search only it requires Get operations
  * less than a given threshold (100 by default).
  *
  * The id of index is local. That is, the id is unique among all
  * indices of a base table. The indices of different tables may
  * have the same id.
  */
case class Index(id: Int, name: String, family: String, columns: Seq[IndexColumn], indexType: IndexType = IndexType.Default) {
  require(columns.size > 0, "Index columns cannot be empty")

  val codec = IndexCodec(this)
  val qualifiers = columns.map { column => new ByteArray(column.qualifier) }.toSet

  /** If the index doesn't cover any columns to update, return empty set.
    * Otherwise, returns the covered columns of this index. The columns must be
    * in the same column family of index.
    */
  def indexedColumns(columns: ByteArray*): Set[ByteArray] = {
    if (columns.isEmpty) qualifiers
    else {
      if (indexType == IndexType.Text) qualifiers & columns.toSet
      else if (!(qualifiers & columns.toSet).isEmpty) qualifiers
      else Set.empty
    }
  }

  def toJson: JsValue = {
    JsObject(
      "id" -> id,
      "name" -> name,
      "family" -> family,
      "columns" -> columns.map { column =>
        JsObject(
          "qualifier" -> JsBinary(column.qualifier),
          "order" -> column.order.toString
        )
      },
      "indexType" -> indexType.toString
    )
  }
}

object Index {
  def apply(js: JsValue) = {
    val id: Int = js.id
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

    val indexType: String = js.indexType

    new Index(id, name, family, columns, IndexType.withName(indexType))
  }
}