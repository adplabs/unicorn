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

import org.apache.hadoop.io.Text
import unicorn.bigtable._

/**
 * Secondary index.
 * 
 * @author Haifeng Li
 */
trait Index {
  import IndexSortOrder._

  case class IndexColumn(family: Array[Byte], qualifier: Array[Byte], order: IndexSortOrder)

  /**
   * If columns has more than one elements, this is a composite index.
   * If unique is true, this index can be used for unique constriant.
   */
  case class IndexMeta(indexTable: BigTable, columns: Seq[IndexColumn], unique: Boolean = false)

  /** Base table */
  val baseTable: BigTable with RowScan
  /** Index tables. A column may appears in multiple indices (compound index) */
  val indexTables = collection.mutable.Map[(Text, Text), IndexMeta]()

  /** Close the base table and the index table */
  def close: Unit = {
    baseTable.close
    indexTables.foreach { case (_, table) => table.close }
  }

  def createIndex(family: Array[Byte], column: Array[Byte]): Unit = {

  }

  def dropIndex(family: Array[Byte], column: Array[Byte]): Unit = {

  }
}

object IndexSortOrder extends Enumeration {
  type IndexSortOrder = Value

  /**
   * For a composite index of multiple columns, sort order is only
   * meaningful for fixed-width columns (except the last one, which
   * can be of variable length) in case of range search.
   * Otherwise, it can only be used for equality queries.
   */
  val Ascending, Descending = Value
  /**
   * Hashed indexes compute a hash of the value of a field
   * and index the hashed value. These indexes permit only
   * equality queries. Internally, this is based on MD5 hash
   * function, which computes a 16-byte value. Therefore, this
   * is mostly useful for long byte strings/BLOB.
   */
  val Hashed = Value
}

class IndexMeta(db: Database) {
  /** Index meta data table */
  val metaTable = db("unicorn.meta.index")

}