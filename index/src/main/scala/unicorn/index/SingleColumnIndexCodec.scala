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

import unicorn.bigtable.Cell
import unicorn.index.IndexSortOrder._
import unicorn.util._

/** Calculates the cell in the index table for a single column in the base table.
  *
  * @author Haifeng Li
  */
class SingleColumnIndexCodec(val index: Index) extends IndexCodec {
  require(index.columns.size == 1)

  val indexColumn = index.columns.head

  override def apply(row: ByteArray, columns: ColumnMap): Seq[Cell] = {
    columns.get(index.family).map(_.get(indexColumn.qualifier)).getOrElse(None) match {
      case None => Seq.empty
      case Some(column) =>
        val value = indexColumn.order match {
          case Ascending => column.value
          case Descending => ~column.value
        }

        clear
        buffer.put(value)
        val key: Array[Byte] = buffer

        val (qualifier, indexValue) = index.indexType match {
          case IndexType.Unique => (UniqueIndexColumnQualifier, row)
          case _ => (row, IndexDummyValue)
        }

        Seq(Cell(key, IndexColumnFamily, qualifier, indexValue, column.timestamp))
    }
  }
}
