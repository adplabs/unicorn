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

import unicorn.bigtable.{Cell, Column}
import unicorn.util.ByteArray

/**
 * Calculate the cell in the index table for a single column in the base table.
 *
 * @author Haifeng Li
 */
class SingleColumnIndexCodec(index: Index) extends IndexCodec {
  require(index.columns.size == 1)

  val indexColumn = index.columns.head

  override def apply(row: Array[Byte], columns: Map[ByteArray, Map[ByteArray, Column]]): Seq[Cell] = {

    val column = columns.get(indexColumn.family).map(_.get(indexColumn.qualifier)).getOrElse(None) match {
      case Some(c) => c
      case None => throw new IllegalArgumentException("missing covered index column")
    }

    val key = index.prefixedIndexRowKey(column.value, row)

    if (index.unique)
      Seq(Cell(key, IndexMeta.indexColumnFamily, IndexMeta.uniqueIndexColumn, row, column.timestamp))
    else
      Seq(Cell(key, IndexMeta.indexColumnFamily, row, IndexMeta.indexDummyValue, column.timestamp))
  }
}
