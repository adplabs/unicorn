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

/**
 * Calculate the cell in the index table for a composite index (multiple columns) in the base table.
 * By default index has no row key prefix.
 *
 * @author Haifeng Li
 */
class CompositeIndexCodec(unique: Boolean = false, rowKeyPrefix: IndexRowKeyPrefix = NoRowKeyPrefix) extends IndexCodec {
  override def apply(row: Array[Byte], family: Array[Byte], columns: Seq[Column]): Cell = {
    require(columns.size == 1)
    val column = columns.head
    val key = rowKeyPrefix(row, family, columns) match {
      case null => column.value
      case Array() => column.value
      case prefix => prefix ++ column.value
    }

    if (unique)
      Cell(key, IndexMeta.indexColumnFamily, IndexMeta.uniqueIndexColumn, row, column.timestamp)
    else
      Cell(key, IndexMeta.indexColumnFamily, row, IndexMeta.indexDummyValue, column.timestamp)
  }
}
