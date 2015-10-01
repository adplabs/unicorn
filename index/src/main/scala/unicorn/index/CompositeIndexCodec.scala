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

import java.nio.ByteBuffer
import unicorn.bigtable.{Cell, Column}
import unicorn.util._

/**
 * Calculate the cell in the index table for a composite index (multiple columns) in the base table.
 * The combined size of index columns should be less than 64KB.
 *
 * @author Haifeng Li
 */
class CompositeIndexCodec(index: Index) extends IndexCodec {
  require(index.columns.size > 1)

  val buffer = ByteBuffer.allocate(64 * 1024)
  val suffix = ByteBuffer.allocate(4 * index.columns.size)

  override def apply(row: Array[Byte], columns: Map[ByteArray, Map[ByteArray, Column]]): Seq[Cell] = {
    buffer.reset
    suffix.reset
    var timestamp = 0L
    index.columns.foreach { indexColumn =>
      val column = columns.get(indexColumn.family).map(_.get(indexColumn.qualifier)).getOrElse(None) match {
        case Some(c) => if (c.timestamp > timestamp) timestamp = c.timestamp; c
        case None => throw new IllegalArgumentException("missing covered index column")
      }
      buffer.put(column.value)
      suffix.putInt(column.value.size)
    }

    val key = index.prefixedIndexRowKey(row, buffer.array ++ suffix.array)

    Cell(key, IndexMeta.indexColumnFamily, row, IndexMeta.indexDummyValue, timestamp)

    if (index.unique)
      Seq(Cell(key, IndexMeta.indexColumnFamily, IndexMeta.uniqueIndexColumn, row, timestamp))
    else
      Seq(Cell(key, IndexMeta.indexColumnFamily, row, IndexMeta.indexDummyValue, timestamp))
  }
}
