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
 * Hash index doesn't support unique constraint.
 *
 * @author Haifeng Li
 */
class HashIndexCodec(index: Index) extends IndexCodec {
  // Hash index doesn't support unique constraint
  require(index.unique == false)

  val suffix = ByteBuffer.allocate(4 * index.columns.size)
  val empty = Array[Byte]()

  override def apply(row: ByteArray, columns: RowMap): Seq[Cell] = {
    suffix.reset
    var timestamp = 0L
    index.columns.foreach { indexColumn =>
      val column = columns.get(index.family).map(_.get(indexColumn.qualifier)).getOrElse(None) match {
        case Some(c) => if (c.timestamp > timestamp) timestamp = c.timestamp; c.value.bytes
        case None => empty
      }
      md5Encoder.update(column)
      suffix.putInt(column.size)
    }

    md5Encoder.update(suffix.array)
    val hash = md5Encoder.digest

    val key = index.prefixedIndexRowKey(hash, row)

    Seq(Cell(key, IndexColumnFamily, row, IndexDummyValue, timestamp))
  }
}
