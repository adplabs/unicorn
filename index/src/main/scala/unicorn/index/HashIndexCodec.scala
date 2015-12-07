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

import unicorn.bigtable.Cell
import unicorn.index.IndexSortOrder._
import unicorn.util._

/**
 * Calculate the cell in the index table for a composite index (multiple columns) in the base table.
 * Hash index doesn't support unique constraint.
 *
 * @author Haifeng Li
 */
class HashIndexCodec(index: Index) extends IndexCodec {
  require(index.indexType == IndexType.Hashed)

  val buffer = ByteBuffer.allocate(64 * 1024)

  override def apply(row: ByteArray, columns: RowMap): Seq[Cell] = {
    val hasUndefinedColumn = index.columns.exists { indexColumn =>
      columns.get(index.family).map(_.get(indexColumn.qualifier)).getOrElse(None) match {
        case Some(_) => false
        case None => true
      }
    }

    if (hasUndefinedColumn) return Seq.empty

    val hasZeroTimestamp = index.columns.exists { indexColumn =>
      columns.get(index.family).map(_.get(indexColumn.qualifier)).getOrElse(None) match {
        case Some(c) => c.timestamp == 0L
        case None => false
      }
    }

    val timestamp = if (hasZeroTimestamp) 0L else index.columns.foldLeft(0L) { (b, indexColumn) =>
      val ts = columns(index.family)(indexColumn.qualifier).timestamp
      Math.max(b, ts)
    }

    buffer.reset
    index.columns.foreach { indexColumn =>
      val column = columns(index.family)(indexColumn.qualifier).value

      indexColumn.order match {
        case Ascending => buffer.put(column)
        case Descending => buffer.put(~column)
      }
    }

    val key = index.prefixedIndexRowKey(md5(buffer.array), row)

    val (qualifier: ByteArray, indexValue: ByteArray) = index.indexType match {
      case IndexType.Unique => (UniqueIndexColumnQualifier, row)
      case _ => (row, IndexDummyValue)
    }

    Seq(Cell(key, IndexColumnFamily, qualifier, indexValue, timestamp))
  }
}
