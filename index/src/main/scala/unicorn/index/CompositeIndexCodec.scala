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

/**
 * Calculate the cell in the index table for a composite index (multiple columns) in the base table.
 * The combined size of index columns should be less than 64KB.
 *
 * In case of range search, composite index can only be used for fixed-width columns
 * (except the last one, which can be of variable length).
 * Otherwise, it can only be used for equality queries. In relational database,
 * this problem is solved by padding varchar to the maximal length. We don't employ
 * this approach because we don't want to limit the data size.
 * It is the user's responsibility to use it correctly.
 *
 * If any field of index is missing we won't index the row.
 *
 * @author Haifeng Li
 */
class CompositeIndexCodec(val index: Index) extends IndexCodec {
  require(index.columns.size > 1)

  override def apply(tenant: Option[Array[Byte]], row: ByteArray, columns: ColumnMap): Seq[Cell] = {
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

    resetBuffer(tenant)
    index.columns.foreach { indexColumn =>
      val column = columns(index.family)(indexColumn.qualifier).value

      indexColumn.order match {
        case Ascending => buffer.put(column)
        case Descending => buffer.put(~column)
      }
    }

    val (qualifier, indexValue) = index.indexType match {
      case IndexType.Unique => (UniqueIndexColumnQualifier, row)
      case _ => (row, IndexDummyValue)
    }

    Seq(Cell(buffer, IndexColumnFamily, qualifier, indexValue, timestamp))
  }
}
