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

import unicorn.bigtable.{Column, ColumnFamily, Row}
import unicorn.util.ByteArray

/**
 * Index builder. Because of the sparse natural of BigTable, all indices are sparse.
 * That is, omit references to rows that do not include the indexed field.
 *
 * @author Haifeng Li
 */
case class IndexBuilder(index: Index, indexTable: IndexableTable) {
  val codec = if (index.indexType == IndexType.Hashed) new HashIndexCodec(index)
    else if (index.indexType == IndexType.Text) new TextIndexCodec(index)
    else if (index.columns.size == 1) new SingleColumnIndexCodec(index)
    else new CompositeIndexCodec(index)

  def close: Unit = indexTable.close

  def insertIndex(row: ByteArray, map: RowMap): Unit = {
    val cells = codec(row, map)
    cells.foreach { cell =>
      indexTable.put(cell.row, cell.family, cell.qualifier, cell.value, cell.timestamp)
    }
  }

  def deleteIndex(row: ByteArray, map: RowMap): Unit = {
    val cells = codec(row, map)
    cells.foreach { cell =>
      indexTable.delete(cell.row, cell.family, cell.qualifier)
    }
  }
}

object RowMap {
  def apply(families: ColumnFamily*): RowMap = {
    families.map { family =>
      val columns = collection.mutable.Map(family.columns.map { column => (ByteArray(column.qualifier), column) }: _*)
      (family.family, columns)
    }.toMap
  }

  def apply(family: String, columns: Column*): RowMap = {
    Map(family -> collection.mutable.Map(columns.map { column => (ByteArray(column.qualifier), column) }: _*))
  }

  def apply(row: Row): RowMap = {
    apply(row.families: _*)
  }
}