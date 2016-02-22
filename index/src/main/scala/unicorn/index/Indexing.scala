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

import unicorn.bigtable.ScanFilter.BasicExpression
import unicorn.bigtable._
import unicorn.index.IndexType._
import unicorn.json._
import unicorn.util._

/** Index manager. A BigTable with Indexing trait will automatically maintain
  * secondary index.
  *
  * Use the stackable trait pattern by "abstract override".
  * See details at http://www.artima.com/scalazine/articles/stackable_trait_pattern.html
  *
  * @author Haifeng Li
  */
trait Indexing extends BigTable with RowScan with FilterScan with Rollback with Counter {
  val db: Database[IndexableTable]

  val indexTableName = IndexTableNamePrefix + name

  if (!db.tableExists(indexTableName)) {
    db.createTable(indexTableName, IndexColumnFamilies: _*)
  }

  val indexBuilder = IndexBuilder(db(indexTableName))
  var tenant: Option[Array[Byte]] = None

  /** Closes the base table and the index table */
  abstract override def close(): Unit = {
    super.close
    indexBuilder.close
  }

  /** Creates an index. */
  def createIndex(indexName: String, family: String, columns: Seq[IndexColumn], indexType: IndexType = IndexType.Default): Unit = {
    val index = indexBuilder.createIndex(indexName, family, columns, indexType)

    // Build the index
    val scanner = super.scanAll(family, columns.map(_.qualifier): _*)
    indexBuilder.buildIndex(index, scanner)
  }

  /** Drops an index. */
  def dropIndex(indexName: String): Unit = {
    indexBuilder.dropIndex(indexName)
  }

  abstract override def update(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit = {
    put(row, family, column, value, 0L)
  }

  abstract override def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(family, column)

    if (indices.isEmpty) {
      super.put(row, family, column, value, timestamp)
    } else {
      val oldValues = get(row, family, qualifiers: _*)
      super.put(row, family, column, value, timestamp)
      val newValues = get(row, family, qualifiers: _*)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(family, oldValues))
      indexBuilder.put(tenant, indices, row, ColumnMap(family, newValues))
    }
  }

  abstract override def put(row: ByteArray, family: String, columns: Column*): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(family, columns.map(_.qualifier): _*)

    if (indices.isEmpty) {
      super.put(row, family, columns: _*)
    } else {
      val oldValues = get(row, family, qualifiers: _*)
      super.put(row, family, columns: _*)
      val newValues = get(row, family, qualifiers: _*)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(family, oldValues))
      indexBuilder.put(tenant, indices, row, ColumnMap(family, newValues))
    }
  }

  abstract override def put(row: ByteArray, families: Seq[ColumnFamily]): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(families.map { case ColumnFamily(family, columns) =>
      (family, columns.map(_.qualifier))
    })

    if (indices.isEmpty) {
      super.put(row, families)
    } else {
      val oldValues = get(row, qualifiers)
      super.put(row, families)
      val newValues = get(row, qualifiers)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(oldValues))
      indexBuilder.put(tenant, indices, row, ColumnMap(newValues))
    }
  }

  abstract override def putBatch(rows: Row*): Unit = {
    rows.foreach { case Row(row, families) =>
      put(row, families)
    }
  }

  abstract override def delete(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(family, columns: _*)

    if (indices.isEmpty) {
      super.delete(row, family, columns: _*)
    } else {
      val oldValues = get(row, family, qualifiers: _*)
      super.delete(row, family, columns: _*)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(family, oldValues))
    }
  }

  abstract override def delete(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(families)

    if (indices.isEmpty) {
      super.delete(row, families)
    } else {
      val oldValues = get(row, qualifiers)
      super.delete(row, families)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(oldValues))
    }
  }

  abstract override def deleteBatch(rows: Seq[ByteArray]): Unit = {
    rows.foreach { row =>
      delete(row)
    }
  }

  abstract override def rollback(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(family, columns: _*)

    if (indices.isEmpty) {
      super.rollback(row, family, columns: _*)
    } else {
      val oldValues = get(row, family, qualifiers: _*)
      super.rollback(row, family, columns: _*)
      val newValues = get(row, family, qualifiers: _*)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(family, oldValues))
      indexBuilder.put(tenant, indices, row, ColumnMap(family, newValues))
    }
  }

  abstract override def rollback(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit = {
    val (indices, qualifiers) = indexBuilder.indexedColumns(families)

    if (indices.isEmpty) {
      super.rollback(row, families)
    } else {
      val oldValues = get(row, qualifiers)
      super.rollback(row, families)
      val newValues = get(row, qualifiers)

      if (!oldValues.isEmpty) indexBuilder.delete(tenant, indices, row, ColumnMap(oldValues))
      indexBuilder.put(tenant, indices, row, ColumnMap(newValues))
    }
  }

  abstract override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    super.filterScan(filter, startRow, stopRow, families)
  }

  abstract override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*): RowScanner = {
    super.filterScan(filter, startRow, stopRow, family, columns: _*)
  }

  /*
    abstract override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*): RowScanner = {
      val index = getIndex(filter)
      if (index.isEmpty) super.filterScan(filter, startRow, stopRow, family, columns: _*)
      else {
        val (idx, value) = index.get
        val prefix = idx.codec.prefix(tenant, value)
        indexBuilder.indexTable.scanPrefix(prefix, IndexColumnFamily)
      }
    }

    private def getIndex(filter: ScanFilter.Expression): Option[(Index, ByteArray)] = {
      import ScanFilter._

      filter match {
        case basic: BasicExpression => getIndex(basic)
        case And(list) => list.map(getIndex(_)).filter(_.isDefined).headOption.map(_.get) // TODO choose most effective index
        case Or(list) => None // TODO query rewrite and pick one index
      }
    }

    private def getIndex(filter: BasicExpression): Option[(Index, ByteArray)] = {
      val (_, family, column, value) = filter
      val indices = indexBuilder.indices.filter { index =>
        index.family == family && index.columns.head.qualifier == column
      }

      val single = indices.filter(_.columns.size == 1)
      val index = if (single.isEmpty) indices.headOption else Some(single.head)
      index.map((_, value))
    }
  }

  class IndexRowScanner(table: BigTable with RowScan with FilterScan, scanner: RowScanner, filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*) extends RowScanner {
    val rows = scanner.flatMap { row =>
      if (row.families.isEmpty) Seq.empty
      else {
        row.families.head.columns.map(_.qualifier).filter { key =>
          key >= startRow && key < stopRow
        }
      }
    }

    private def getNextRow: Option[Row] = {
      if (!rows.hasNext) return None
      val key = rows.next
      table.filterGet(filter, key, family, columns: _*).map { columns =>
        Row(key, Seq(ColumnFamily(family, columns)))
      }
    }

    private var _row = getNextRow

    override def close: Unit = ()

    override def hasNext: Boolean = _row.isDefined

    override def next: Row = {
      val row = _row.get
      _row = getNextRow
      row
    }
    */
}

