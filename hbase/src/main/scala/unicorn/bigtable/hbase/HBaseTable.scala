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

package unicorn.bigtable.hbase

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Append, Delete, Get, Increment, Put, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter.{ColumnRangeFilter, CompareFilter, FilterList, SingleColumnValueFilter}, CompareFilter.CompareOp
import org.apache.hadoop.hbase.security.visibility.{Authorizations, CellVisibility}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HConstants}
import unicorn.bigtable._, ScanFilter._
import unicorn.util._

/**
 * HBase table adapter.
 * 
 * @author Haifeng Li
 */
class HBaseTable(val db: HBase, val name: String) extends BigTable with RowScan with IntraRowScan with FilterScan with CellLevelSecurity with Appendable with Rollback with Counter {

  val table = db.connection.getTable(TableName.valueOf(name))
  val self = this

  override def close(): Unit = table.close

  override val columnFamilies = table.getTableDescriptor.getColumnFamilies.map(_.getNameAsString).toSeq

  override val startRowKey: ByteArray = HConstants.EMPTY_START_ROW
  override val endRowKey: ByteArray = HConstants.EMPTY_END_ROW

  var cellVisibility: Option[CellVisibility] = None
  var authorizations: Option[Authorizations] = None

  override def setCellVisibility(expression: String): Unit = {
    cellVisibility = Some(new CellVisibility(expression))
  }

  override def getCellVisibility: String = {
    cellVisibility.map(_.getExpression).getOrElse("")
  }

  override def setAuthorizations(labels: String*): Unit = {
    authorizations = Some(new Authorizations(labels: _*))
  }

  override def getAuthorizations: Seq[String] = {
    if (authorizations.isDefined) authorizations.get.getLabels
    else Seq.empty
  }

  override def apply(row: ByteArray, family: String, column: ByteArray): Option[ByteArray] = {
    val get = newGet(row)
    get.addColumn(family, column)
    Option(table.get(get).getValue(family, column)).map(ByteArray(_))
  }

  private def getColumns(get: Get, family: String, columns: Seq[ByteArray]): Unit = {
    if (columns.isEmpty)
      get.addFamily(family)
    else
      columns.foreach { column => get.addColumn(family, column) }
  }

  override def get(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Seq[ColumnFamily] = {
    val get = newGet(row)
    families.foreach { case (family, columns) => getColumns(get, family, columns) }
    HBaseTable.getRow(table.get(get)).families
  }

  override def get(row: ByteArray, family: String, columns: ByteArray*): Seq[Column] = {
    val get = newGet(row)
    getColumns(get, family, columns)

    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) Seq.empty else result.families.head.columns
  }

  override def getBatch(rows: Seq[ByteArray], families: Seq[(String, Seq[ByteArray])]): Seq[Row] = {
    val gets = rows.map { row =>
      val get = newGet(row)
      families.foreach { case (family, columns) => getColumns(get, family, columns) }
      get
    }

    HBaseTable.getRows(table.get(gets))
  }

  override def getBatch(rows: Seq[ByteArray], family: String, columns: ByteArray*): Seq[Row] = {
    val gets = rows.map { row =>
      val get = newGet(row)
      if (columns.isEmpty)
        get.addFamily(family)
      else
        columns.foreach { column => get.addColumn(family, column) }
      get
    }

    HBaseTable.getRows(table.get(gets))
  }

  override def scan(startRow: ByteArray, stopRow: ByteArray, families: Seq[String]): RowScanner = {
    val scan = newScan(startRow, stopRow)
    families.foreach { family => scan.addFamily(family) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def scan(startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*): RowScanner = {
    val scan = newScan(startRow, stopRow)
    if (columns.isEmpty)
      scan.addFamily(family)
    else
      columns.foreach { column => scan.addColumn(family, column) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, families: Seq[String]): RowScanner = {
    val scan = newScan(startRow, stopRow)
    scan.setFilter(hbaseFilter(filter))
    families.foreach { family => scan.addFamily(family) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*): RowScanner = {
    val scan = newScan(startRow, stopRow)
    scan.setFilter(hbaseFilter(filter))
    if (columns.isEmpty)
      scan.addFamily(family)
    else
      columns.foreach { column => scan.addColumn(family, column) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  private def hbaseFilter(filter: ScanFilter.Expression): org.apache.hadoop.hbase.filter.Filter = filter match {
    case BasicExpression(op, family, column, value) =>
      val f = op match {
        case CompareOperator.Equal => new SingleColumnValueFilter(family, column, CompareOp.EQUAL, value)
        case CompareOperator.NotEqual => new SingleColumnValueFilter(family, column, CompareOp.NOT_EQUAL, value)
        case CompareOperator.Greater => new SingleColumnValueFilter(family, column, CompareOp.GREATER, value)
        case CompareOperator.GreaterOrEqual => new SingleColumnValueFilter(family, column, CompareOp.GREATER_OR_EQUAL, value)
        case CompareOperator.Less => new SingleColumnValueFilter(family, column, CompareOp.LESS, value)
        case CompareOperator.LessOrEqual => new SingleColumnValueFilter(family, column, CompareOp.LESS_OR_EQUAL, value)
      }
      f.setFilterIfMissing(true)
      f
    case And(left, right) => new FilterList(FilterList.Operator.MUST_PASS_ALL, hbaseFilter(left), hbaseFilter(right))
    case Or(left, right) => new FilterList(FilterList.Operator.MUST_PASS_ONE, hbaseFilter(left), hbaseFilter(right))
  }

  override def intraRowScan(row: ByteArray, family: String, startColumn: ByteArray, stopColumn: ByteArray): IntraRowScanner = {
    val scan = newIntraRowScan(row, family, startColumn, stopColumn, 100)
    new HBaseColumnScanner(table.getScanner(scan))
  }

  override def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit = {
    val put = newPut(row)
    if (timestamp != 0) put.addColumn(family, column, timestamp, value) else put.addColumn(family, column, value)
    table.put(put)
  }

  override def put(row: ByteArray, family: String, columns: Column*): Unit = {
    val put = newPut(row)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      if (timestamp == 0)
        put.addColumn(family, qualifier, value)
      else
        put.addColumn(family, qualifier, timestamp, value)
    }
    table.put(put)
  }

  override def put(row: ByteArray, families: ColumnFamily*): Unit = {
    val put = newPut(row)
    families.foreach { case ColumnFamily(family, columns) =>
      columns.foreach { case Column(qualifier, value, timestamp) =>
        if (timestamp == 0)
          put.addColumn(family, qualifier, value)
        else
          put.addColumn(family, qualifier, timestamp, value)
      }
    }
    table.put(put)
  }

  override def putBatch(rows: Row*): Unit = {
    val puts = rows.map { case Row(row, families) =>
      val put = newPut(row)
      families.foreach { case ColumnFamily(family, columns) =>
        columns.foreach { case Column(qualifier, value, timestamp) =>
          if (timestamp == 0)
            put.addColumn(family, qualifier, value)
          else
            put.addColumn(family, qualifier, timestamp, value)
        }
      }
      put
    }
    table.put(puts)
  }

  override def delete(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    val deleter = newDelete(row)
    if (columns.isEmpty) deleter.addFamily(family)
    else columns.foreach { column => deleter.addColumns(family, column) }
    table.delete(deleter)
  }

  override def delete(row: ByteArray, families: Seq[String]): Unit = {
    val deleter = newDelete(row)
    families.foreach { family => deleter.addFamily(family) }
    table.delete(deleter)
  }

  override def deleteBatch(rows: Seq[ByteArray], families: Seq[String]): Unit = {
    val deletes = rows.map { row =>
      val deleter = newDelete(row)
      families.foreach { family => deleter.addFamily(family)}
      deleter
    }
    // HTable modifies the input parameter deletes.
    // Make sure we pass in a mutable collection.
    table.delete(deletes.toBuffer)
  }

  override def deleteBatch(rows: Seq[ByteArray], family: String, columns: ByteArray*): Unit = {
    val deletes = rows.map { row =>
      val deleter = newDelete(row)
      if (columns.isEmpty) deleter.addFamily(family)
      else columns.foreach { column => deleter.addColumns(family, column) }
      deleter
    }
    table.delete(deletes)
  }

  override def rollback(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    require(!columns.isEmpty)

    val deleter = newDelete(row)
    columns.foreach { column => deleter.addColumn(family, column) }
    table.delete(deleter)
  }

  override def append(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit = {
    val append = newAppend(row)
    append.add(family, column, value)
    table.append(append)
  }

  override def addCounter(row: ByteArray, family: String, column: ByteArray, value: Long): Unit = {
    val increment = newIncrement(row)
    increment.addColumn(family, column, value)
    table.increment(increment)
  }

  override def getCounter(row: ByteArray, family: String, column: ByteArray): Long = {
    val value = apply(row, family, column)
    value.map { x => Bytes.toLong(x) }.getOrElse(0)
  }

  private def newGet(row: Array[Byte]): Get = {
    val get = new Get(row)
    if (authorizations.isDefined) get.setAuthorizations(authorizations.get)
    get
  }

  /**
   * @param caching Set the number of rows for caching that will be passed to scanners.
   *                Higher caching values will enable faster scanners but will use more memory.
   * @param cacheBlocks When true, default settings of the table and family are used (this will never override
   *                    caching blocks if the block cache is disabled for that family or entirely).
   *                    If false, default settings are overridden and blocks will not be cached
   */
  private def newScan(startRow: Array[Byte], stopRow: Array[Byte], caching: Int = 20, cacheBlocks: Boolean = true): Scan = {
    val scan = new Scan(startRow, stopRow)
    scan.setCacheBlocks(cacheBlocks)
    scan.setCaching(caching)
    if (authorizations.isDefined) scan.setAuthorizations(authorizations.get)
    scan
  }

  /**
   * @param batch Set the maximum number of values to return for each call to next() to
   *              avoid getting all columns for the row.
   */
  private def newIntraRowScan(row: Array[Byte], family: Array[Byte], startColumn: Array[Byte], stopColumn: Array[Byte], batch: Int = 100): Scan = {
    val scan = new Scan(row, row)
    scan.addFamily(family)
    val filter = new ColumnRangeFilter(startColumn, true, stopColumn, true)
    scan.setFilter(filter)
    scan.setBatch(batch)
    if (authorizations.isDefined) scan.setAuthorizations(authorizations.get)
    scan
  }

  private def newPut(row: Array[Byte]): Put = {
    val put = new Put(row)
    if (cellVisibility.isDefined) put.setCellVisibility(cellVisibility.get)
    put
  }

  private def newDelete(row: Array[Byte]): Delete = {
    val del = new Delete(row)
    if (cellVisibility.isDefined) del.setCellVisibility(cellVisibility.get)
    del
  }

  private def newAppend(row: Array[Byte]): Append = {
    val append = new Append(row)
    if (cellVisibility.isDefined) append.setCellVisibility(cellVisibility.get)
    append
  }

  private def newIncrement(row: Array[Byte]): Increment = {
    val increment = new Increment(row)
    if (cellVisibility.isDefined) increment.setCellVisibility(cellVisibility.get)
    increment
  }
}

object HBaseTable {
  def getRow(result: Result): Row = {
    val valueMap = result.getMap
    if (valueMap == null) return Row(result.getRow, Seq.empty)

    val families = valueMap.map { case (family, columns) =>
      val values = columns.flatMap { case (column, ver) =>
        ver.map { case (timestamp, value) =>
          Column(column, value, timestamp)
        }
      }.toSeq
      ColumnFamily(new String(family, utf8), values)
    }.toSeq
    Row(result.getRow, families)
  }

  def getRows(results: Seq[Result]): Seq[Row] = {
    results.map { result =>
      HBaseTable.getRow(result)
    }.filter(!_.families.isEmpty)
  }
}

class HBaseRowScanner(scanner: ResultScanner) extends RowScanner {
  private val iterator = scanner.iterator

  override def close: Unit = scanner.close

  override def hasNext: Boolean = iterator.hasNext

  override def next: Row = {
    HBaseTable.getRow(iterator.next)
  }
}

class HBaseColumnScanner(scanner: ResultScanner) extends IntraRowScanner {
  private val rowIterator = scanner.iterator
  private var cellIterator = if (rowIterator.hasNext) rowIterator.next.listCells.iterator else null

  override def close: Unit = scanner.close

  override def hasNext: Boolean = {
    if (cellIterator == null) return false
    cellIterator.hasNext
  }

  override def next: Column = {
    val cell = cellIterator.next
    if (!cellIterator.hasNext)
      cellIterator = if (rowIterator.hasNext) rowIterator.next.listCells.iterator else null
    Column(CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell), cell.getTimestamp)
  }
}
