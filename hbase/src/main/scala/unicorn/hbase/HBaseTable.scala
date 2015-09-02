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

package unicorn.hbase

import org.apache.hadoop.hbase.TableName

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.{Append, Delete, Get, Increment, Put, Result, ResultScanner, Scan => HBaseScan}
import org.apache.hadoop.hbase.security.visibility.{Authorizations, CellVisibility}
import org.apache.hadoop.hbase.util.Bytes
import unicorn.bigtable._

/**
 * HBase table adapter.
 * 
 * @author Haifeng Li
 */
class HBaseTable(val db: HBase, val name: String) extends BigTable with Scan with CellLevelSecurity with Appendable with Rollback with Counter {
  val table = db.connection.getTable(TableName.valueOf(name))

  override def close: Unit = table.close

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
    else Seq()
  }

  override def get(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Option[Array[Byte]] = {
    val get = newGet(row)
    get.addColumn(family, column)
    Option(table.get(get).getValue(family, column))
  }

  override def get(row: Array[Byte], families: Seq[Array[Byte]]): Seq[ColumnFamily] = {
    val get = newGet(row)
    families.foreach { family => get.addFamily(family) }
    HBaseTable.getRow(table.get(get)).families
  }

  override def get(row: Array[Byte], family: Array[Byte]): Seq[Column] = {
    val get = newGet(row)
    get.addFamily(family)

    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) Seq() else result.families.head.columns
  }

  override def get(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Column] = {
    val get = newGet(row)
    columns.foreach { column => get.addColumn(family, column) }
    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) Seq() else result.families.head.columns
  }

  override def get(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Seq[Row] = {
    val gets = rows.map { row =>
      val get = newGet(row)
      families.foreach { family => get.addFamily(family) }
      get
    }

    HBaseTable.getRows(table.get(gets))
  }

  override def get(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Row] = {
    val gets = rows.map { row =>
      val get = newGet(row)
      columns.foreach { column => get.addColumn(family, column) }
      get
    }

    HBaseTable.getRows(table.get(gets))
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Seq[Array[Byte]]): Iterator[Row] = {
    val scan = newScan(startRow, stopRow)
    families.foreach { family => scan.addFamily(family) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Iterator[Row] = {
    val scan = newScan(startRow, stopRow)
    columns.foreach { column => scan.addColumn(family, column) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def put(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    val put = newPut(row)
    put.addColumn(family, column, value)
    table.put(put)
  }

  override def put(row: Array[Byte], family: Array[Byte], columns: Column*): Unit = {
    val put = newPut(row)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      if (timestamp == 0)
        put.addColumn(family, qualifier, value)
      else
        put.addColumn(family, qualifier, timestamp, value)
    }
    table.put(put)
  }

  override def put(row: Array[Byte], families: ColumnFamily*): Unit = {
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

  override def put(rows: Row*): Unit = {
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

  override def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit = {
    val deleter = newDelete(row)
    deleter.addColumns(family, column)
    table.delete(deleter)
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    val deleter = newDelete(row)
    if (columns.isEmpty) deleter.addFamily(family)
    else columns.foreach { column => deleter.addColumns(family, column) }
    table.delete(deleter)
  }

  override def delete(row: Array[Byte], families: Seq[Array[Byte]]): Unit = {
    val deleter = newDelete(row)
    families.foreach { family => deleter.addFamily(family) }
    table.delete(deleter)
  }

  override def delete(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Unit = {
    val deletes = rows.map { row =>
      val deleter = newDelete(row)
      families.foreach { family => deleter.addFamily(family)}
      deleter
    }
    // HTable modifies the input parameter deletes.
    // Make sure we pass in a mutable collection.
    table.delete(deletes.toBuffer)
  }

  override def delete(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    val deletes = rows.map { row =>
      val deleter = newDelete(row)
      if (columns.isEmpty) deleter.addFamily(family)
      else columns.foreach { column => deleter.addColumns(family, column) }
      deleter
    }
    table.delete(deletes)
  }

  override def rollback(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit = {
    val deleter = newDelete(row)
    deleter.addColumn(family, column)
    table.delete(deleter)
  }

  override def rollback(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    if (!columns.isEmpty) {
      val deleter = newDelete(row)
      columns.foreach { column => deleter.addColumn(family, column) }
      table.delete(deleter)
    }
  }

  override def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    val append = newAppend(row)
    append.add(family, column, value)
    table.append(append)
  }

  override def addCounter(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit = {
    val increment = newIncrement(row)
    increment.addColumn(family, column, value)
    table.increment(increment)
  }

  override def getCounter(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Long = {
    val value = get(row, family, column)
    value.map { x => Bytes.toLong(x) }.getOrElse(0)
  }

  private def newGet(row: Array[Byte]): Get = {
    val get = new Get(row)
    if (authorizations.isDefined) get.setAuthorizations(authorizations.get)
    get
  }

  private def newScan(startRow: Array[Byte], stopRow: Array[Byte]): HBaseScan = {
    val scan = new HBaseScan(startRow, stopRow)
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
    if (valueMap == null) return Row(result.getRow, Seq())

    val families = valueMap.map { case (family, columns) =>
      val values = columns.flatMap { case (column, ver) =>
        ver.map { case (timestamp, value) =>
          Column(column, value, timestamp)
        }
      }.toSeq
      ColumnFamily(family, values)
    }.toSeq
    Row(result.getRow, families)
  }

  def getRows(results: Seq[Result]): Seq[Row] = {
    results.map { result =>
      HBaseTable.getRow(result)
    }.filter(!_.families.isEmpty)
  }
}

class HBaseRowScanner(scanner: ResultScanner) extends Iterator[Row] {
  private val iterator = scanner.iterator

  def close: Unit = scanner.close

  override def hasNext: Boolean = iterator.hasNext

  override def next: Row = {
    HBaseTable.getRow(iterator.next)
  }
}
