/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.hbase

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.security.visibility.{Authorizations, CellVisibility}

/**
 * HBase table adapter.
 * 
 * @author Haifeng Li
 */
class HBaseTable(table: Table) extends unicorn.bigtable.Table {
  var updates = List[Row]()
  var cellVisibility: Option[CellVisibility] = None
  var authorizations: Option[Authorizations] = None

  class HBaseScanner(scanner: ResultScanner) extends Scanner {
    val iter = scanner.iterator
    def close: Unit = scanner.close
    def hasNext: Boolean = iter.hasNext
    def next: Map[Key, Value] = {
      getResults(iter.next)
    }
  }

  override def setCellVisibility(expression: String): Unit = {
    cellVisibility = Some(new CellVisibility(expression))
  }

  override def getCellVisibility: Option[String] = cellVisibility.map(_.getExpression)

  override def setAuthorizations(labels: String*): Unit = {
    authorizations = Some(new Authorizations(labels: _*))
  }

  override def getAuthorizations: Option[Seq[String]] = authorizations.map(_.getLabels)

  override def get(row: Array[Byte], families: Array[Byte]*): Map[Key, Value] = {
    val get = newGet(row)
    families.foreach { family => get.addFamily(family) }
    getResults(table.get(get))
  }

  override def get(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Map[Key, Value] = {
    val get = newGet(row)
    columns.foreach { column => get.addColumn(family, column) }
    getResults(table.get(get))
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Array[Byte]*): Scanner = {
    val scan = newScan(startRow, stopRow)
    families.foreach { family => scan.addFamily(family) }
    new HBaseScanner(table.getScanner(scan))
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Scanner = {
    val scan = newScan(startRow, stopRow)
    columns.foreach { column => scan.addColumn(family, column) }
    new HBaseScanner(table.getScanner(scan))
  }

  override def put(row: Array[Byte], family: Array[Byte], columns: (Array[Byte], Array[Byte])*): Unit = {
    val put = newPut(row)
    columns.foreach { case (column, value) => put.addColumn(family, column, value) }
    table.put(put)
  }

  override def put(values: (Key, Array[Byte])*): Unit = {
    val puts = values.map { case ((row, family, column), value) =>
      val put = newPut(row)
      put.addColumn(family, column, value)
      put
    }
    table.put(puts)
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    val del = newDelete(row)
    columns.foreach { column => del.addColumn(family, column) }
    table.delete(del)
  }

  override def delete(keys: Key*): Unit = {
    val deletes = keys.map { case (row, family, column) =>
      val del = newDelete(row)
      del.addColumn(family, column)
      del
    }
    table.delete(deletes)
  }

  override def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    val append = newAppend(row)
    append.add(family, column, value)
    table.append(append)
  }

  override def increment(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit = {
    val increment = newIncrement(row)
    increment.addColumn(family, column, value)
    table.increment(increment)
  }

  private def newGet(row: Array[Byte]): Get = {
    val get = new Get(row)
    if (authorizations.isDefined) get.setAuthorizations(authorizations.get)
    get
  }

  private def newScan(startRow: Array[Byte], stopRow: Array[Byte]): Scan = {
    val scan = new Scan(startRow, stopRow)
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

  private def getResults(result: Result): Map[Key, Value] = {
    result.listCells.map { cell =>
      val key = (cell.getRowArray, cell.getFamilyArray, cell.getQualifierArray)
      val value = (cell.getValueArray, cell.getTimestamp)
      (key, value)
    }.toMap
  }
}
