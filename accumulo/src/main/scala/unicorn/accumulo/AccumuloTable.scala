/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.accumulo

import scala.collection.JavaConversions._
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.data.{Range, Key => AccumuloKey, Value => AccumuloValue}
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility => CellVisibility}
import org.apache.accumulo.core.data.Mutation

/**
 * Accumulo table adapter.
 * 
 * @author Haifeng Li (293050)
 */
class AccumuloTable(conn: Connector, table: String) extends unicorn.bigtable.Table {
  class AccumuloScanner(scanner: org.apache.accumulo.core.client.Scanner) extends Scanner {
    val iter = scanner.iterator
    def close: Unit = scanner.close
    def hasNext: Boolean = iter.hasNext
    def next: Map[Key, Value] = {
      val cell = iter.next
      val key = (cell.getKey.getRow.copyBytes, cell.getKey.getColumnFamily.copyBytes, cell.getKey.getColumnQualifier.copyBytes())
      val value = (cell.getValue.get, cell.getKey.getTimestamp)
      Map(key -> value)
    }
  }

  override def close: Unit = () // Connector has no close method

  var expression: Option[String] = None
  var labels: Option[Seq[String]] = None
  var cellVisibility: Option[CellVisibility] = None
  var authorizations: Option[Authorizations] = None

  override def setCellVisibility(expression: String): Unit = {
    this.expression = Some(expression)
    cellVisibility = Some(new CellVisibility(expression))
  }

  override def getCellVisibility: Option[String] = expression

  override def setAuthorizations(labels: String*): Unit = {
    this.labels = Some(labels)
    authorizations = Some(new Authorizations(labels: _*))
  }

  override def getAuthorizations: Option[Seq[String]] = labels

  override def get(row: Array[Byte], families: Array[Byte]*): Map[Key, Value] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    families.foreach { family => scanner.fetchColumnFamily(new Text(family)) }
    getResults(scanner)
  }
  
  override def get(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Map[Key, Value]  = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    columns.foreach { column => scanner.fetchColumn(new Text(family), new Text(column)) }
    getResults(scanner)
  }

  override def get(keys: Key*): Map[Key, Value] = {
    keys.foldLeft(Map.empty[Key, Value]) { case (acc, (row, family, column)) =>
      val scanner = newScanner
      scanner.setRange(new Range(new Text(row)))
      scanner.fetchColumn(new Text(family), new Text(column))
      acc ++ getResults(scanner)
    }
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Array[Byte]*): Scanner = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(startRow), new Text(stopRow)))
    families.foreach { family => scanner.fetchColumnFamily(new Text(family)) }
    new AccumuloScanner(scanner)
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Scanner = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(startRow), new Text(stopRow)))
    columns.foreach { column => scanner.fetchColumn(new Text(family), new Text(column)) }
    new AccumuloScanner(scanner)
  }

  private def newScanner: org.apache.accumulo.core.client.Scanner = {
    authorizations match {
      case None => throw new IllegalStateException("Authorizations not set yet")
      case Some(auth) => conn.createScanner(table, auth)
    }
  }

  private def getResults(scanner: org.apache.accumulo.core.client.Scanner): Map[Key, Value] = {
    scanner.map { cell =>
      val key = (cell.getKey.getRow.copyBytes, cell.getKey.getColumnFamily.copyBytes, cell.getKey.getColumnQualifier.copyBytes())
      val value = (cell.getValue.get, cell.getKey.getTimestamp)
      (key, value)
    }.toMap
  }

  private val batchWriterConfig = new BatchWriterConfig
  // bytes available to batchwriter for buffering mutations
  batchWriterConfig.setMaxMemory(10000000L)
  private val writer = conn.createBatchWriter(table, batchWriterConfig)

  override def put(row: Array[Byte], family: Array[Byte], columns: (Array[Byte], Array[Byte])*): Unit = {
    require(!columns.isEmpty)
    val mutation = new Mutation(row)
    if (cellVisibility.isDefined)
      columns.foreach { case (column, value) => mutation.put(family, column, cellVisibility.get, value) }
    else
      columns.foreach { case (column, value) => mutation.put(family, column, value) }

    writer.addMutation(mutation)
    writer.flush
  }

  override def put(values: (Key, Array[Byte])*): Unit = {
    require(!values.isEmpty)
    values.foreach { case (key, value) =>
      val mutation = new Mutation(key._1)
      if (cellVisibility.isDefined)
        mutation.put(key._2, key._3, cellVisibility.get, value)
      else
        mutation.put(key._2, key._3, value)

      writer.addMutation(mutation)
    }
    writer.flush
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    require(!columns.isEmpty)
    val mutation = new Mutation(row)
    if (cellVisibility.isDefined)
      columns.foreach { column => mutation.putDelete(family, column, cellVisibility.get) }
    else
      columns.foreach { column => mutation.putDelete(family, column) }

    writer.addMutation(mutation)
    writer.flush
  }

  override def delete(keys: Key*): Unit = {
    require(!keys.isEmpty)
    keys.foreach { key =>
      val mutation = new Mutation(key._1)
      if (cellVisibility.isDefined)
        mutation.putDelete(key._2, key._3, cellVisibility.get)
      else
        mutation.putDelete(key._2, key._3)

      writer.addMutation(mutation)
    }
    writer.flush
  }

  override def delete(row: Array[Byte]): Unit = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))

    val mutation = new Mutation(row)
    // iterate through the keys
    scanner.foreach { case cell =>
      if (cellVisibility.isDefined)
        mutation.putDelete(cell.getKey.getColumnFamily, cell.getKey.getColumnQualifier, cellVisibility.get)
      else
        mutation.putDelete(cell.getKey.getColumnFamily, cell.getKey.getColumnQualifier)
      writer.addMutation(mutation)
    }
    writer.flush
  }

  /** Unsupported */
  override def rollback(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def rollback(keys: Key*): Unit = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def increment(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit = {
    throw new UnsupportedOperationException
  }
}
