/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.accumulo

import scala.collection.JavaConversions._
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.client.{BatchWriterConfig, ScannerBase}
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility => CellVisibility}
import unicorn.bigtable._

/**
 * Accumulo table adapter.
 * 
 * @author Haifeng Li
 */
class AccumuloTable(val db: Accumulo, val name: String, auth: String, expr: String) extends BigTable with CellLevelSecurity {
  override def close: Unit = () // Connector has no close method

  var expression: Option[String] = Some(expr)
  var labels: Option[Seq[String]] = Some(auth)
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

  override def get(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Option[Array[Byte]] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    scanner.fetchColumn(new Text(family), new Text(column))
    val iter = scanner.iterator
    if (iter.hasNext) Option(iter.next.getValue.get)
    else None
  }

  override def get(row: Array[Byte], families: Seq[Array[Byte]]): Seq[ColumnFamily] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    families.foreach { family => scanner.fetchColumnFamily(new Text(family)) }
    val rowScanner = new AccumuloRowScanner(scanner)
    rowScanner.next.families
  }

  override def get(row: Array[Byte], family: Array[Byte]): Seq[Column] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    scanner.fetchColumnFamily(new Text(family))
    scanner.map { cell =>
      Column(cell.getKey.getColumnQualifier.copyBytes, cell.getValue.get, cell.getKey.getTimestamp)
    }.toSeq
  }

  override def get(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Column] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    columns.foreach { column => scanner.fetchColumn(new Text(family), new Text(column)) }
    scanner.map { cell =>
      Column(cell.getKey.getColumnQualifier.copyBytes, cell.getValue.get, cell.getKey.getTimestamp)
    }.toSeq
  }

  override def get(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Seq[Row] = {
    val scanner = newBatchScanner(numBatchThreads(rows))
    val ranges = rows.map { row => new Range(new Text(row)) }
    scanner.setRanges(ranges)
    families.foreach { family => scanner.fetchColumnFamily(new Text(family)) }
    val rowScanner = new AccumuloRowScanner(scanner)
    rowScanner.toSeq
  }

  override def get(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Row] = {
    val scanner = newBatchScanner(numBatchThreads(rows))
    val ranges = rows.map { row => new Range(new Text(row)) }
    scanner.setRanges(ranges)
    columns.foreach { column => scanner.fetchColumn(new Text(family), new Text(column)) }
    val rowScanner = new AccumuloRowScanner(scanner)
    rowScanner.toSeq
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Seq[Array[Byte]]): RowScanner = {
    val scanner = newScanner
    // from startRow inclusive to endRow exclusive.
    scanner.setRange(new Range(new Text(startRow), true, new Text(stopRow), false))
    families.foreach { family => scanner.fetchColumnFamily(new Text(family)) }
    new AccumuloRowScanner(scanner)
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): RowScanner = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(startRow), new Text(stopRow)))
    columns.foreach { column => scanner.fetchColumn(new Text(family), new Text(column)) }
    new AccumuloRowScanner(scanner)
  }

  override def put(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    val mutation = new Mutation(row)
    if (cellVisibility.isDefined)
      mutation.put(family, column, cellVisibility.get, value)
    else
      mutation.put(family, column, value)

    val writer = newBatchWriter(1)
    writer.addMutation(mutation)
    writer.flush
  }

  override def put(row: Array[Byte], family: Array[Byte], columns: Column*): Unit = {
    val mutation = new Mutation(row)
    if (cellVisibility.isDefined) {
      columns.foreach { case Column(qualifier, value, timestamp) =>
        if (timestamp == 0)
          mutation.put(family, qualifier, cellVisibility.get, value)
        else
          mutation.put(family, qualifier, cellVisibility.get, timestamp, value)
      }
    } else {
      columns.foreach { case Column(qualifier, value, timestamp) =>
        if (timestamp == 0)
          mutation.put(family, qualifier, value)
        else
          mutation.put(family, qualifier, timestamp, value)
      }
    }

    val writer = newBatchWriter(1)
    writer.addMutation(mutation)
    writer.flush
  }

  override def put(row: Array[Byte], families: ColumnFamily*): Unit = {
    val mutation = new Mutation(row)
    if (cellVisibility.isDefined) {
      families.foreach { case ColumnFamily(family, columns) =>
        columns.foreach { case Column(qualifier, value, timestamp) =>
          if (timestamp == 0)
            mutation.put(family, qualifier, cellVisibility.get, value)
          else
            mutation.put(family, qualifier, cellVisibility.get, timestamp, value)
        }
      }
    } else {
      families.foreach { case ColumnFamily(family, columns) =>
        columns.foreach { case Column(qualifier, value, timestamp) =>
          if (timestamp == 0)
            mutation.put(family, qualifier, value)
          else
            mutation.put(family, qualifier, timestamp, value)
        }
      }
    }

    val writer = newBatchWriter(1)
    writer.addMutation(mutation)
    writer.flush
  }

  override def put(rows: Row*): Unit = {
    val writer = newBatchWriter(1)
    rows.foreach { case Row(row, families) =>
      val mutation = new Mutation(row)

      if (cellVisibility.isDefined) {
        families.foreach { case ColumnFamily(family, columns) =>
          columns.foreach { case Column(qualifier, value, timestamp) =>
            if (timestamp == 0)
              mutation.put(family, qualifier, cellVisibility.get, value)
            else
              mutation.put(family, qualifier, cellVisibility.get, timestamp, value)
          }
        }
      } else {
        families.foreach { case ColumnFamily(family, columns) =>
          columns.foreach { case Column(qualifier, value, timestamp) =>
            if (timestamp == 0)
              mutation.put(family, qualifier, value)
            else
              mutation.put(family, qualifier, timestamp, value)
          }
        }
      }

      writer.addMutation(mutation)
    }

    writer.flush
  }

  override def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit = {
    val writer = newBatchWriter(1)
    val mutation = new Mutation(row)
    if (cellVisibility.isDefined)
      mutation.putDelete(family, column, cellVisibility.get)
    else
      mutation.putDelete(family, column)

    writer.addMutation(mutation)
    writer.flush
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    if (columns.isEmpty) {
      val range = Range.exact(new Text(row), new Text(family))
      val deleter = newBatchDeleter(1)
      deleter.setRanges(Seq(range))
      deleter.delete
    } else {
      val writer = newBatchWriter(1)
      val mutation = new Mutation(row)
      if (cellVisibility.isDefined)
        columns.foreach { column => mutation.putDelete(family, column, cellVisibility.get) }
      else
        columns.foreach { column => mutation.putDelete(family, column) }

      writer.addMutation(mutation)
      writer.flush
    }
  }

  override def delete(row: Array[Byte], families: Seq[Array[Byte]]): Unit = {
    val deleter = newBatchDeleter(1)
    val ranges = if (families.isEmpty)
      Seq(Range.exact(new Text(row)))
    else
      families.map { family => Range.exact(new Text(row), new Text(family)) }

    deleter.setRanges(ranges)
    deleter.delete
  }

  override def delete(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Unit = {
    val deleter = newBatchDeleter(numBatchThreads(rows))
    val ranges = rows.flatMap { row =>
      if (families.isEmpty)
        Seq(Range.exact(new Text(row)))
      else
        families.map { family => Range.exact(new Text(row), new Text(family)) }
    }
    deleter.setRanges(ranges)
    deleter.delete
  }

  override def delete(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    val deleter = newBatchDeleter(numBatchThreads(rows))
    val ranges = rows.flatMap { row =>
      if (columns.isEmpty)
        Seq(Range.exact(new Text(row), new Text(family)))
      else
        columns.map { column => Range.exact(new Text(row), new Text(family), new Text(column)) }
    }
    deleter.setRanges(ranges)
    deleter.delete
  }

  private def numBatchThreads[T](rows: Seq[T]): Int = Math.min(rows.size, Runtime.getRuntime.availableProcessors)

  private def newScanner = authorizations match {
    case None => throw new IllegalStateException("Authorizations not set yet")
    case Some(auth) => db.connector.createScanner(name, auth)
  }

  private def newBatchScanner(numQueryThreads: Int) = authorizations match {
    case None => throw new IllegalStateException("Authorizations not set yet")
    case Some(auth) => db.connector.createBatchScanner(name, auth, numQueryThreads)
  }

  private def newBatchDeleter(numQueryThreads: Int, maxMemory: Long = 10000000L) = authorizations match {
    case None => throw new IllegalStateException("Authorizations not set yet")
    case Some(auth) =>
      val config = new BatchWriterConfig
      config.setMaxMemory(maxMemory)
      db.connector.createBatchDeleter(name, auth, numQueryThreads, config)
  }

  /**
   *  @param maxMemory the maximum memory in bytes to batch before writing.
   *                   The smaller this value, the more frequently the BatchWriter will write.
   */
  private def newBatchWriter(maxMemory: Long = 10000000L) = {
    // Use the default durability that is the table's durability setting.
    val config = new BatchWriterConfig
    config.setMaxMemory(maxMemory)
    db.connector.createBatchWriter(name, config)
  }
}

class AccumuloRowScanner(scanner: ScannerBase) extends RowScanner {
  val iter = scanner.iterator
  private var cell = if (iter.hasNext) iter.next else null

  def close: Unit = scanner.close

  def hasNext: Boolean = cell != null

  def nextColumnFamily: ColumnFamily = {
    if (cell == null) throw new NoSuchElementException
    val family = cell.getKey.getColumnFamily
    val columns = new collection.mutable.ArrayBuffer[Column]
    do {
      val column = Column(cell.getKey.getColumnQualifier.copyBytes, cell.getValue.get, cell.getKey.getTimestamp)
      columns.append(column)
      if (iter.hasNext) cell = iter.next else cell = null
    } while (cell != null && cell.getKey.getColumnFamily.equals(family))
    ColumnFamily(family.copyBytes, columns)
  }

  def next: Row = {
    if (cell == null) throw new NoSuchElementException
    val rowKey = cell.getKey.getRow
    val families = new collection.mutable.ArrayBuffer[ColumnFamily]
    do {
      val family = nextColumnFamily
      families.append(family)
      if (iter.hasNext) cell = iter.next else cell = null
    } while (cell != null && cell.getKey.getRow.equals(rowKey))
    Row(rowKey.copyBytes, families)
  }
}
