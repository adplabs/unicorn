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

package unicorn.bigtable.accumulo

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
class AccumuloTable(val db: Accumulo, val name: String) extends BigTable with RowScan with CellLevelSecurity {
  override def close: Unit = () // Connector has no close method

  var cellVisibility = new CellVisibility
  var authorizations = new Authorizations

  override def setCellVisibility(expression: String): Unit = {
    cellVisibility = new CellVisibility(expression)
  }

  override def getCellVisibility: String = {
    new String(cellVisibility.getExpression)
  }

  override def setAuthorizations(labels: String*): Unit = {
    authorizations = new Authorizations(labels: _*)
  }

  override def getAuthorizations: Seq[String] = {
    authorizations.getAuthorizations.map { bytes => new String(bytes)}
  }

  override def get(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Option[Array[Byte]] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    scanner.fetchColumn(new Text(family), new Text(column))
    val iterator = scanner.iterator
    if (iterator.hasNext) Option(iterator.next.getValue.get)
    else None
  }

  override def get(row: Array[Byte], families: Seq[Array[Byte]]): Seq[ColumnFamily] = {
    val scanner = newScanner
    scanner.setRange(new Range(new Text(row)))
    families.foreach { family => scanner.fetchColumnFamily(new Text(family)) }
    val rowScanner = new AccumuloRowScanner(scanner)
    if (rowScanner.hasNext) rowScanner.next.families else Seq()
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
    mutation.put(family, column, cellVisibility, value)

    val writer = newBatchWriter(1)
    writer.addMutation(mutation)
    writer.flush
  }

  override def put(row: Array[Byte], family: Array[Byte], columns: Column*): Unit = {
    val mutation = new Mutation(row)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      if (timestamp == 0)
        mutation.put(family, qualifier, cellVisibility, value)
      else
        mutation.put(family, qualifier, cellVisibility, timestamp, value)
    }

    val writer = newBatchWriter(1)
    writer.addMutation(mutation)
    writer.flush
  }

  override def put(row: Array[Byte], families: ColumnFamily*): Unit = {
    val mutation = new Mutation(row)
    families.foreach { case ColumnFamily(family, columns) =>
      columns.foreach { case Column(qualifier, value, timestamp) =>
        if (timestamp == 0)
          mutation.put(family, qualifier, cellVisibility, value)
        else
          mutation.put(family, qualifier, cellVisibility, timestamp, value)
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

      families.foreach { case ColumnFamily(family, columns) =>
        columns.foreach { case Column(qualifier, value, timestamp) =>
          if (timestamp == 0)
            mutation.put(family, qualifier, cellVisibility, value)
          else
            mutation.put(family, qualifier, cellVisibility, timestamp, value)
        }
      }

      writer.addMutation(mutation)
    }

    writer.flush
  }

  override def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit = {
    val writer = newBatchWriter(1)
    val mutation = new Mutation(row)
    mutation.putDelete(family, column, cellVisibility)

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
      columns.foreach { column => mutation.putDelete(family, column, cellVisibility) }

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

  private def newScanner = db.connector.createScanner(name, authorizations)

  private def newBatchScanner(numQueryThreads: Int) = db.connector.createBatchScanner(name, authorizations, numQueryThreads)

  private def newBatchDeleter(numQueryThreads: Int, maxMemory: Long = 10000000L) = {
    val config = new BatchWriterConfig
      config.setMaxMemory(maxMemory)
      db.connector.createBatchDeleter(name, authorizations, numQueryThreads, config)
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
  private val iterator = scanner.iterator
  private var cell = if (iterator.hasNext) iterator.next else null

  override def close: Unit = scanner.close

  override def hasNext: Boolean = cell != null

  def nextColumnFamily: ColumnFamily = {
    if (cell == null) throw new NoSuchElementException
    val family = cell.getKey.getColumnFamily
    val columns = new collection.mutable.ArrayBuffer[Column]
    do {
      val column = Column(cell.getKey.getColumnQualifier.copyBytes, cell.getValue.get, cell.getKey.getTimestamp)
      columns.append(column)
      if (iterator.hasNext) cell = iterator.next else cell = null
    } while (cell != null && cell.getKey.getColumnFamily.equals(family))
    ColumnFamily(family.copyBytes, columns)
  }

  override def next: Row = {
    if (cell == null) throw new NoSuchElementException
    val rowKey = cell.getKey.getRow
    val families = new collection.mutable.ArrayBuffer[ColumnFamily]
    do {
      val family = nextColumnFamily
      families.append(family)
    } while (cell != null && cell.getKey.getRow.equals(rowKey))
    Row(rowKey.copyBytes, families)
  }
}
