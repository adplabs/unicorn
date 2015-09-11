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

package unicorn.bigtable.cassandra

import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.cassandra.thrift.{Column => CassandraColumn}
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ColumnPath
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.Mutation
import org.apache.cassandra.thrift.NotFoundException
import org.apache.cassandra.thrift.Deletion
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import unicorn.bigtable._
import unicorn.util.utf8

/**
 * Cassandra keyspace adapter. Cassandra's keyspaces may be regarded as tables
 * in other NoSQL solutions such as Accumulo and HBase.
 * 
 * @author Haifeng Li
 */
class CassandraTable(val db: Cassandra, val name: String, consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) extends BigTable with IntraRowScan {
  val client = db.client
  client.set_keyspace(name)
  val columnFamilies = client.describe_keyspace(name).getCf_defs.map(_.getName)

  override def close: Unit = () // Client has no close method

  private val nullRange = Array[Byte]()
  //private val nullRange = ByteBuffer.wrap(emptyBytes)

  override def get(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Option[Array[Byte]] = {
    get(row, new String(family, utf8), column)
  }

  def get(row: Array[Byte], family: String, column: Array[Byte]): Option[Array[Byte]] = {
    val key = ByteBuffer.wrap(row)
    val path = new ColumnPath(family).setColumn(column)

    try {
      val result = client.get(key, path, consistency)
      Some(result.getColumn.getValue)
    } catch {
      case _: NotFoundException => None
    }
  }

  override def get(row: Array[Byte], families: Seq[Array[Byte]]): Seq[ColumnFamily] = {
    val f = if (families.isEmpty) columnFamilies else families.map(new String(_, utf8))
    f.map { family => ColumnFamily(family.getBytes(utf8), get(row, family)) }.filter(!_.columns.isEmpty)
  }

  override def get(row: Array[Byte], family: Array[Byte]): Seq[Column] = {
    get(row, new String(family, utf8))
  }

  def get(row: Array[Byte], family: String): Seq[Column] = {
    val columns = new ArrayBuffer[Column]
    var iterator = get(row, family, nullRange, nullRange, 100).iterator
    while (iterator.hasNext) {
      columns.appendAll(iterator)
      iterator = get(row, family, columns.last.qualifier, nullRange, 100).iterator
      iterator.next // get ride of the first that is the last one of previous bulk
    }
    columns
  }

  /**
   * Get a slice of rows.
   * The default count should be sufficient for most documents.
   */
  def get(row: Array[Byte], family: String, startColumn: Array[Byte], stopColumn: Array[Byte], count: Int): Seq[Column] = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(family)
    val predicate = new SlicePredicate
    val range = new SliceRange
    range.start = ByteBuffer.wrap(startColumn)
    range.finish = ByteBuffer.wrap(stopColumn)
    range.reversed = false
    range.count = count
    predicate.setSlice_range(range)

    val slice = client.get_slice(key, parent, predicate, consistency)
    getColumns(slice)
  }

  override def get(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Column] = {
    get(row, new String(family, utf8), columns)
  }

  def get(row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Seq[Column] = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(family)
    val predicate = new SlicePredicate
    columns.foreach { column =>
      predicate.addToColumn_names(ByteBuffer.wrap(column))
    }

    val slice = client.get_slice(key, parent, predicate, consistency)
    getColumns(slice)
  }

  override def get(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Seq[Row] = {
    rows.map { row =>
      val result = get(row, families)
      Row(row, result)
    }.filter(!_.families.isEmpty)
  }

  override def get(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Row] = {
    get(rows, new String(family, utf8), columns)
  }

  def get(rows: Seq[Array[Byte]], family: String, columns: Seq[Array[Byte]]): Seq[Row] = {
    val keys = rows.map(ByteBuffer.wrap(_))
    val parent = new ColumnParent(family)
    val predicate = new SlicePredicate
    columns.foreach { column =>
      predicate.addToColumn_names(ByteBuffer.wrap(column))
    }

    val slices = client.multiget_slice(keys, parent, predicate, consistency)
    slices.map { case (row, slice) =>
      Row(row.array, Seq(ColumnFamily(family.getBytes(utf8), getColumns(slice))))
    }.toSeq
  }

  override def scan(row: Array[Byte], family: Array[Byte], startColumn: Array[Byte], stopColumn: Array[Byte]): IntraRowScanner = {
    scan(row, new String(family, utf8), startColumn, stopColumn)
  }

  def scan(row: Array[Byte], family: String, startColumn: Array[Byte], stopColumn: Array[Byte]): IntraRowScanner = {
    new IntraRowScanner {
      var iterator = get(row, family, startColumn, stopColumn, 100).iterator

      override def close: Unit = ()

      override def hasNext: Boolean = {
        iterator.hasNext
      }

      override def next: Column = {
        val column = iterator.next
        if (!iterator.hasNext) {
          iterator = get(row, family, column.qualifier, stopColumn, 100).iterator
          iterator.next // get ride of the first one
        }
        column
      }
    }
  }

  private def getColumns(result: java.util.List[ColumnOrSuperColumn]): Seq[Column] = {
    result.map { column =>
      val c = column.getColumn
      Column(c.getName, c.getValue, c.getTimestamp)
    }
  }

  override def put(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    put(row, new String(family, utf8), column, value)
  }

  def put(row: Array[Byte], family: String, column: Array[Byte], value: Array[Byte]): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(family)
    val put = new CassandraColumn(ByteBuffer.wrap(column)).setValue(value).setTimestamp(System.currentTimeMillis)
    client.insert(key, parent, put, consistency)
  }

  override def put(row: Array[Byte], family: Array[Byte], columns: Column*): Unit = {
    put(row, new String(family, utf8), columns: _*)
  }

  def put(row: Array[Byte], family: String, columns: Column*): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = family
    val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]

    createMutationMapEntry(updates, key, parent)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      val put = new ColumnOrSuperColumn
      put.column = new CassandraColumn(ByteBuffer.wrap(qualifier))
      put.column.setValue(value)
      if (timestamp == 0) put.column.setTimestamp(System.currentTimeMillis)
      else put.column.setTimestamp(timestamp)
      val mutation = new Mutation
      mutation.column_or_supercolumn = put
      updates.get(key).get(parent).add(mutation)
    }

    client.batch_mutate(updates, consistency)
  }

  override def put(row: Array[Byte], families: ColumnFamily*): Unit = {
    val key = ByteBuffer.wrap(row)
    val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]

    families.foreach { case ColumnFamily(family, columns) =>
      val parent = new String(family, utf8)
      createMutationMapEntry(updates, key, parent)
      columns.foreach { case Column(qualifier, value, timestamp) =>
        val put = new ColumnOrSuperColumn
        put.column = new CassandraColumn(ByteBuffer.wrap(qualifier))
        put.column.setValue(value)
        if (timestamp == 0) put.column.setTimestamp(System.currentTimeMillis)
        else put.column.setTimestamp(timestamp)
        val mutation = new Mutation
        mutation.column_or_supercolumn = put
        updates.get(key).get(parent).add(mutation)
      }
    }

    client.batch_mutate(updates, consistency)
  }

  override def put(rows: Row*): Unit = {
    val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]

    val puts = rows.map { case Row(row, families) =>
      val key = ByteBuffer.wrap(row)
      families.foreach { case ColumnFamily(family, columns) =>
        val parent = new String(family, utf8)
        createMutationMapEntry(updates, key, parent)
        columns.foreach { case Column(qualifier, value, timestamp) =>
          val put = new ColumnOrSuperColumn
          put.column = new CassandraColumn(ByteBuffer.wrap(qualifier))
          put.column.setValue(value)
          if (timestamp == 0) put.column.setTimestamp(System.currentTimeMillis)
          else put.column.setTimestamp(timestamp)
          val mutation = new Mutation
          mutation.column_or_supercolumn = put
          updates.get(key).get(parent).add(mutation)
        }
      }
    }

    client.batch_mutate(updates, consistency)
  }

  override def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit = {
    delete(row, new String(family, utf8), column)
  }

  def delete(row: Array[Byte], family: String, column: Array[Byte]): Unit = {
    val key = ByteBuffer.wrap(row)
    val path = new ColumnPath(family).setColumn(column)
    client.remove(key, path, System.currentTimeMillis, consistency)
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    delete(row, new String(family, utf8), columns)
  }

  def delete(row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Unit = {
    val key = ByteBuffer.wrap(row)

    if (columns.isEmpty) {
      val path = new ColumnPath(family)
      client.remove(key, path, System.currentTimeMillis, consistency)
    } else {
      val parent = family
      val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]
      createMutationMapEntry(updates, key, parent)

      val predicate = new SlicePredicate
      columns.foreach { column => predicate.addToColumn_names(ByteBuffer.wrap(column)) }

      val deletion = new Deletion
      deletion.setTimestamp(System.currentTimeMillis)
      deletion.setPredicate(predicate)

      val mutation = new Mutation
      mutation.deletion = deletion
      updates.get(key).get(parent).add(mutation)

      client.batch_mutate(updates, consistency)
    }
  }

  override def delete(row: Array[Byte], families: Seq[Array[Byte]]): Unit = {
    val key = ByteBuffer.wrap(row)
    val f = if (families.isEmpty) columnFamilies else families.map(new String(_, utf8))
    f.foreach { family =>
      val path = new ColumnPath(family)
      client.remove(key, path, System.currentTimeMillis, consistency)
    }
  }

  override def delete(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Unit = {
    rows.foreach { row =>
      delete(row, families)
    }
  }

  override def delete(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Unit = {
    if (columns.isEmpty) {
      val path = new ColumnPath(new String(family, utf8))
      rows.foreach { row =>
        val key = ByteBuffer.wrap(row)
        client.remove(key, path, System.currentTimeMillis, consistency)
      }
    } else {
      val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]
      val parent = new String(family, utf8)
      rows.foreach { row =>
      val key = ByteBuffer.wrap(row)
        createMutationMapEntry(updates, key, parent)

        val predicate = new SlicePredicate
        columns.foreach { column => predicate.addToColumn_names(ByteBuffer.wrap(column)) }

        val deletion = new Deletion
        deletion.setTimestamp(System.currentTimeMillis)
        deletion.setPredicate(predicate)

        val mutation = new Mutation
        mutation.deletion = deletion
        updates.get(key).get(parent).add(mutation)
      }

      client.batch_mutate(updates, consistency)
    }
  }


  /**
   * Create mutation map entry if necessary.
   */
  private def createMutationMapEntry(updates: java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]], key: ByteBuffer, family: String) {
    if (!updates.containsKey(key)) {
      val row = new java.util.HashMap[String, java.util.List[Mutation]]
      row.put(family, new java.util.ArrayList[Mutation])
      updates.put(key, row)
    } else if (!updates.get(key).containsKey(family)) {
      updates.get(key).put(family, new java.util.ArrayList[Mutation])
    }
  }
}
