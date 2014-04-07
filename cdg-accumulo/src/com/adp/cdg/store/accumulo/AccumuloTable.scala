package com.adp.cdg.store.accumulo

import scala.collection.JavaConversions._
import scala.collection.convert.WrapAsScala.enumerationAsScalaIterator
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.client.{BatchWriter, Scanner}
import org.apache.accumulo.core.data.{Range, Key, Value}
import org.apache.accumulo.core.client.{Connector, Scanner, ZooKeeperInstance}
import org.apache.accumulo.core.client.{BatchWriter, BatchWriterConfig}
import org.apache.accumulo.core.security.Authorizations
import com.adp.cdg.store.DataSet
import com.adp.cdg.Document
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.ColumnVisibility

class AccumuloTable(conn: Connector, table: String, auth: String) extends DataSet {
  
  lazy val scanner = {
    val auths = new Authorizations(auth)
    conn.createScanner(table, auths)
  }
  
  lazy val writer = {
    val config = new BatchWriterConfig()
    // bytes available to batchwriter for buffering mutations
    config.setMaxMemory(10000000L);
    
    conn.createBatchWriter(table, config)
  }
  
  override def read(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val range = new Range(row)
    scanner.setRange(range)
    scanner.clearColumns
    scanner.fetchColumnFamily(new Text(columnFamily))
    
    val attributes = collection.mutable.Map[String, Array[Byte]]()
    
    val it:scala.collection.Iterator[java.util.Map.Entry[Key,Value]] = scanner.iterator
    while (it.hasNext) {
      val entry = it.next
      val col = entry.getKey.getColumnQualifier.toString
      val bytes = entry.getValue.get
      attributes += (col -> bytes)
    }

    attributes
  }
  
  override def read(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    val range = new Range(row)
    scanner.setRange(range)
    scanner.clearColumns
    val family = new Text(columnFamily)
    columns.foreach { column => scanner.fetchColumn(family, new Text(column)) }
    
    val attributes = collection.mutable.Map[String, Array[Byte]]()
    
    val it:scala.collection.Iterator[java.util.Map.Entry[Key,Value]] = scanner.iterator
    while (it.hasNext) {
      val entry = it.next
      val col = entry.getKey.getColumnQualifier.toString
      val bytes = entry.getValue.get
      attributes += (col -> bytes)
    }

    attributes
  }
  
  override def write(row: String, columnFamily: String, column: String, value: Array[Byte], visibility: String): Unit = {
    val mutation = new Mutation(row)
    mutation.put(columnFamily, column, new ColumnVisibility(visibility), new Value(value))
    writer.addMutation(mutation)
  }

  override def delete(row: String, columnFamily: String, column: String): Unit = {
    val mutation = new Mutation(row)
    mutation.putDelete(columnFamily, column)
    writer.addMutation(mutation)
  }
  
  override def deleteRows(start: String, end: String): Unit = {
    conn.tableOperations.deleteRows(table, new Text(start), new Text(end))
  }
  
  override def commit: Unit = {
    writer.flush
  }
}