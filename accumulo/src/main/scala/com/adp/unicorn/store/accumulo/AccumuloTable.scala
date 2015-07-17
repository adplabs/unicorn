/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.accumulo

import scala.collection.JavaConversions._
import com.adp.unicorn.store.Dataset
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.data.{Range, Key, Value}
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility}
import org.apache.accumulo.core.data.Mutation

/**
 * Accumulo table adapter.
 * 
 * @author Haifeng Li (293050)
 */
class AccumuloTable(conn: Connector, table: String, visibility: Option[String], authorizations: Option[Seq[String]]) extends Dataset {
  
  val columnVisibility = visibility match {
    case None => new ColumnVisibility()
    case Some(vis) => new ColumnVisibility(vis)
  }

  val auth = authorizations match {
    case None => new Authorizations()
    case Some(a) => new Authorizations(a: _*)
  }

  val scanner = conn.createScanner(table, auth)
  
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
  
  override def write(row: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    val mutation = new Mutation(row)
    mutation.put(columnFamily, column, columnVisibility, new Value(value))
    writer.addMutation(mutation)
  }

  override def delete(row: String, columnFamily: String, column: String): Unit = {
    val mutation = new Mutation(row)
    mutation.putDelete(columnFamily, column, columnVisibility)
    writer.addMutation(mutation)
  }

  /**
   * Delete rows between (start, end].
   */
  def deleteRows(start: String, end: String): Unit = {
    conn.tableOperations.deleteRows(table, new Text(start), new Text(end))
  }
  
  override def commit: Unit = {
    writer.flush
  }
}