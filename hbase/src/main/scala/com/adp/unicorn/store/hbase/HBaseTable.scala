/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.hbase

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Row
import org.apache.hadoop.hbase.util.Bytes
import com.adp.unicorn.store.Dataset
import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.apache.hadoop.hbase.security.visibility.Authorizations

/**
 * HBase table adapter.
 * 
 * @author Haifeng Li (293050)
 */
class HBaseTable(table: Table, visibility: Option[String], authorizations: Option[Seq[String]]) extends Dataset {
  var updates = List[Row]()

  val cellVisibility = visibility match {
    case None => new CellVisibility("")
    case Some(vis) => new CellVisibility(vis)
  }

  val auth = authorizations match {
    case None => new Authorizations()
    case Some(a) => new Authorizations(a: _*)
  }
  
  override def read(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val get = new Get(Bytes.toBytes(row))
    if (!authorizations.isEmpty) get.setAuthorizations(auth)
    val family = Bytes.toBytes(columnFamily)
    get.addFamily(family)
    
    val result = table.get(get).getFamilyMap(family)
    mapOf(result)
  }
  
  override def read(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    val get = new Get(Bytes.toBytes(row))
    if (!authorizations.isEmpty) get.setAuthorizations(auth)
    val family = Bytes.toBytes(columnFamily)
    columns.foreach { column => get.addColumn(family, Bytes.toBytes(column)) }
    val result = table.get(get).getFamilyMap(family)
    mapOf(result)
  }
  
  private def mapOf(result: java.util.NavigableMap[Array[Byte], Array[Byte]]): collection.mutable.Map[String, Array[Byte]] = {
    val map = collection.mutable.Map[String, Array[Byte]]()
    if (result != null) {
      result.foreach { case (key, value) => map(Bytes.toString(key)) = value }
    }
    map
  }
  
  override def write(row: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    val put = new Put(Bytes.toBytes(row));
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value)
    if (visibility.isDefined) put.setCellVisibility(cellVisibility)
    updates = put :: updates
  }

  override def delete(row: String, columnFamily: String, column: String): Unit = {
    val del = new Delete(Bytes.toBytes(row));
    del.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    if (visibility.isDefined) del.setCellVisibility(cellVisibility)
    updates = del :: updates
  }

  override def commit: Unit = {
    table.batch(updates, new Array[Object](updates.length))
    updates = List[Row]()
  }
}