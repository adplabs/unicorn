/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.hbase

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Row
import org.apache.hadoop.hbase.util.Bytes
import com.adp.unicorn.store.DataSet
import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.apache.hadoop.hbase.security.visibility.Authorizations

/**
 * HBase table adapter.
 * 
 * @author Haifeng Li (293050)
 */
class HBaseTable(table: HTable, visibility: String, authorizations: String*) extends DataSet {
  var updates = List[Row]()
  val auth = new Authorizations(authorizations: _*)
  
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
    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value)
    if (visibility != "") put.setCellVisibility(new CellVisibility(visibility))
    updates = put :: updates
  }

  override def delete(row: String, columnFamily: String, column: String): Unit = {
    val del = new Delete(Bytes.toBytes(row));
    del.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    if (visibility != "") del.setCellVisibility(new CellVisibility(visibility))
    updates = del :: updates
  }

  override def commit: Unit = {
    table.batch(updates, new Array[Object](updates.length))
    updates = List[Row]()
  }
}