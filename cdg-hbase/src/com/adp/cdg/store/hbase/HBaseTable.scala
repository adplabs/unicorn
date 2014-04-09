package com.adp.cdg.store.hbase

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Row
import org.apache.hadoop.hbase.util.Bytes
import com.adp.cdg.store.DataSet
import java.util.ArrayList

class HBaseTable(table: HTable) extends DataSet {
  var updates = List[Row]()

  @throws(classOf[Exception])
  override def read(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val get = new Get(Bytes.toBytes(row))
    val family = Bytes.toBytes(columnFamily)
    get.addFamily(family)
    
    val result = table.get(get).getFamilyMap(family)
    result.map { case (key, value) => (new String(key), value) }
  }
  
  @throws(classOf[Exception])
  override def read(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    val get = new Get(Bytes.toBytes(row))
    val family = Bytes.toBytes(columnFamily)
    columns.foreach { column => get.addColumn(family, Bytes.toBytes(column)) }

    val result = table.get(get).getFamilyMap(family)
    result.map { case (key, value) => (new String(key), value) }
  }
  
  @throws(classOf[Exception])
  override def write(row: String, columnFamily: String, column: String, value: Array[Byte], visibility: String): Unit = {
    val put = new Put(Bytes.toBytes(row));
    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value)
    updates = put :: updates
  }

  @throws(classOf[Exception])
  override def delete(row: String, columnFamily: String, column: String, visibility: String): Unit = {
    val d = new Delete(Bytes.toBytes(row));
    d.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    updates = d :: updates
  }

  @throws(classOf[Exception])
  override def commit: Unit = {
    table.batch(updates, new Array[Object](updates.length))
    updates = List[Row]()
  }
}