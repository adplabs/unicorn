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

  override def read(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val get = new Get(Bytes.toBytes(row))
    val family = Bytes.toBytes(columnFamily)
    get.addFamily(family)
    
    val map = collection.mutable.Map[String, Array[Byte]]()
    val result = table.get(get).getFamilyMap(family)
    if (result != null) {
      result.foreach { case (key, value) => map(new String(key)) = value }
    }
    map
  }
  
  override def read(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    val get = new Get(Bytes.toBytes(row))
    val family = Bytes.toBytes(columnFamily)
    columns.foreach { column => get.addColumn(family, Bytes.toBytes(column)) }

    val map = collection.mutable.Map[String, Array[Byte]]()
    val result = table.get(get).getFamilyMap(family)
    if (result != null) {
      result.foreach { case (key, value) => map(new String(key)) = value }
    }
    map
  }
  
  override def write(row: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    val put = new Put(Bytes.toBytes(row));
    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value)
    updates = put :: updates
  }

  override def delete(row: String, columnFamily: String, column: String): Unit = {
    val del = new Delete(Bytes.toBytes(row));
    del.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    updates = del :: updates
  }

  override def commit: Unit = {
    table.batch(updates, new Array[Object](updates.length))
    updates = List[Row]()
  }
}