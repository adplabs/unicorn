package com.adp.cdg.store.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import com.adp.cdg.store.DataStore
import com.adp.cdg.store.DataSet
import org.apache.hadoop.hbase.HColumnDescriptor

class HBaseServer(config: Configuration) extends DataStore {
  lazy val admin = new HBaseAdmin(config)
  
  override def dataset(name: String, auth: String): DataSet = {
    val table = new HTable(config, name)
    new HBaseTable(table)
  }
  
  override def createDataSet(name: String, columnFamilies: String*): DataSet = {
    if (admin.tableExists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(name)
    columnFamilies.foreach { columnFamily =>
      val meta = new HColumnDescriptor(columnFamily.getBytes());
      tableDesc.addFamily(meta)
    }
    admin.createTable(tableDesc)

    dataset(name)
  }
  
  override def dropDataSet(name: String): Unit = {
    if (!admin.tableExists(name))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    admin.disableTable(name)
    admin.deleteTable(name)
  }
}

object HBaseServer {
  def apply(): HBaseServer = {
    // HBaseConfiguration reads in hbase-site.xml and in hbase-default.xml that
    // can be found on the CLASSPATH
    val config = HBaseConfiguration.create();
    new HBaseServer(config)
  }
}
