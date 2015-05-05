/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.TableName
import com.adp.unicorn.store.DataStore
import com.adp.unicorn.store.DataSet
import com.adp.unicorn.Document
import org.apache.hadoop.hbase.util.Bytes

/**
 * HBase server adapter.
 *
 * @author Haifeng Li (293050)
 */
class HBaseServer(config: Configuration) extends DataStore {
  lazy val admin = new HBaseAdmin(config)
  
  def dataset(name: String): DataSet = dataset(name, "")
  
  override def dataset(name: String, visibility: String, authorizations: String*): DataSet = {
    val table = new HTable(config, name)
    new HBaseTable(table, visibility, authorizations: _*)
  }
  
  override def createDataSet(name: String): Unit = {
    createDataSet(name, "", 1, Document.AttributeFamily, Document.RelationshipFamily)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): Unit = {
    if (admin.tableExists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(TableName.valueOf(name))
    columnFamilies.foreach { columnFamily =>
      val meta = new HColumnDescriptor(Bytes.toBytes(columnFamily))
      tableDesc.addFamily(meta)
    }
    admin.createTable(tableDesc)
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
    val config = HBaseConfiguration.create
    new HBaseServer(config)
  }
  
  // Configuration config =  HBaseConfiguration.create
  // config.set("hbase.zookeeper.quorum", "cdldvtitavap015:2181,cdldvtitavap016:2181,cdldvtitavap017:2181")
  // config.set("hbase.zookeeper.property.clientPort", "2181")
  def apply(config: Configuration): HBaseServer = {
    new HBaseServer(config)
  }
}
