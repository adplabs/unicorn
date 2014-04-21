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
  
  override def dataset(name: String, auth: String): DataSet = {
    val table = new HTable(config, name)
    new HBaseTable(table)
  }
  
  override def createDataSet(name: String): DataSet = {
    createDataSet(name, "", 1, Document.AttributeFamily, Document.RelationshipFamily)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): DataSet = {
    if (admin.tableExists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(TableName.valueOf(name))
    columnFamilies.foreach { columnFamily =>
      val meta = new HColumnDescriptor(Bytes.toBytes(columnFamily))
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
    val config = HBaseConfiguration.create
    new HBaseServer(config)
  }
}
