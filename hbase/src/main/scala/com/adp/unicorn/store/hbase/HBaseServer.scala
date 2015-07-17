/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.TableName
import com.adp.unicorn.store.Database
import com.adp.unicorn.store.Dataset
import com.adp.unicorn.Document
import org.apache.hadoop.hbase.util.Bytes

/**
 * HBase server adapter.
 *
 * @author Haifeng Li (293050)
 */
class HBaseServer(config: Configuration) extends Database {
  lazy val connection = ConnectionFactory.createConnection(config)
  lazy val admin = connection.getAdmin
  
  override def dataset(name: String, visibility: Option[String], authorizations: Option[Seq[String]]): Dataset = {
    val table = connection.getTable(TableName.valueOf(name))
    new HBaseTable(table, visibility, authorizations)
  }
  
  override def createDataSet(name: String): Unit = {
    createDataSet(name, "", 1, Document.AttributeFamily, Document.RelationshipFamily)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): Unit = {
    if (admin.tableExists(TableName.valueOf(name)))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(TableName.valueOf(name))
    columnFamilies.foreach { columnFamily =>
      val meta = new HColumnDescriptor(Bytes.toBytes(columnFamily))
      tableDesc.addFamily(meta)
    }
    admin.createTable(tableDesc)
  }
  
  override def dropDataSet(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    if (!admin.tableExists(tableName))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }
}

object HBaseServer {
  def apply(): HBaseServer = {
    // HBaseConfiguration reads in hbase-site.xml and in hbase-default.xml that
    // can be found on the CLASSPATH
    val config = HBaseConfiguration.create
    new HBaseServer(config)
  }

  def apply(config: Configuration): HBaseServer = {
    new HBaseServer(config)
  }
}
