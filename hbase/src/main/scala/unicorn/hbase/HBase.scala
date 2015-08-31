/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.hbase

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.TableName
import unicorn.bigtable.BigTable

/**
 * HBase server adapter.
 *
 * @author Haifeng Li (293050)
 */
class HBase(config: Configuration) extends unicorn.bigtable.Database {
  val connection = ConnectionFactory.createConnection(config)
  val admin = connection.getAdmin

  override def close: Unit = connection.close

  override def apply(name: String): BigTable = {
    new HBaseTable(this, name)
  }

  override def createTable(name: String, families: String*): BigTable = {
    createTable(name, new Properties(), families: _*)
  }

  override def createTable(name: String, props: Properties, families: String*): BigTable = {
    if (admin.tableExists(TableName.valueOf(name)))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(TableName.valueOf(name))
    props.stringPropertyNames.foreach { p => tableDesc.setConfiguration(p, props.getProperty(p))}
    families.foreach { family =>
      val desc = new HColumnDescriptor(family)
      props.stringPropertyNames.foreach { p => desc.setConfiguration(p, props.getProperty(p))}
      tableDesc.addFamily(desc)
    }
    admin.createTable(tableDesc)
    apply(name)
  }

  /** Truncates a table and preserves the splits */
  override def truncateTable(name: String): Unit = {
    admin.truncateTable(TableName.valueOf(name), true)
  }

  override def dropTable(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }

  override def compactTable(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    admin.compact(tableName)
  }

  override def majorCompactTable(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    admin.majorCompact(tableName)
  }
}

object HBase {
  /* Uses hbase-site.xml and in hbase-default.xml that can be found on the CLASSPATH */
  def apply(): HBase = {
    val config = HBaseConfiguration.create
    new HBase(config)
  }

  def apply(config: Configuration): HBase = {
    new HBase(config)
  }
}
