/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.TableName

/**
 * HBase server adapter.
 *
 * @author Haifeng Li (293050)
 */
class HBase(config: Configuration) extends unicorn.bigtable.Database {
  private val connection = ConnectionFactory.createConnection(config)
  private val admin = connection.getAdmin

  override def close: Unit = connection.close

  override def apply(name: String): unicorn.bigtable.Table = {
    val table = connection.getTable(TableName.valueOf(name))
    new HBaseTable(table)
  }
  
  override def createTable(name: String, strategy: String, replication: Int, families: String*): Unit = {
    if (admin.tableExists(TableName.valueOf(name)))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(TableName.valueOf(name))
    families.foreach { family =>
      val desc = new HColumnDescriptor(family)
      desc.setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.SNAPPY)
      desc.setDataBlockEncoding(org.apache.hadoop.hbase.io.encoding.DataBlockEncoding.FAST_DIFF)
      //will be available in 2.0
      //desc.setMobEnabled(true)
      tableDesc.addFamily(desc)
    }
    admin.createTable(tableDesc)
  }
  
  override def dropTable(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    if (!admin.tableExists(tableName))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    admin.disableTable(tableName)
    admin.deleteTable(tableName)
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
