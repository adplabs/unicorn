/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.accumulo

import scala.collection.JavaConversions._
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import unicorn.bigtable.BigTable

/**
 * Accumulo server adapter.
 * 
 * @author Haifeng Li
 */
class Accumulo(val connector: Connector) extends unicorn.bigtable.Database {
  override def close: Unit = () // Connector has no close method

  override def apply(name: String): BigTable = {
    new AccumuloTable(this, name)
  }
  
  override def createTable(name: String, strategy: String, replication: Int, families: String*): Unit = {
    if (connector.tableOperations.exists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")

    connector.tableOperations.create(name)

    val localityGroups = families.map { family =>
      val set = new java.util.TreeSet[Text]()
      set.add(new Text(family))
      (family, set)
    }.toMap

    connector.tableOperations().setLocalityGroups(name, localityGroups)
  }
  
  override def dropTable(name: String): Unit = {
    if (!connector.tableOperations.exists(name))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    connector.tableOperations.delete(name)
  }
}

object Accumulo {
  def apply(instance: String, zookeeper: String, user: String, password: String): Accumulo = {
    val inst = new ZooKeeperInstance(instance, zookeeper)
    val conn = inst.getConnector(user, new PasswordToken(password))
    new Accumulo(conn)
  }
}
