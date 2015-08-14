/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.accumulo

import scala.collection.JavaConversions._
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken

/**
 * Accumulo server adapter.
 * 
 * @author Haifeng Li
 */
class Accumulo(conn: Connector) extends unicorn.bigtable.Database {
  override def close: Unit = () // Connector has no close method

  override def apply(name: String): unicorn.bigtable.Table = {
    new AccumuloTable(conn, name)
  }
  
  override def createTable(name: String, strategy: String, replication: Int, families: String*): Unit = {
    if (conn.tableOperations.exists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")

    conn.tableOperations.create(name)

    val localityGroups = families.map { family =>
      val set = new java.util.TreeSet[Text]()
      set.add(new Text(family))
      (family, set)
    }.toMap

    conn.tableOperations().setLocalityGroups(name, localityGroups)
  }
  
  override def dropTable(name: String): Unit = {
    if (!conn.tableOperations.exists(name))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    conn.tableOperations.delete(name)
  }
}

object Accumulo {
  def apply(instance: String, zookeeper: String, user: String, password: String): Accumulo = {
    val inst = new ZooKeeperInstance(instance, zookeeper)
    val conn = inst.getConnector(user, new PasswordToken(password))
    new Accumulo(conn)
  }
}
