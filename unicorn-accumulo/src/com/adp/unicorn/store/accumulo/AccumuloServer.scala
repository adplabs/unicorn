/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.accumulo


import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import com.adp.unicorn.store.DataStore
import com.adp.unicorn.store.DataSet

/**
 * Accumulo server adapter.
 * 
 * @author Haifeng Li (293050)
 */
class AccumuloServer(conn: Connector) extends DataStore {
  override def dataset(name: String, auth: String): DataSet = {
    new AccumuloTable(conn, name, auth)
  }
  
  override def createDataSet(name: String): DataSet = {
    createDataSet(name, "", 1)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): DataSet = {
    if (conn.tableOperations.exists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")

    conn.tableOperations.create(name)
    dataset(name)
  }
  
  override def dropDataSet(name: String): Unit = {
    if (!conn.tableOperations.exists(name))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    conn.tableOperations.delete(name)
  }
}

object AccumuloServer {
  def apply(instance: String, zookeeper: String, user: String, password: String): AccumuloServer = {
    val inst = new ZooKeeperInstance(instance, zookeeper)
    val conn = inst.getConnector(user, new PasswordToken(password))
    new AccumuloServer(conn)
  }
}
