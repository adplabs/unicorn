/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store.accumulo

import com.adp.unicorn.store._
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken

/**
 * Accumulo server adapter.
 * 
 * @author Haifeng Li (293050)
 */
class AccumuloServer(conn: Connector) extends Database {
  override def dataset(name: String, visibility: Option[String], authorizations: Option[Seq[String]]): Dataset = {
    new AccumuloTable(conn, name, visibility, authorizations)
  }
  
  override def createDataSet(name: String): Unit = {
    createDataSet(name, "", 1)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): Unit = {
    if (conn.tableOperations.exists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")

    conn.tableOperations.create(name)
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
