/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package com.adp.unicorn.store.accumulo

import com.adp.unicorn.store._
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken

/**
 * Accumulo server adapter.
 * 
 * @author Haifeng Li
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
