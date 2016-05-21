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

package unicorn.narwhal

import unicorn.bigtable.hbase.HBase
import unicorn.oid.{Snowflake, LongIdGenerator}
import unicorn.unibase.graph.Graph
import unicorn.unibase.{TableMeta, Unibase}
import unicorn.narwhal.graph.GraphX

/** Unibase specialized for HBase. */
class Narwhal(hbase: HBase) extends Unibase(hbase) {
  /** Returns a document table.
    * @param name the name of table.
    */
  override def apply(name: String): HTable = {
    new HTable(hbase(name), TableMeta(hbase, name))
  }

  /** Returns a graph table with Snowflake ID generator.
    * The Snowflake worker id is coordinated by the
    * ZooKeeper instance used by HBase.
    *
    * @param name the name of table.
    */
  def graph(name: String): GraphX = {
    val zookeeper = hbase.connection.getConfiguration.get("hbase.zookeeper.quorum")
    graph(name, zookeeper)
  }

  /** Returns a graph table with Snowflake ID generator.
    * The Snowflake worker id is coordinated by zookeeper.
    *
    * @param name The name of graph table.
    * @param zookeeper ZooKeeper connection string.
    */
  override def graph(name: String, zookeeper: String): GraphX = {
    graph(name, zookeeper, s"/unicorn/snowflake/graph/$name")
  }

  /** Returns a graph table with Snowflake ID generator.
    * The Snowflake worker id is coordinated by zookeeper.
    *
    * @param name The name of graph table.
    * @param zookeeper ZooKeeper connection string.
    * @param group The ZooKeeper group node of Snowflake workers.
    */
  override def graph(name: String, zookeeper: String, group: String): GraphX = {
    graph(name, Snowflake(zookeeper, group))
  }

  override def graph(name: String, idgen: LongIdGenerator): GraphX = {
    val table = hbase(name)
    new GraphX(table, idgen)
  }

/*
  /** Returns a document table.
    * @param name the name of table.
    */
  def getTableWithIndex(name: String): HTableWithIndex = {
    new HTableWithIndex(hbase.getTableWithIndex(name), TableMeta(hbase, name))
  }
  */
}
