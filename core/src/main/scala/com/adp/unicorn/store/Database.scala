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

package com.adp.unicorn.store

/**
 * A NoSQL database that include a number of datasets,
 * which don't have to support JOIN operation.
 * 
 * @author Haifeng Li
 */
trait Database {
  /**
   * Returns a data set connection.
   * @param name the name of data set.
   * @param visibility the visibility label expression (for mutations).
   * @param authorizations the authorization labels (for read).
   */
  def dataset(name: String, visibility: Option[String] = None, authorizations: Option[Seq[String]] = None): Dataset
  /**
   * Creates a data set with default settings.
   */
  def createDataSet(name: String): Unit
  /**
   * Creates a data set.
   * @param name the name of data set.
   * @param strategy the replica placement strategy.
   * @param replication the replication factor.
   * @param columnFamilies the column families in the data set.
   *   In some NoSQL solutions (e.g. HBase), column families are static
   *   and should be created when creating the table.
   */
  def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): Unit
  /**
   * Drops a data set.
   */
  def dropDataSet(name: String): Unit
}