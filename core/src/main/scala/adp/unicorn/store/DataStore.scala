/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.store

/**
 * A NoSQL data store that include a number of data sets,
 * which don't have to support JOIN operation.
 * 
 * @author Haifeng Li (293050)
 */
trait DataStore {
  /**
   * Returns a data set connection.
   * @param name the name of data set.
   * @param visibility the visibility label expression (for mutations).
   * @param authorizations the authorization labels (for read).
   */
  def dataset(name: String, visibility: String, authorizations: String*): DataSet
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