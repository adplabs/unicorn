/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.bigtable

/**
 * A NoSQL database that include a number of datasets,
 * which don't have to support JOIN operation.
 * 
 * @author Haifeng Li
 */
trait Database extends AutoCloseable {
  /**
   * Returns a data set connection.
   * @param name the name of data set.
   */
  def getTable(name: String): Table
  /**
   * Creates a data set.
   * @param name the name of data set.
   * @param strategy the replica placement strategy.
   * @param replication the replication factor.
   * @param columnFamilies the column families in the data set.
   *   In some NoSQL solutions (e.g. HBase), column families are static
   *   and should be created when creating the table.
   */
  def createTable(name: String, strategy: String, replication: Int, columnFamilies: String*): Unit
  /**
   * Drops a data set.
   */
  def dropTable(name: String): Unit
}
