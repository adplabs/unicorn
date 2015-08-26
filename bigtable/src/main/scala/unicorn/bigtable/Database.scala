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
   * Returns a table.
   * @param name the name of table.
   */
  def apply(name: String): BigTable
  /**
   * Creates a table.
   * @param name the name of table.
   * @param strategy the replica placement strategy.
   * @param replication the replication factor.
   * @param families the column families in the table.
   *   In some NoSQL solutions (e.g. HBase), column families are static
   *   and should be created when creating the table.
   */
  def createTable(name: String, strategy: String, replication: Int, families: String*): Unit
  /**
   * Drops a table.
   */
  def dropTable(name: String): Unit
}
