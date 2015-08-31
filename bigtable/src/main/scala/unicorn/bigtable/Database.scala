/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.bigtable

import java.util.Properties

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
   * @param families the column families in the table. A column family name
   *   must be printable -- digit or letter -- and may not contain a :.
   *   In analogy with relational databases, a column family is as a "table".
   *   In some NoSQL solutions (e.g. HBase), column families are static
   *   and should be created when creating the table.
   */
  def createTable(name: String, families: String*): BigTable
  /**
   * Creates a table.
   * @param name the name of table.
   * @param props table configurations.
   * @param families the column families in the table. A column family name
   *   must be printable -- digit or letter -- and may not contain a :.
   *   In analogy with relational databases, a column family is as a "table".
   *   In some NoSQL solutions (e.g. HBase), column families are static
   *   and should be created when creating the table.
   */
  def createTable(name: String, props: Properties, families: String*): BigTable

  /**
   * Truncates a table
   * @param name the name of table.
   */
  def truncateTable(name: String)
  /**
   * Drops a table.
   */
  def dropTable(name: String): Unit

  /**
   * Compacts a table
   * @param name the name of table.
   */
  def compactTable(name: String): Unit

  /**
   * Major compacts a table
   * @param name the name of table.
   */
  def majorCompactTable(name: String): Unit
}
