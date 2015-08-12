/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.bigtable

/**
 * Abstraction of column data table.
 * 
 * @author Haifeng Li (293050)
 */
trait Table {
  type Key = (Array[Byte], Array[Byte], Array[Byte]) // (row, family, column)
  type Value = (Array[Byte], Long) // (value, timestamp)

  trait Scanner extends Iterator[Map[Key, Value]] {
    def close: Unit
    def hasNext: Boolean
    def next: Map[Key, Value]
  }

  /**
   * Visibility expression which can be associated with a cell.
   * When it is set with a Mutation, all the cells in that mutation will get associated with this expression.
   */
  def setCellVisibility(expression: String): Unit
  def getCellVisibility: Option[String]

  /**
   * Visibility labels associated with a Scan/Get deciding which all labeled data current scan/get can access.
   */
  def setAuthorizations(labels: String*): Unit
  def getAuthorizations: Option[Seq[String]]

  /**
   * Get all columns in one or more column families. If family is empty, get all column families.
   */
  def get(row: Array[Byte], families: Array[Byte]*): Map[Key, Value]
  /**
   * Get one or more columns. If columns is empty, get all columns in the column family.
   */
  def get(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Map[Key, Value]

  /**
   * Scan all columns in one ore more column families. If family is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Array[Byte]*): Scanner
  /**
   * Scan one or more columns. If columns is empty, get all columns in a column family
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Scanner

  /**
   * Update the values of a row.
   */
  def put(row: Array[Byte], family: Array[Byte], columns: (Array[Byte], Array[Byte])*): Unit

  /**
   * Update the values of one or more rows.
   */
  def put(values: (Key, Array[Byte])*): Unit

  /**
   * Delete the columns of a row.
   */
  def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]*): Unit

  /**
   * Update the values of one or more rows.
   */
  def delete(keys: Key*): Unit

  /**
   * Append the value of a column.
   */
  def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit

  /**
   * Increment a column.
   */
  def increment(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit
}
