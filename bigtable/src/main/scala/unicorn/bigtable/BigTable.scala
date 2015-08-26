/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.bigtable

/**
 * Cell Key. In original BigTable design, timestamp is treated as part of key.
 * However, we treat it as part of value in this abstraction and the timestamp
 * is always implicitly set by the database server rather than the client.
 * This makes the API simple and we always retrieve the last value. Meanwhile,
 * the value/timestamp pair is like a stack, which we can rollback to previous
 * value. Beside, the timestamp may be used in a MVCC implementation to provide
 * ACID transaction. In that situation, it is harder to use timestamps in regular
 * time series or multi-versioning approach. However, we can easily implement
 * time series in other modeling.
 */
case class Key(row: Array[Byte], family: Array[Byte], column: Array[Byte])
/** Cell Value */
case class Value(value: Array[Byte], timestamp: Long)
/** Cell in wide columnar table */
case class Cell(key: Key, value: Value)

/** Big table scanner */
trait Scanner extends Iterator[Map[Key, Value]] {
  def close: Unit
  def hasNext: Boolean
  def next: Map[Key, Value]
}

/**
 * Abstraction of column data table.
 *
 * @author Haifeng Li (293050)
 */
trait BigTable extends AutoCloseable {
  def db: Database
  def name: String

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
   * Get the entire row.
   */
  def get(row: Array[Byte]): Map[Key, Value]
  /**
   * Get all columns in one or more column families. If family is empty, get all column families.
   */
  def get(row: Array[Byte], families: Array[Byte]): Map[Key, Value]
  /**
   * Get one or more columns. If columns is empty, get all columns in the column family.
   */
  def get(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Map[Key, Value]

  /**
   * Batch get multiple rows.
   */
  def get(keys: Key*): Map[Key, Value]

  /**
   * Scan all columns in a column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte]): Scanner
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
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  def delete(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit

  /**
   * Delete the whole row.
   */
  def delete(row: Array[Byte]): Unit

  /**
   * Delete the values of one or more rows.
   */
  def delete(keys: Key*): Unit

  /**
   * Rollback to the previous version for the given columns of a row. Columns must not be empty.
   */
  def rollback(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit

  /**
   * Rollback to the previous version for one or more rows.
   */
  def rollback(keys: Key*): Unit

  /**
   * Append the value of a column.
   */
  def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit

  /**
   * Increment a column.
   */
  def increment(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit
}
