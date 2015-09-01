/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.bigtable

import java.nio.charset.Charset

/** Cell in wide columnar table */
case class Cell(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long = 0)
/** A column of a column family */
case class Column(qualifier: Array[Byte], value: Array[Byte], timestamp: Long = 0)
/** A column family */
case class ColumnFamily(family: Array[Byte], columns: Seq[Column])
/** A Row */
case class Row(row: Array[Byte], families: Seq[ColumnFamily])

/** Big table row scanner */
trait RowScanner extends Iterator[Row] {
  def close: Unit
  def hasNext: Boolean
  def next: Row
}

/**
 * Abstraction of wide columnar data table.
 *
 * @author Haifeng Li (293050)
 */
trait BigTable extends AutoCloseable {
  def db: Database

  def name: String

  /**
   * Get a value.
   */
  def get(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Option[Array[Byte]]

  /**
   * Get a value.
   */
  def get(row: String, family: String, column: String): Option[Array[Byte]] = {
    get(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset))
  }

  // Scala compiler disallows overloaded methods with default arguments
  // because of a deterministic naming-scheme for the methods with
  // default arguments. So instead of define a method like
  // get(row: Array[Byte], families: Seq[Array[Byte]] = Seq())
  // we have to manually define two methods and provide the default
  // parameter by ourselves.
  /**
   * Get the row.
   */
  def get(row: Array[Byte]): Seq[ColumnFamily] = {
    get(row, Seq())
  }

  /**
   * Get the row.
   */
  def get(row: String): Seq[ColumnFamily] = {
    get(row.getBytes(BigTable.charset), Seq())
  }

  /**
   * Get all columns in one or more column families. If families is empty, get all column families.
   */
  def get(row: Array[Byte], families: Seq[Array[Byte]]): Seq[ColumnFamily]

  /**
   * Get all columns in one or more column families. If families is empty, get all column families.
   */
  def get(row: String, families: Seq[String]): Seq[ColumnFamily] = {
    get(row.getBytes(BigTable.charset), families.map(_.getBytes(BigTable.charset)))
  }

  /**
   * Get the column family.
   */
  def get(row: Array[Byte], family: Array[Byte]): Seq[Column]

  /**
   * Get the column family.
   */
  def get(row: String, family: String): Seq[Column] = {
    get(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset))
  }

  /**
   * Get one or more columns of a column family. If columns is empty, get all columns in the column family.
   */
  def get(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Column]

  /**
   * Get one or more columns of a column family. If columns is empty, get all columns in the column family.
   */
  def get(row: String, family: String, columns: Seq[String]): Seq[Column] = {
    get(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), columns.map(_.getBytes(BigTable.charset)))
  }

  /**
   * Get multiple rows.
   */
  def get(rows: Seq[Array[Byte]]): Seq[Row] = {
    get(rows, Seq())
  }

  /**
   * Get multiple rows for given column families. If families is empty, get all column families.
   */
  def get(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Seq[Row]

  /**
   * Get multiple rows for given columns. If columns is empty, get all columns of the column family.
   */
  def get(rows: Seq[Array[Byte]], family: Array[Byte]): Seq[Row] = {
    get(rows, family, Seq())
  }

  /**
   * Get multiple rows for given columns. If columns is empty, get all columns of the column family.
   */
  def get(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Row]

  /**
   * Scan all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte]): RowScanner = {
    scan(startRow, stopRow, Seq())
  }

  /**
   * Scan the range for all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: String, stopRow: String): RowScanner = {
    scan(startRow.getBytes(BigTable.charset), stopRow.getBytes(BigTable.charset), Seq())
  }

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Seq[Array[Byte]]): RowScanner

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: String, stopRow: String, families: Seq[String] = Seq()): RowScanner = {
    scan(startRow.getBytes(BigTable.charset), stopRow.getBytes(BigTable.charset), families.map(_.getBytes(BigTable.charset)))
  }

  /**
   * Scan the range for the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte]): RowScanner = {
    scan(startRow, stopRow, family, Seq())
  }

  /**
   * Scan the range for the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: String, stopRow: String, family: String): RowScanner = {
    scan(startRow.getBytes(BigTable.charset), stopRow.getBytes(BigTable.charset), family.getBytes(BigTable.charset), Seq())
  }

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): RowScanner

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  def scan(startRow: String, stopRow: String, family: String, columns: Seq[String]): RowScanner = {
    scan(startRow.getBytes(BigTable.charset), stopRow.getBytes(BigTable.charset), family.getBytes(BigTable.charset), columns.map(_.getBytes(BigTable.charset)))
  }

  /**
   * Upsert a value.
   */
  def put(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit

  /**
   * Upsert a value.
   */
  def put(row: String, family: String, column: String, value: String): Unit = {
    put(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset), value.getBytes(BigTable.charset))
  }

  /**
   * Upsert values.
   */
  def put(row: Array[Byte], family: Array[Byte], columns: Column*): Unit

  /**
   * Upsert values.
   */
  def put(row: Array[Byte], families: ColumnFamily*): Unit

  /**
   * Update the values of one or more rows.
   */
  def put(rows: Row*): Unit

  /**
   * Delete a value.
   */
  def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit

  /**
   * Delete a value.
   */
  def delete(row: String, family: String, column: String): Unit = {
    delete(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset))
  }

  /**
   * Delete a row.
   */
  def delete(row: Array[Byte]): Unit = {
    delete(row, Seq())
  }

  /**
   * Delete a row.
   */
  def delete(row: String): Unit = {
    delete(row.getBytes(BigTable.charset), Seq())
  }

  /**
   * Delete all columns of a column family of a row.
   */
  def delete(row: Array[Byte], family: Array[Byte]): Unit = {
    delete(row, Seq(family))
  }

  /**
   * Delete all columns of a column family of a row.
   */
  def delete(row: String, family: String): Unit = {
    delete(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset))
  }

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  def delete(row: Array[Byte], families: Seq[Array[Byte]]): Unit

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  def delete(row: String, families: Seq[String]): Unit = {
    delete(row.getBytes(BigTable.charset), families.map(_.getBytes(BigTable.charset)))
  }

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  def delete(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  def delete(row: String, family: String, columns: Seq[String]): Unit = {
    delete(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), columns.map(_.getBytes(BigTable.charset)))
  }

  /**
   * Delete multiple rows.
   */
  def delete(rows: Seq[Array[Byte]]): Unit = {
    delete(rows, Seq())
  }

  /**
   * Delete multiple rows.
   */
  def delete(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Unit

  /**
   * Delete multiple rows.
   */
  def delete(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Unit
}

/** If BigTable supports cell level security. */
trait CellLevelSecurity {
  /**
   * Visibility expression which can be associated with a cell.
   * When it is set with a Mutation, all the cells in that mutation will get associated with this expression.
   */
  def setCellVisibility(expression: String): Unit

  /**
   * HBase supports cell level security since 0.98. But it is not mandatory.
   */
  def getCellVisibility: Option[String]

  /**
   * Visibility labels associated with a Scan/Get deciding which all labeled data current scan/get can access.
   */
  def setAuthorizations(labels: String*): Unit

  /**
   * HBase supports cell level security since 0.98. But it is not mandatory.
   */
  def getAuthorizations: Option[Seq[String]]
}

/** If BigTable supports rollback to previous version of a cell. */
trait Rollback {
  /**
   * Rollback to the previous version for the given column of a row.
   */
  def rollback(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit

  /**
   * Rollback to the previous version for the given column of a row.
   */
  def rollback(row: String, family: String, column: String): Unit = {
    rollback(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset))
  }

  /**
   * Rollback to the previous version for the given columns of a row.
   */
  def rollback(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit

  /**
   * Rollback to the previous version for the given column(s) of a row.
   */
  def rollback(row: String, family: String, columns: Seq[String]): Unit = {
    rollback(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), columns.map(_.getBytes(BigTable.charset)))
  }
}

/** If BigTable supports appending to a cell. */
trait Appendable {
  /**
   * Append the value of a column.
   */
  def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit

  /**
   * Append the value of a column.
   */
  def append(row: String, family: String, column: String, value: Array[Byte]): Unit = {
    append(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset), value)
  }
}

/** If BigTable supports counter data type. */
trait Counter {
  /** Get the value of a counter column */
  def getCounter(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Long

  /** Get the value of a counter column */
  def getCounter(row: String, family: String, column: String): Long = {
    getCounter(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset))
  }

  /**
   * Add to a counter column.
   */
  def addCounter(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit

  /**
   * Add to a counter column.
   */
  def addCounter(row: String, family: String, column: String, value: Long): Unit = {
    addCounter(row.getBytes(BigTable.charset), family.getBytes(BigTable.charset), column.getBytes(BigTable.charset), value)
  }
}

object BigTable {
  /** string encoder/decoder */
  val charset = Charset.forName("UTF-8")
}