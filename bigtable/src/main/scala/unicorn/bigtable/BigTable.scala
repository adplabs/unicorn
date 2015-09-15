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

package unicorn.bigtable

import unicorn.util.utf8

/** Cell in wide columnar table */
case class Cell(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long = 0)
/** A column of a column family */
case class Column(qualifier: Array[Byte], value: Array[Byte], timestamp: Long = 0)
/** A column family */
case class ColumnFamily(family: Array[Byte], columns: Seq[Column])
/** A Row */
case class Row(row: Array[Byte], families: Seq[ColumnFamily])

/**
 * Abstraction of wide columnar data table.
 *
 * @author Haifeng Li
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
    get(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8))
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
    get(row.getBytes(utf8), Seq())
  }

  /**
   * Get all columns in one or more column families. If families is empty, get all column families.
   */
  def get(row: Array[Byte], families: Seq[Array[Byte]]): Seq[ColumnFamily]

  /**
   * Get all columns in one or more column families. If families is empty, get all column families.
   */
  def get(row: String, families: Seq[String]): Seq[ColumnFamily] = {
    get(row.getBytes(utf8), families.map(_.getBytes(utf8)))
  }

  /**
   * Get the column family.
   */
  def get(row: Array[Byte], family: Array[Byte]): Seq[Column]

  /**
   * Get the column family.
   */
  def get(row: String, family: String): Seq[Column] = {
    get(row.getBytes(utf8), family.getBytes(utf8))
  }

  /**
   * Get one or more columns of a column family. If columns is empty, get all columns in the column family.
   */
  def get(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Seq[Column]

  /**
   * Get one or more columns of a column family. If columns is empty, get all columns in the column family.
   */
  def get(row: String, family: String, columns: Seq[String]): Seq[Column] = {
    get(row.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)))
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
   * Upsert a value.
   */
  def put(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit

  /**
   * Upsert a value.
   */
  def put(row: String, family: String, column: String, value: String): Unit = {
    put(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8), value.getBytes(utf8))
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
    delete(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8))
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
    delete(row.getBytes(utf8), Seq())
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
    delete(row.getBytes(utf8), family.getBytes(utf8))
  }

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  def delete(row: Array[Byte], families: Seq[Array[Byte]]): Unit

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  def delete(row: String, families: Seq[String]): Unit = {
    delete(row.getBytes(utf8), families.map(_.getBytes(utf8)))
  }

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  def delete(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  def delete(row: String, family: String, columns: Seq[String]): Unit = {
    delete(row.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)))
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

/** Row scan iterator */
trait RowScanner extends Iterator[Row] {
  def close: Unit
}

/** If BigTable supports row scan. */
trait RowScan {
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
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), Seq())
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
  def scan(startRow: String, stopRow: String, families: Seq[String]): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), families.map(_.getBytes(utf8)))
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
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), family.getBytes(utf8), Seq())
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
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)))
  }
}


/** If BigTable supports scanning rows with given prefix. */
trait PrefixScan {
  /**
   * Scan all column families.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: Array[Byte]): RowScanner = {
    prefixScan(prefix, Seq())
  }

  /**
   * Scan the range for all column families.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: String): RowScanner = {
    prefixScan(prefix.getBytes(utf8), Seq())
  }

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: Array[Byte], families: Seq[Array[Byte]]): RowScanner

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: String, families: Seq[String] = Seq()): RowScanner = {
    prefixScan(prefix.getBytes(utf8), families.map(_.getBytes(utf8)))
  }

  /**
   * Scan the range for the column family.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: Array[Byte], family: Array[Byte]): RowScanner = {
    prefixScan(prefix, family, Seq())
  }

  /**
   * Scan the range for the column family.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: String, family: String): RowScanner = {
    prefixScan(prefix.getBytes(utf8), family.getBytes(utf8), Seq())
  }

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): RowScanner

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param prefix returns rows which key starts with the specified prefix
   */
  def prefixScan(prefix: String, family: String, columns: Seq[String]): RowScanner = {
    prefixScan(prefix.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)))
  }
}

/** Intra-row scan iterator */
trait IntraRowScanner extends Iterator[Column] {
  def close: Unit
}

/** If BigTable supports intra-row scan. */
trait IntraRowScan {
  /**
   * Scan a column range for a given row.
   * @param startColumn column to start scanner at or after (inclusive)
   * @param stopColumn column to stop scanner before or at (inclusive)
   */
  def intraRowScan(row: Array[Byte], family: Array[Byte], startColumn: Array[Byte], stopColumn: Array[Byte]): IntraRowScanner

  /**
   * Scan a column range for a given row.
   * @param startColumn column to start scanner at or after (inclusive)
   * @param stopColumn column to stop scanner before or at (inclusive)
   */
  def intraRowScan(row: String, family: String, startColumn: String, stopColumn: String): IntraRowScanner = {
    intraRowScan(row.getBytes(utf8), family.getBytes(utf8), startColumn.getBytes(utf8), stopColumn.getBytes(utf8))
  }
}

/** If BigTable supports filter. */
trait Filter {
  /**
   * Scan all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], filter: String): RowScanner = {
    scan(startRow, stopRow, Seq(), filter)
  }

  /**
   * Scan all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], filter: FilterExpression): RowScanner = {
    scan(startRow, stopRow, Seq(), filter)
  }

  /**
   * Scan the range for all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, filter: String): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), Seq(), filter)
  }

  /**
   * Scan the range for all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, filter: FilterExpression): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), Seq(), filter)
  }

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Seq[Array[Byte]], filter: String): RowScanner = {
    scan(startRow, stopRow, families, FilterExpression(filter))
  }

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Seq[Array[Byte]], filter: FilterExpression): RowScanner

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, families: Seq[String], filter: String): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), families.map(_.getBytes(utf8)), filter)
  }

  /**
   * Scan the range for all columns in one or more column families. If families is empty, get all column families.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, families: Seq[String], filter: FilterExpression): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), families.map(_.getBytes(utf8)), filter)
  }

  /**
   * Scan the range for the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], filter: String): RowScanner = {
    scan(startRow, stopRow, family, Seq(), filter)
  }

  /**
   * Scan the range for the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], filter: FilterExpression): RowScanner = {
    scan(startRow, stopRow, family, Seq(), filter)
  }

  /**
   * Scan the range for the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, family: String, filter: String): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), family.getBytes(utf8), Seq(), filter)
  }

  /**
   * Scan the range for the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, family: String, filter: FilterExpression): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), family.getBytes(utf8), Seq(), filter)
  }

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]], filter: String): RowScanner = {
    scan(startRow, stopRow, family, columns, FilterExpression(filter))
  }

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]], filter: FilterExpression): RowScanner

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, family: String, columns: Seq[String], filter: String): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)), filter)
  }

  /**
   * Scan one or more columns. If columns is empty, get all columns in the column family.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   * @param filter filter expression
   */
  def scan(startRow: String, stopRow: String, family: String, columns: Seq[String], filter: FilterExpression): RowScanner = {
    scan(startRow.getBytes(utf8), stopRow.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)), filter)
  }
}


/** If BigTable supports cell level security. */
trait CellLevelSecurity {
  /**
   * Visibility expression which can be associated with a cell.
   * When it is set with a Mutation, all the cells in that mutation will get associated with this expression.
   */
  def setCellVisibility(expression: String): Unit

  /**
   * Returns the current visibility expression setting.
   */
  def getCellVisibility: String

  /**
   * Visibility labels associated with a Scan/Get deciding which all labeled data current scan/get can access.
   */
  def setAuthorizations(labels: String*): Unit

  /**
   * Returns the current authorization labels.
   */
  def getAuthorizations: Seq[String]
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
    rollback(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8))
  }

  /**
   * Rollback to the previous version for the given columns of a row.
   */
  def rollback(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit

  /**
   * Rollback to the previous version for the given column(s) of a row.
   */
  def rollback(row: String, family: String, columns: Seq[String]): Unit = {
    rollback(row.getBytes(utf8), family.getBytes(utf8), columns.map(_.getBytes(utf8)))
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
    append(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8), value)
  }
}

/** If BigTable supports counter data type. */
trait Counter {
  /** Get the value of a counter column */
  def getCounter(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Long

  /** Get the value of a counter column */
  def getCounter(row: String, family: String, column: String): Long = {
    getCounter(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8))
  }

  /**
   * Add to a counter column.
   */
  def addCounter(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit

  /**
   * Add to a counter column.
   */
  def addCounter(row: String, family: String, column: String, value: Long): Unit = {
    addCounter(row.getBytes(utf8), family.getBytes(utf8), column.getBytes(utf8), value)
  }
}
