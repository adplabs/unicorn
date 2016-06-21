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

package unicorn.json

import java.util.Date
import scala.collection.mutable.ArrayBuffer
import unicorn.util._

/** Data frame of named columns.
  *
  * @param columnNames column names.
  * @param rows data rows.
  * @param explain optional string of operations (e.g. SQL statement) generating this data frame.
  *
  * @author Haifeng Li
  */
class DataFrame(val columnNames: Seq[String], val rows: Seq[Row], val explain: Option[String] = None) extends Traversable[Row] {
  override def toString: String = toString(10)
  override def copyToArray[B >: Row](xs: Array[B], start: Int, len: Int): Unit = rows.copyToArray(xs, start, len)
  override def find(p: (Row) => Boolean): Option[Row] = rows.find(p)
  override def exists(p: (Row) => Boolean): Boolean = rows.exists(p)
  override def forall(p: (Row) => Boolean): Boolean = rows.forall(p)
  override def foreach[U](p: (Row) => U): Unit = rows.foreach(p)
  override def hasDefiniteSize: Boolean = rows.hasDefiniteSize
  override def isEmpty: Boolean = rows.isEmpty
  override def seq: Traversable[Row] = rows.seq
  override def toIterator: Iterator[Row] = rows.toIterator
  override def toStream: Stream[Row] = rows.toStream
  override def size: Int = rows.size
  override def take(n: Int): Seq[Row] = rows.take(n)
  override def head(): Row = rows.head

  override def filter(f: Row => Boolean): Seq[Row] = rows.filter(f)

  def map(f: Row => Row): Seq[Row] = rows.map(f)
/*
  def groupBy(col: String): Map[JsValue, Seq[Row]] = {
    val i = columnNames.indexOf(col)
    require(i >= 0, s"Column $col doesn't exist")
    rows.groupBy(_(i))
  }

  def groupBy(col1: String, col2: String): Map[(JsValue, JsValue), Seq[Row]] = {
    val i = columnNames.indexOf(col1)
    require(i >= 0, s"Column $col1 doesn't exist")
    val j = columnNames.indexOf(col2)
    require(j >= 0, s"Column $col2 doesn't exist")
    rows.groupBy { row => (row(i), row(j)) }
  }

  def groupBy(col1: String, col2: String, col3: String): Map[(JsValue, JsValue, JsValue), Seq[Row]] = {
    val i = columnNames.indexOf(col1)
    require(i >= 0, s"Column $col1 doesn't exist")
    val j = columnNames.indexOf(col2)
    require(j >= 0, s"Column $col2 doesn't exist")
    val k = columnNames.indexOf(col3)
    require(k >= 0, s"Column $col3 doesn't exist")
    rows.groupBy { row => (row(i), row(j), row(k)) }
  }

  def orderBy(col: String, asc: Boolean = true): Seq[Row] = {
    val i = columnNames.indexOf(col)
    require(i >= 0, s"Column $col doesn't exist")
    val sorted = rows.sortBy(_(i))
    if (asc) sorted else sorted.reverse
  }

  def groupBy(col1: String, col2: String, asc: Boolean = true): Seq[Row] = {
    val i = columnNames.indexOf(col1)
    require(i >= 0, s"Column $col1 doesn't exist")
    val j = columnNames.indexOf(col2)
    require(j >= 0, s"Column $col2 doesn't exist")
    rows.groupBy { row => (row(i), row(j)) }
    val sorted = rows.sortBy { row => (row(i), row(j)) }
    if (asc) sorted else sorted.reverse
  }

  def groupBy(col1: String, col2: String, col3: String, asc: Boolean = true): Seq[Row] = {
    val i = columnNames.indexOf(col1)
    require(i >= 0, s"Column $col1 doesn't exist")
    val j = columnNames.indexOf(col2)
    require(j >= 0, s"Column $col2 doesn't exist")
    val k = columnNames.indexOf(col3)
    require(k >= 0, s"Column $col3 doesn't exist")
    val sorted = rows.sortBy { row => (row(i), row(j), row(k)) }
    if (asc) sorted else sorted.reverse
  }
*/
  /** Compose the string representing rows for output
    * @param numRows Number of rows to show
    * @param truncate Whether truncate long strings and align cells right
    */
  def toString(numRows: Int, truncate: Boolean = true): String = {
    require(numRows > 0, s"Invalid numRows: $numRows")

    val sb = new StringBuilder
    val hasMoreData = size > numRows
    val data = take(numRows)
    val numCols = columnNames.length

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond 20 characters, replace it with the first 17 and "..."
    val rows: Seq[Seq[String]] = columnNames +: data.map { row =>
      row.elements.map { cell =>
        val str = cell.toString
        if (truncate && str.length > 20) str.substring(0, 17) + "..." else str
      }
    }

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate) {
        leftPad(cell, colWidths(i))
      } else {
        rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        if (truncate) {
          leftPad(cell.toString, colWidths(i))
        } else {
          rightPad(cell.toString, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}

object DataFrame {
  def apply(columns: String*) = {
    new DataFrame(columns, new ArrayBuffer[Row]())
  }
}

/** A row in data frame. */
case class Row(elements: IndexedSeq[JsValue]) extends Traversable[JsValue] {
  override def toString(): String = elements.mkString("[", ",", "]")

  override def copyToArray[B >: JsValue](xs: Array[B], start: Int, len: Int): Unit = elements.copyToArray(xs, start, len)
  override def find(p: (JsValue) => Boolean): Option[JsValue] = elements.find(p)
  override def exists(p: (JsValue) => Boolean): Boolean = elements.exists(p)
  override def forall(p: (JsValue) => Boolean): Boolean = elements.forall(p)
  override def foreach[U](p: (JsValue) => U): Unit = elements.foreach(p)
  override def hasDefiniteSize: Boolean = elements.hasDefiniteSize
  override def isEmpty: Boolean = elements.isEmpty
  override def seq: Traversable[JsValue] = elements.seq
  override def toIterator: Iterator[JsValue] = elements.toIterator
  override def toStream: Stream[JsValue] = elements.toStream

  /** Number of elements in the Row. */
  override def size: Int = elements.size

  /** Returns the value at position i. */
  def apply(i: Int): JsValue = elements(i)

  /** Returns the value at position i. */
  def get(i: Int): JsValue = elements(i)

  /** Checks whether the value at position i is null or undefined. */
  def isNullAt(i: Int): Boolean = {
    val v = elements(i)
    v == JsNull || v == JsUndefined
  }

  /** Returns the value at position i as a primitive boolean. */
  def getBoolean(i: Int): Boolean = elements(i).asBoolean

  /** Returns the value at position i as a primitive int. */
  def getInt(i: Int): Int = elements(i).asInt

  /** Returns the value at position i as a primitive long. */
  def getLong(i: Int): Long = elements(i).asLong

  /** Returns the value at position i as a primitive double. */
  def getDouble(i: Int): Double = elements(i).asDouble

  /** Returns the value at position i as a String object. */
  def getString(i: Int): String = elements(i).toString

  /** Returns the value at position i of date type as java.util.Date. */
  def getDate(i: Int): Date = elements(i).asDate

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    elements.exists { v =>
      v == JsNull || v == JsUndefined
    }
  }
}

object Row {
  def apply(values: JsValue*) = new Row(values.toIndexedSeq)
  def fromSeq(seq: Seq[JsValue]) = new Row(seq.toIndexedSeq)
  val empty = new Row(IndexedSeq.empty)
}