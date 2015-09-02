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

package unicorn.index

import unicorn.bigtable._

/**
 * Indexed table.
 * 
 * @author Haifeng Li
 */
class IndexedTable(table: BigTable) extends BigTable {
  val indexTable = table.db(super.name + "_Index")

  override def db: Database = table.db
  override def name: String = table.name

  override def close: Unit = {
    table.close
    indexTable.close
  }

  override def setCellVisibility(expression: String): Unit = {
    table.setCellVisibility(expression)
  }

  override def getCellVisibility: Option[String] = {
    table.getCellVisibility
  }

  override def setAuthorizations(labels: String*): Unit = {
    table.setAuthorizations(labels: _*)
  }

  override def getAuthorizations: Option[Seq[String]] = {
    table.getAuthorizations
  }

  override def get(row: Array[Byte]): Map[Key, Value] = {
    table.get(row)
  }

  override def get(row: Array[Byte], family: Array[Byte]): Map[Key, Value] = {
    table.get(row, family)
  }

  override def get(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Map[Key, Value] = {
    table.get(row, family, columns: _*)
  }

  override def get(keys: Key*): Map[Key, Value] = {
    table.get(keys: _*)
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte]): RowScanner = {
    table.scan(startRow, stopRow, family)
  }

  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Array[Byte]*): RowScanner = {
    table.scan(startRow, stopRow, family, columns: _*)
  }

  override def put(row: Array[Byte], family: Array[Byte], columns: (Array[Byte], Array[Byte])*): Unit = {
    table.put(row, family, columns: _*)
  }

  override def put(values: (Key, Array[Byte])*): Unit = {
    table.put(values: _*)
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    table.delete(row, family, columns: _*)
  }

  override def delete(keys: Key*): Unit = {
    table.delete(keys: _*)
  }

  override def delete(row: Array[Byte]): Unit = {
    table.delete(row)
  }

  override def rollback(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    table.rollback(row, family, columns: _*)
  }

  override def rollback(keys: Key*): Unit = {
    table.rollback(keys: _*)
  }

  override def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    table.append(row, family, column, value)
  }

  override def increment(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit = {
    table.increment(row, family, column, value)
  }
}
