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

import scala.collection.mutable.ArrayBuffer
import unicorn.bigtable._
import unicorn.json._
import unicorn.util._

trait Indexing {
  type IndexTable = BigTable with RowScan with Counter

  val metaTableName = "unicorn.meta.index"
  val metaTableColumnFamily = "meta"
  val metaTableColumnFamilyBytes = metaTableColumnFamily.getBytes(utf8)
  val indexTableIndexColumnFamily = "index"
  val indexTableStatColumnFamily = "stat"
  val indexValue = Array[Byte](1)

  /** Base table */
  val baseTable: IndexTable
  val db: Database[IndexTable]

  val indexMeta = getIndexMeta

  /** Close the base table and the index table */
  def close: Unit = {
    baseTable.close
    indexMeta.foreach { case (_, indexTable) => indexTable.close }
  }

  def createIndex(name: String, index: Index): Unit = {
    indexMeta.foreach { case (index, indexTable) =>
      if (index.indexTableName == name) throw new IllegalArgumentException(s"Index $name exists")
      if (index.coverSameColumns(index)) throw new IllegalArgumentException(s"Index ${index.name} covers the same columns")
    }

    val indexTable = db.createTable(name, IndexMeta.indexColumnFamilies: _*)
    baseTable.scan(baseTable.startRowKey, baseTable.endRowKey, index.family, index.columns.map(_.qualifier)).foreach { row =>
      insertIndex(index, indexTable, row)
    }

    indexMeta.append((index, indexTable))
    addIndex(index)
  }

  def dropIndex(name: String): Unit = {
    val index = indexMeta.zipWithIndex.find(_._1._1.indexTableName == name)

    if (index.isDefined) {
      indexMeta.remove(index.get._2)
      db.dropTable(name)
    }
  }

  def insertIndex(index: Index, indexTable: IndexTable, row: Row): Unit = {
    val map = row.families.map { family =>
      val columns = family.columns.map { column => (ByteArray(column.qualifier), column) }.toMap
      (ByteArray(family.family), columns)
    }.toMap

    val cells = index.codec(row.row, map)
    cells.foreach { cell =>
      indexTable.put(cell.row, cell.family, cell.qualifier, cell.value, cell.timestamp)
    }
  }

  def deleteIndex(index: Index, indexTable: IndexTable, row: Row): Unit = {
    val map = row.families.map { family =>
      val columns = family.columns.map { column => (ByteArray(column.qualifier), column) }.toMap
      (ByteArray(family.family), columns)
    }.toMap

    val cells = index.codec(row.row, map)
    cells.foreach { cell =>
      indexTable.delete(cell.row, cell.family, cell.qualifier)
    }
  }

  /**
   * Upsert a value.
   */
  def put(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte], timestamp: Long = 0L): Unit = {
    val missing = indexMeta.flatMap { case (index, indexTable) =>
      if (compareByteArray(family, index.family) == 0) {
        val missing = index.findNonCoveredColumns(column)
        missing.getOrElse(Seq())
      } else Seq()
    }

    val missingColumns = if (!missing.isEmpty) baseTable.get(row, family, missing) else Seq()

    if (indexed.isDefined) {
      val old = baseTable.get(row, family, column)
      if (old.isDefined) {
        val data = Row(row, Seq(ColumnFamily(family, Seq(Column(column, old.get)))))
        indexed.get.foreach { case (index, indexTable) =>
          deleteIndex(index, indexTable, data)
        }
      }
    }

    baseTable.put(row, family, column, value, timestamp)

    if (indexed.isDefined) {
      val data = Row(row, Seq(ColumnFamily(family, Seq(Column(column, value, timestamp)))))
      indexed.get.foreach { case (index, indexTable) =>
          insertIndex(index, indexTable, data)
      }
    }
  }

  /**
   * Upsert values.
   */
  def put(row: Array[Byte], family: Array[Byte], columns: Column*): Unit = {
    baseTable.put(row, family, columns: _*)
    val data = Row(row, Seq(ColumnFamily(family, columns)))
    columns.foreach { column =>
      val indexed = indexTables.get((family, column.qualifier))
      if (indexed.isDefined) {
        indexed.get.foreach { case (index, indexTable) =>
          insertIndex(index, indexTable, data)
        }
      }
    }
  }

  /**
   * Upsert values.
   */
  def put(row: Array[Byte], families: ColumnFamily*): Unit = {
    baseTable.put(row, families: _*)
    val data = Row(row, families)
    families.foreach { family =>
      family.columns.foreach { column =>
        val indexed = indexTables.get((family.family, column.qualifier))
        if (indexed.isDefined) {
          indexed.get.foreach { case (index, indexTable) =>
            insertIndex(index, indexTable, data)
          }
        }
      }
    }
  }

  /**
   * Update the values of one or more rows.
   */
  def put(rows: Row*): Unit = {
    baseTable.put(rows: _*)
    rows.foreach { case Row(row, families) =>
      val data = Row(row, families)
      families.foreach { family =>
        family.columns.foreach { column =>
          val indexed = indexTables.get((family.family, column.qualifier))
          if (indexed.isDefined) {
            indexed.get.foreach { case (index, indexTable) =>
              insertIndex(index, indexTable, data)
            }
          }
        }
      }
    }
  }

  /**
   * Delete a value.
   */
  def delete(row: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  def delete(row: Array[Byte], families: Seq[Array[Byte]]): Unit

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  def delete(row: Array[Byte], family: Array[Byte], columns: Seq[Array[Byte]]): Unit

  /**
   * Delete multiple rows.
   */
  def delete(rows: Seq[Array[Byte]], families: Seq[Array[Byte]]): Unit

  /**
   * Delete multiple rows.
   */
  def delete(rows: Seq[Array[Byte]], family: Array[Byte], columns: Seq[Array[Byte]]): Unit

  /**
   * Gets the meta of all indices of a base table.
   * Each row of metaTable encodes the index information for a table
   * The row key is the base table name. Each column is a BSON object
   * about the index. The column name is the index name.
   */
  def getIndexMeta: ArrayBuffer[(Index, IndexTable)] = {
    if (!db.tableExists(metaTableName)) ArrayBuffer[(Index, IndexTable)]()

    // Index meta data table
    val metaTable = db(metaTableName)

    // Meta data encoded in BSON format.
    val bson = new BsonSerializer

    metaTable.get(baseTable.name, metaTableColumnFamily).map { column =>
      val index = Index(bson.deserialize(collection.immutable.Map("$" -> column.value)))
      val indexTable = db(index.indexTableName)
      (index, indexTable)
    }.to[ArrayBuffer]
  }

  def addIndex(index: Index): Unit = {
    if (!db.tableExists(metaTableName))
      db.createTable(metaTableName, metaTableColumnFamily)

    val metaTable = db(metaTableName)
    val bson = new BsonSerializer
    val json = bson.serialize(index.toJson)
    put(baseTable.name.getBytes(utf8), metaTableColumnFamilyBytes, index.name.getBytes(utf8), json("$"))
  }
}