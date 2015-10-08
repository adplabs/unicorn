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
  /** Base table */
  val baseTable: BigTable with RowScan
  val db = baseTable.db

  val indexMeta = Indexing.getIndexMeta(baseTable)

  /** Index tables. A column may appears in multiple indices (composite index) */
  val indexTables = collection.mutable.Map[(ByteArray, ByteArray), ArrayBuffer[(Index, BigTable)]]().withDefaultValue(ArrayBuffer())
  indexMeta.foreach { case (index, indexTable) =>
    index.columns.foreach { column =>
      indexTables((index.family, column.qualifier)).append((index, indexTable))
    }
  }

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
    Indexing.addIndex(baseTable, index)

    index.columns.foreach { column =>
      indexTables((index.family, column.qualifier)).append((index, indexTable))
    }
  }

  def dropIndex(name: String): Unit = {
    val i = indexMeta.zipWithIndex.find(_._1._1.indexTableName == name)

    if (i.isDefined) {
      val index = indexMeta.remove(i.get._2)._1

      index.columns.foreach { column =>
        val a = indexTables((index.family, column.qualifier))
        val i = a.zipWithIndex.find(_._1._1.name == name)
        if (i.isDefined) a.remove(i.get._2)
      }

      db.dropTable(name)
    }
  }

  def insertIndex(index: Index, indexTable: BigTable, row: Row): Unit = {
    val map = row.families.map { family =>
      val columns = family.columns.map { column => (ByteArray(column.qualifier), column) }.toMap
      (ByteArray(family.family), columns)
    }.toMap

    val cells = index.codec(row.row, map)
    cells.foreach { cell =>
      indexTable.put(cell.row, cell.family, cell.qualifier, cell.value, cell.timestamp)
    }
  }

  def deleteIndex(index: Index, indexTable: BigTable, row: Row): Unit = {
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
    val indexed = indexTables.get((family, column))
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
}

object Indexing {
  val metaTableName = "unicorn.meta.index"
  val metaTableColumnFamily = "meta"
  val metaTableColumnFamilyBytes = metaTableColumnFamily.getBytes(utf8)
  val indexTableIndexColumnFamily = "index"
  val indexTableStatColumnFamily = "stat"
  val indexValue = Array[Byte](1)

  /**
   * Gets the meta of all indices of a base table.
   * Each row of metaTable encodes the index information for a table
   * The row key is the base table name. Each column is a BSON object
   * about the index. The column name is the index name.
   */
  def getIndexMeta(baseTable: BigTable): ArrayBuffer[(Index, BigTable)] = {
    if (!baseTable.db.tableExists(metaTableName)) ArrayBuffer[(Index, BigTable)]()

    // Index meta data table
    val metaTable = baseTable.db(metaTableName)

    // Meta data encoded in BSON format.
    val bson = new BsonSerializer

    metaTable.get(baseTable.name, metaTableColumnFamily).map { column =>
      val index = Index(bson.deserialize(collection.immutable.Map("$" -> column.value)))
      val indexTable = baseTable.db(index.indexTableName)
      (index, indexTable)
    }.to[ArrayBuffer]
  }

  def addIndex(baseTable: BigTable, index: Index): Unit = {
    if (!baseTable.db.tableExists(metaTableName))
      baseTable.db.createTable(metaTableName, metaTableColumnFamily)

    val metaTable = baseTable.db(metaTableName)
    val bson = new BsonSerializer
    val json = bson.serialize(index.toJson)
    metaTable.put(baseTable.name.getBytes(utf8), metaTableColumnFamily.getBytes(utf8), index.name.getBytes(utf8), json("$"))
  }
}