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

/**
 * Index manager. A BigTable with Indexing trait will automatically maintain
 * secondary index.
 *
 * @author Haifeng Li
 */
trait Indexing {
  /** Base table */
  val baseTable: IndexTable
  val db: Database[IndexTable]

  var builders = getIndexBuilders

  /** Close the base table and the index table */
  def close: Unit = {
    baseTable.close
    builders.foreach(_.close)
  }

  /**
   * Gets the meta of all indices of a base table.
   * Each row of metaTable encodes the index information for a table
   * The row key is the base table name. Each column is a BSON object
   * about the index. The column name is the index name.
   */
  def getIndexBuilders: Seq[IndexBuilder] = {
    if (!db.tableExists(IndexMetaTableName)) ArrayBuffer[(Index, IndexTable)]()

    // Index meta data table
    val metaTable = db(IndexMetaTableName)

    // Meta data encoded in BSON format.
    val bson = new BsonSerializer

    metaTable.get(baseTable.name, IndexMetaTableColumnFamily).map { column =>
      val index = Index(bson.deserialize(collection.immutable.Map("$" -> column.value.bytes)))
      val indexTable = db(index.indexTableName)
      IndexBuilder(index, indexTable)
    }
  }

  def addIndex(index: Index): Unit = {
    if (!db.tableExists(IndexMetaTableName))
      db.createTable(IndexMetaTableName, IndexMetaTableColumnFamily)

    val metaTable = db(IndexMetaTableName)
    val bson = new BsonSerializer
    val json = bson.serialize(index.toJson)
    metaTable.put(baseTable.name.getBytes(utf8), IndexMetaTableColumnFamily, index.name, json("$"))
  }

  def createIndex(name: String, index: Index): Unit = {
    builders.foreach { builder =>
      if (builder.index.indexTableName == name) throw new IllegalArgumentException(s"Index $name exists")
      if (builder.index.coverSameColumns(index)) throw new IllegalArgumentException(s"Index ${index.name} covers the same columns")
    }

    val indexTable = db.createTable(name, IndexColumnFamilies: _*)
    val builder = IndexBuilder(index, indexTable)
    baseTable.scan(baseTable.startRowKey, baseTable.endRowKey, index.family, index.columns.map(_.qualifier): _*).foreach { case Row(row, families) =>
      builder.insertIndex(row, RowMap(families: _*))
    }

    addIndex(index)
    builders = getIndexBuilders
  }

  def dropIndex(name: String): Unit = {
    val index = builders.find(_.index.indexTableName == name)

    if (index.isDefined) {
      val metaTable = db(IndexMetaTableName)
      metaTable.delete(baseTable.name.getBytes(utf8), IndexMetaTableColumnFamily, name)
      db.dropTable(name)
      builders = getIndexBuilders
    }
  }

  /**
   * Upsert a value.
   */
  def update(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit = {
    val coveredColumns = builders.flatMap { builder =>
      if (builder.index.family == family) {
        builder.index.findCoveredColumns(family, column)
      } else Seq.empty
    }.distinct

    if (coveredColumns.isEmpty) {
      baseTable.update(row, family, column, value)
    } else {
      val values = baseTable.get(row, family, coveredColumns: _*)
      val oldValue = RowMap(family, values: _*)
      val newValue = RowMap(family, values: _*)

      baseTable.update(row, family, column, value)

      // TODO to use the same timestamp as the base cell, we need to read it back.
      // HBase support key only read by filter.
      newValue(family)(column) = Column(column, value)

      builders.foreach { builder =>
        if (builder.index.cover(family, value)) {
          builder.deleteIndex(row, oldValue)
          builder.insertIndex(row, newValue)
        }
      }
    }
  }

  /**
   * Upsert a value.
   */
  //def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long = 0L): Unit

  /**
   * Upsert values.
   */
  //def put(row: ByteArray, family: String, columns: Column*): Unit

  /**
   * Upsert values.
   */
  //def put(row: ByteArray, families: ColumnFamily*): Unit

  /**
   * Update the values of one or more rows.
   * The implementation may or may not optimize the batch operations.
   */
  //def putBatch(rows: Row*): Unit

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  //def delete(row: ByteArray, family: String, columns: ByteArray*): Unit

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  //def delete(row: ByteArray, families: Seq[String] = Seq()): Unit

  /**
   * Delete multiple rows.
   * The implementation may or may not optimize the batch operations.
   */
  //def deleteBatch(rows: Seq[ByteArray], family: String, columns: ByteArray*): Unit

  /**
   * Delete multiple rows.
   * The implementation may or may not optimize the batch operations.
   */
  //def deleteBatch(rows: Seq[ByteArray], families: Seq[String] = Seq()): Unit
}