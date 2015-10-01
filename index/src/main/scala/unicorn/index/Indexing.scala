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
  val indexTables = collection.mutable.Map[(ByteArray, ByteArray), ArrayBuffer[(BigTable, Index)]]().withDefaultValue(ArrayBuffer())
  indexMeta.foreach { case (indexTable, meta) =>
    meta.columns.foreach { column =>
      indexTables((meta.family, column.qualifier)).append((indexTable, meta))
    }
  }

  /** Close the base table and the index table */
  def close: Unit = {
    baseTable.close
    indexMeta.foreach { case (indexTable, _) => indexTable.close }
  }

  def createIndex(name: String, index: Index): Unit = {
    indexMeta.foreach { case (_, meta) =>
        if (meta.indexTable == name) throw new IllegalArgumentException(s"Index $name exists")
    }
    val indexTable = db.createTable(name, Indexing.indexTableIndexColumnFamily, Indexing.indexTableStatColumnFamily)
    indexMeta.append((indexTable, index))
    Indexing.addIndex(baseTable, index)

    index.columns.foreach { column =>
      indexTables((index.family, column.qualifier)).append((indexTable, index))
    }

    baseTable.scan(baseTable.startRowKey, baseTable.endRowKey, index.family, index.columns.map(_.qualifier)).foreach { row =>
      indexTable.put(indexRowkey(row.row, row.families.head.columns), Indexing.indexTableIndexColumnFamily, row.row, Indexing.indexValue)
    }
  }

  def dropIndex(name: String): Unit = {
    db.dropTable(name)
    var i = -1
    var index: Index = null
    indexMeta.zipWithIndex.foreach{ case ((indexTable, meta), idx) =>
        if (meta.indexTable == name) {
          indexTable.close
          i = idx
          index = meta
        }
    }
    indexMeta.remove(i)

    index.columns.foreach { column =>
      val a = indexTables((index.family, column.qualifier))
      val i = a.find(_._1.name == name)
      if (i.isDefined) a.remove()
    }
  }

  def indexRowkey(row: Array[Byte], columns: Seq[Column]): Array[Byte] = {
    row
  }
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
  def getIndexMeta(table: BigTable): ArrayBuffer[(BigTable, Index)] = {
    if (table.db.tableExists(metaTableName))
      table.db.createTable(metaTableName, metaTableColumnFamily)

    /** Index meta data table */
    val metaTable = table.db(metaTableName)

    /**
     * Meta data encoded in BSON format.
     */
    val bson = new BsonSerializer

    metaTable.get(table.name, metaTableColumnFamily).map { column =>
      val index = Index(bson.deserialize(collection.immutable.Map("$" -> column.value)))
      (table.db(index.indexTable), index)
    }.to[ArrayBuffer]
  }

  def addIndex(baseTable: BigTable, index: Index): Unit = {
    val metaTable = baseTable.db(metaTableName)
    val bson = new BsonSerializer
    val json = bson.serialize(Index.toJson(index))
    metaTable.put(baseTable.name.getBytes(utf8), metaTableColumnFamily.getBytes(utf8), index.indexTable.getBytes(utf8), json("$"))
  }
}