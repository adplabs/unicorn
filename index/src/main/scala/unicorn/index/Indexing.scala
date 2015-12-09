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
import unicorn.json._
import unicorn.util._

/**
 * Index manager. A BigTable with Indexing trait will automatically maintain
 * secondary index.
 *
 * Use the stackable trait pattern by "abstract override".
 * See details at http://www.artima.com/scalazine/articles/stackable_trait_pattern.html
 *
 * @author Haifeng Li
 */
trait Indexing extends BigTable with RowScan with Counter {
  val db: Database[IndexableTable]

  var builders = getIndexBuilders

  /** Close the base table and the index table */
  abstract override def close(): Unit = {
    super.close
    builders.foreach(_.close)
  }

  /**
   * Gets the meta of all indices of a base table.
   * Each row of metaTable encodes the index information for a table
   * The row key is the base table name. Each column is a BSON object
   * about the index. The column name is the index name.
   */
  private def getIndexBuilders: Seq[IndexBuilder] = {
    if (!db.tableExists(IndexMetaTableName)) return Seq[IndexBuilder]()

    // Index meta data table
    val metaTable = db(IndexMetaTableName)

    // Meta data encoded in BSON format.
    val bson = new BsonSerializer

    metaTable.get(name, IndexMetaTableColumnFamily).map { column =>
      val index = Index(bson.deserialize(collection.immutable.Map("$" -> column.value.bytes)))
      val indexTable = db(index.indexTableName)
      IndexBuilder(index, indexTable)
    }
  }

  /** Add an index to meta data.    */
  private def addIndex(index: Index): Unit = {
    // If the index meta data table doesn't exist, create it.
    if (!db.tableExists(IndexMetaTableName))
      db.createTable(IndexMetaTableName, IndexMetaTableColumnFamily)

    val metaTable = db(IndexMetaTableName)
    val bson = new BsonSerializer
    val json = bson.serialize(index.toJson)
    metaTable.put(name, IndexMetaTableColumnFamily, index.name, json("$"))
  }

  /** Create an index. */
  def createIndex(index: Index): Unit = {
    builders.foreach { builder =>
      if (builder.index.name == index.name) throw new IllegalArgumentException(s"Index ${index.name} exists")
      if (builder.index.coverSameColumns(index)) throw new IllegalArgumentException(s"Index ${index.name} covers the same columns")
    }

    val indexTable = db.createTable(index.indexTableName, IndexColumnFamilies: _*)
    val builder = IndexBuilder(index, indexTable)
    scan(startRowKey, endRowKey, index.family, index.columns.map(_.qualifier): _*).foreach { case Row(row, families) =>
      builder.insertIndex(row, RowMap(families: _*))
    }

    addIndex(index)
    builders = getIndexBuilders
  }

  /** Drop an index. */
  def dropIndex(indexName: String): Unit = {
    val indexBuilder = builders.find(_.index.name == indexName)

    if (indexBuilder.isDefined) {
      val metaTable = db(IndexMetaTableName)
      metaTable.delete(name, IndexMetaTableColumnFamily, indexName)
      db.dropTable(indexBuilder.get.index.indexTableName)
      builders = getIndexBuilders
    }
  }

  /**
   * Upsert a value.
   */
  abstract override def update(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit = {
    put(row, family, column, value)
  }

  /**
   * Upsert a value.
   */
  abstract override def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long = 0L): Unit = {
    val coveredColumns = builders.flatMap { builder =>
      if (builder.index.family == family) {
        builder.index.findCoveredColumns(family, column)
      } else Seq.empty
    }.distinct

    if (coveredColumns.isEmpty) {
      super.put(row, family, column, value)
    } else {
      val values = get(row, family, coveredColumns: _*)
      val oldValue = RowMap(family, values: _*)
      val newValue = RowMap(family, values: _*)

      super.put(row, family, column, value)

      // TODO to use the same timestamp as the base cell, we need to read it back.
      // HBase supports key only read by filter.
      newValue(family)(column) = Column(column, value)

      builders.foreach { builder =>
        if (builder.index.cover(family, column)) {
          if (!values.isEmpty) builder.deleteIndex(row, oldValue)
          builder.insertIndex(row, newValue)
        }
      }
    }
  }

  /**
   * Upsert values.
   */
  abstract override def put(row: ByteArray, family: String, columns: Column*): Unit = {
    val qualifiers = columns.map(_.qualifier)

    val coveredColumns = builders.flatMap { builder =>
      if (builder.index.family == family) {
        builder.index.findCoveredColumns(family, qualifiers: _*)
      } else Seq.empty
    }.distinct

    if (coveredColumns.isEmpty) {
      super.put(row, family, columns: _*)
    } else {
      val values = get(row, family, coveredColumns: _*)
      val oldValue = RowMap(family, values: _*)
      val newValue = RowMap(family, values: _*)

      super.put(row, family, columns: _*)

      // TODO to use the same timestamp as the base cell, we need to read it back.
      // HBase supports key only read by filter.
      columns.foreach { column =>
        newValue(family)(column.qualifier) = column
      }

      builders.foreach { builder =>
        if (builder.index.cover(family, qualifiers: _*)) {
          builder.deleteIndex(row, oldValue)
          builder.insertIndex(row, newValue)
        }
      }
    }
  }

  /**
   * Upsert values.
   */
  /*
  abstract override def put(row: ByteArray, families: ColumnFamily*): Unit = {
    val qualifiers = columns.map(_.qualifier)

    val coveredColumns = builders.flatMap { builder =>
      if (builder.index.family == family) {
        builder.index.findCoveredColumns(family, qualifiers: _*)
      } else Seq.empty
    }.distinct

    if (coveredColumns.isEmpty) {
      super.put(row, families: _*)
    } else {
      val values = get(row, family, coveredColumns: _*)
      val oldValue = RowMap(family, values: _*)
      val newValue = RowMap(family, values: _*)

      super.put(row, families: _*)

      // TODO to use the same timestamp as the base cell, we need to read it back.
      // HBase supports key only read by filter.
      columns.foreach { column =>
        newValue(family)(column.qualifier) = column
      }

      builders.foreach { builder =>
        if (builder.index.cover(family, qualifiers: _*)) {
          builder.deleteIndex(row, oldValue)
          builder.insertIndex(row, newValue)
        }
      }
    }
  }
  */


  /**
   * Update the values of one or more rows.
   * The implementation may or may not optimize the batch operations.
   */
  //def putBatch(rows: Row*): Unit

  /**
   * Delete the columns of a row. If columns is empty, delete all columns in the family.
   */
  abstract override def delete(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    val coveredColumns = builders.flatMap { builder =>
      if (builder.index.family == family) {
        builder.index.findCoveredColumns(family, columns: _*)
      } else Seq.empty
    }.distinct

    if (coveredColumns.isEmpty) {
      super.delete(row, family, columns: _*)
    } else {
      val values = get(row, family, coveredColumns: _*)
      val rowMap = RowMap(family, values: _*)

      super.delete(row, family, columns: _*)

      builders.foreach { builder =>
        if (builder.index.cover(family, columns: _*)) {
          builder.deleteIndex(row, rowMap)
        }
      }
    }
  }

  /**
   * Delete the columns of a row. If families is empty, delete the whole row.
   */
  abstract override def delete(row: ByteArray, families: Seq[String]): Unit = {
    val coveredColumns = families.map { family =>
      val columns = builders.flatMap { builder =>
        if (builder.index.family == family) {
          builder.index.coveredColumns
        } else Seq.empty
      }.distinct
      (family, columns)
    }.filter(!_._2.isEmpty)

    if (coveredColumns.isEmpty) {
      super.delete(row, families)
    } else {
      val values = get(row, families)
      val rowMap = RowMap(family, values: _*)

      super.delete(row, family, columns: _*)

      builders.foreach { builder =>
        if (builder.index.cover(family, columns: _*)) {
          builder.deleteIndex(row, rowMap)
        }
      }
    }
  }

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
