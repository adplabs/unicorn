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

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import IndexType.IndexType
import unicorn.bigtable.{ColumnFamily, Row, RowScanner}
import unicorn.json.BsonSerializer
import unicorn.util._

/** Index builder. Because of the sparse natural of BigTable, all indices are sparse.
  * That is, we omit references to rows that do not include the indexed field.
  * The work around for users to set JsUndefined to the missing field.
  *
  * @author Haifeng Li
  */
case class IndexBuilder(indexTable: IndexableTable, indices: ArrayBuffer[Index]) {
  def close: Unit = indexTable.close

  def createIndex(name: String, family: String, columns: Seq[IndexColumn], indexType: IndexType = IndexType.Default): Index = {
    require(columns.size > 0, "Index columns cannot be empty")

    indices.foreach { index =>
      require(index.name != name, s"Index $name exists")
      require(family != index.family || columns != index.columns, s"Index ${index.name} covers the same columns")
    }

    val id = indexTable.getCounter(IndexTableMetaRow, IndexMetaColumnFamily, IndexTableNewIndexId).toShort
    indexTable.addCounter(IndexTableMetaRow, IndexMetaColumnFamily, IndexTableNewIndexId, 1L)
    val index = Index(id, name, family, columns, indexType)
    indices += index

    val bson = new BsonSerializer
    indexTable.update(IndexTableMetaRow, IndexMetaColumnFamily, name, bson.toBytes(index.toJson))

    index
  }

  def buildIndex(index: Index, scanner: RowScanner): Unit = {
    val seq = Seq(index)
    scanner.foreach { case Row(row, families) =>
     // put(index, row, ColumnMap(families))
    }
  }

  def dropIndex(indexName: String): Unit = {
    val index = indices.find(indexName == _.name)
    if (index.isDefined) {
      indices -= index.get
      indexTable.delete(IndexTableMetaRow, IndexMetaColumnFamily, indexName)

      val prefix = ByteBuffer.allocate(2)
      prefix.putShort(index.get.id.toShort)
      indexTable.scanPrefix(prefix.array, IndexMetaColumnFamily, IndexTableStatColumnCount) foreach { row =>
        indexTable.delete(row.row)
      }
    }
  }

  def indexedColumns(family: String, columns: ByteArray*): (Seq[Index], Seq[ByteArray]) = {
    val (activedIndices, qualifiers) = indices.filter(_.family == family).map { index =>
      (index, index.indexedColumns(columns: _*))
    }.filter(!_._2.isEmpty).unzip

    (activedIndices, qualifiers.flatten.distinct)
  }

  def indexedColumns(families: Seq[(String, Seq[ByteArray])]): (Seq[Index], Seq[(String, Seq[ByteArray])]) = {
    val (activedIndices, qualifiers) = families.map { case (family, columns) =>
      val (activedIndices, qualifiers) = indices.filter(_.family == family).map { index =>
        (index, index.indexedColumns(columns: _*))
      }.filter(!_._2.isEmpty).unzip

      (activedIndices, (family, qualifiers.flatten.distinct))
    }.filter(!_._1.isEmpty).unzip
    (activedIndices.flatten, qualifiers)
  }

  def put(tenant: Option[Array[Byte]], indices: Seq[Index], row: ByteArray, columns: ColumnMap): Unit = {
    indices.foreach { index =>
      val cells = index.codec(tenant, row, columns)
      cells.foreach { cell =>
        indexTable.put(cell.row, cell.family, cell.qualifier, cell.value, cell.timestamp)
        indexTable.addCounter(cell.row, IndexMetaColumnFamily, IndexTableStatColumnCount, 1)
      }
    }
  }

  def delete(tenant: Option[Array[Byte]], indices: Seq[Index], row: ByteArray, columns: ColumnMap): Unit = {
    indices.foreach { index =>
      val cells = index.codec(tenant, row, columns)
      cells.foreach { cell =>
        indexTable.delete(cell.row, cell.family, cell.qualifier)
        indexTable.addCounter(cell.row, IndexMetaColumnFamily, IndexTableStatColumnCount, -1)
      }
    }
  }
}

object IndexBuilder {
  /** Gets the meta of all indices of a base table.
    * Each cell of meta row encodes an index in BSON.
    * The column name is the index name.
    */
  def apply(indexTable: IndexableTable): IndexBuilder = {
    val bson = new BsonSerializer

    val indices = indexTable.get(IndexTableMetaRow, IndexMetaColumnFamily).filter(_.qualifier != IndexTableNewIndexId).map { column =>
      println(column)
      println(bson.toJson(column.value.bytes))
      Index(bson.toJson(column.value.bytes))
    }

    IndexBuilder(indexTable, ArrayBuffer(indices: _*))
  }
}