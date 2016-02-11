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

package unicorn.bigtable.hbase

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable._
import unicorn.util._
import unicorn.index._

class IndexingSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val hbase = HBase()
  val tableName = "unicorn_test"
  var table: HBaseTable with Indexing = null

  override def beforeAll = {
    hbase.createTable(tableName, "cf1", "cf2")
    table = hbase.getTableWithIndex(tableName)
  }

  override def afterAll = {
    if (table != null) {
      table.close
    }
    hbase.dropTable(tableName)
    hbase.dropTable("unicorn_index_unicorn_test")
  }

  "HBase" should {
    "get the put" in {
      table.put("row1", "cf1", "c1", "v1", 0L)
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      table.delete("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
    }
    "single column index" in {
      val indexName = "test-index-cf1-single"
      table.createIndex(indexName, "cf1", Seq(IndexColumn("c1")))
      table.put("row1", "cf1", "c1", "v1", 0L)
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"

      val indexTable = table.indexBuilder.indexTable
      val index = table.indexBuilder.indices(0)
      var cell = index.codec("row1", "cf1", "c1", "v1")(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true

      table.put("row1", "cf1", "c1", "v2", 0L)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === false
      cell = index.codec("row1", "cf1", "c1", "v2")(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true

      table.delete("row1", "cf1", "c1")
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === false

      table.dropIndex(indexName)

      table("row1", "cf1", "c1") === None
    }
    "composite index" in {
      val indexName = "test-index-cf1-composite"
      val indexName2 = "test-index-cf2-composite"
      table.createIndex(indexName, "cf1", Seq(IndexColumn("c1"), IndexColumn("c2")))
      table.createIndex(indexName2, "cf2", Seq(IndexColumn("c3")))

      table.put("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      new String(table("row1", "cf1", "c2").get, utf8) === "v2"

      val indexTable = table.indexBuilder.indexTable
      val index = table.indexBuilder.indices.filter(_.name == indexName)(0)
      var cell = index.codec("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true

      table.put("row1", "cf1", "c1", "v3", 0L)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === false
      cell = index.codec("row1", "cf1", Column("c1", "v3"), Column("c2", "v2"))(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true
      
      table.delete("row1", Seq(("cf1", Seq.empty), ("cf2", Seq.empty))) // delete multiple column families
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === false

      table.put("row1", Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2"))), ColumnFamily("cf2", Seq(Column("c3", "v3")))))
      val index2 = table.indexBuilder.indices.filter(_.name == indexName2)(0)
      cell = index.codec("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true
      cell = index2.codec("row1", "cf2", "c3", "v3")(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true

      table.delete("row1", Seq(("cf1", Seq.empty), ("cf2", Seq.empty))) // delete multiple column families
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === false
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === false

      table.dropIndex(indexName)
      table.dropIndex(indexName2)

      table("row1", "cf1", "c1") === None
    }
    "text index" in {
      val indexName = "test-index-cf1-text"
      table.createIndex(indexName, "cf1", Seq(IndexColumn("c1"), IndexColumn("c2")), IndexType.Text)
      table.put("row1", "cf1", Column("c1", "adp payroll"), Column("c2", "adp unicorn rocks"))
      new String(table("row1", "cf1", "c1").get, utf8) === "adp payroll"

      val indexTable = table.indexBuilder.indexTable
      val index = table.indexBuilder.indices.filter(_.name == indexName)(0)
      var cells = index.codec("row1", "cf1", Column("c1", "adp payroll"), Column("c2", "adp unicorn rocks"))
      cells.foreach { cell =>
        indexTable(cell.row, cell.family, cell.qualifier).isDefined === true
      }

      table.delete("row1", "cf1", "c1")
      cells = index.codec("row1", "cf1", Column("c1", "adp payroll"))
      cells.foreach { cell =>
        indexTable(cell.row, cell.family, cell.qualifier).isDefined === false
      }

      cells = index.codec("row1", "cf1", Column("c2", "adp unicorn rocks"))
      cells.foreach { cell =>
        indexTable(cell.row, cell.family, cell.qualifier).isDefined === true
      }

      table.dropIndex(indexName)

      cells.foreach { cell =>
        indexTable(cell.row, cell.family, cell.qualifier).isDefined === false
      }
      table("row1", "cf1", "c1") === None
    }
    "hashed index" in {
      val indexName = "test-index-cf1-hash"
      table.createIndex(indexName, "cf1", Seq(IndexColumn("c1"), IndexColumn("c2")), IndexType.Hashed)
      table.put("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      new String(table("row1", "cf1", "c2").get, utf8) === "v2"

      val indexTable = table.indexBuilder.indexTable
      val index = table.indexBuilder.indices.filter(_.name == indexName)(0)
      var cell = index.codec("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true

      table.put("row1", "cf1", "c1", "v3", 0L)
      cell = index.codec("row1", "cf1", Column("c1", "v3"), Column("c2", "v2"))(0)
      indexTable(cell.row, cell.family, cell.qualifier).isDefined === true

      table.delete("row1", "cf1") // delete all columns in the family
      indexTable(cell.row, cell.family, cell.qualifier).isDefined  === false

      table.dropIndex(indexName)

      table("row1", "cf1", "c1") === None
    }
  }
}
