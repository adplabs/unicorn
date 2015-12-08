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
  val indexName = "test-index-c1"

  override def beforeAll = {
    hbase.createTable(tableName, "cf1", "cf2")
    table = hbase.getTableWithIndex(tableName)
  }

  override def afterAll = {
    if (table != null) {
      table.dropIndex(indexName)
      table.close
    }
    hbase.dropTable(tableName)
    // delete the index meta table
    hbase.dropTable("unicorn.meta.index")
  }

  "HBase" should {
    "get the put" in {
      table.put("row1", "cf1", "c1", "v1")
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      table.delete("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
    }
    "single column index" in {
      val index = Index(indexName, "cf1", Seq(IndexColumn("c1")))
      table.createIndex(index)
      table.put("row1", "cf1", "c1", "v1")
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"

      val indexTable = hbase("unicorn.index.test-index-c1")
      indexTable("v1", "index", "row1").isDefined === true

      table.put("row1", "cf1", "c1", "v2")
      indexTable("v1", "index", "row1").isDefined === false
      indexTable("v2", "index", "row1").isDefined === true

      table.delete("row1", "cf1", "c1")
      //indexTable("v2", "index", "row1").isDefined === false

      table.dropIndex(index.name)

      table("row1", "cf1", "c1") === None
    }
    "composite index" in {
      val index = Index(indexName, "cf1", Seq(IndexColumn("c1"), IndexColumn("c2")))
      table.createIndex(index)
      table.put("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      new String(table("row1", "cf1", "c2").get, utf8) === "v2"

      val indexTable = hbase("unicorn.index.test-index-c1")
      indexTable("v1v2", "index", "row1").isDefined === true
      indexTable("v1", "index", "row1").isDefined === false
      indexTable("v2", "index", "row1").isDefined === false

      table.put("row1", "cf1", "c1", "v3")
      indexTable("v3", "index", "row1").isDefined === false
      indexTable("v3v2", "index", "row1").isDefined === true

      table.delete("row1", "cf1", "c1")
      //indexTable("v2", "index", "row1").isDefined === false

      table.dropIndex(index.name)

      table("row1", "cf1", "c1") === None
    }
    "text index" in {
      val index = Index(indexName, "cf1", Seq(IndexColumn("c1"), IndexColumn("c2")), IndexType.Text)
      table.createIndex(index)
      table.put("row1", "cf1", Column("c1", "adp payroll"), Column("c2", "adp unicorn rocks"))
      new String(table("row1", "cf1", "c1").get, utf8) === "adp payroll"

      val indexTable = hbase("unicorn.index.test-index-c1")
      indexTable.get("adp", "index").size === 2
      indexTable.get("payrol", "index").size === 1
      indexTable.get("unicorn", "index").size === 1
      indexTable.get("rock", "index").size === 1

      table.delete("row1", "cf1", "c1")
      //indexTable("v2", "index", "row1").isDefined === false

      table.dropIndex(index.name)

      table("row1", "cf1", "c1") === None
    }
    "hashed index" in {
      val index = Index(indexName, "cf1", Seq(IndexColumn("c1"), IndexColumn("c2")), IndexType.Hashed)
      table.createIndex(index)
      table.put("row1", "cf1", Column("c1", "v1"), Column("c2", "v2"))
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      new String(table("row1", "cf1", "c2").get, utf8) === "v2"

      val indexTable = hbase("unicorn.index.test-index-c1")
      indexTable(md5("v1v2"), "index", "row1").isDefined === true
      indexTable("v1v2", "index", "row1").isDefined === false

      table.put("row1", "cf1", "c1", "v3")
      indexTable("v3v2", "index", "row1").isDefined === false
      indexTable(md5("v3v2"), "index", "row1").isDefined === true

      table.delete("row1", "cf1", "c1")
      //indexTable("v2", "index", "row1").isDefined === false

      table.dropIndex(index.name)

      table("row1", "cf1", "c1") === None
    }
  }
}
