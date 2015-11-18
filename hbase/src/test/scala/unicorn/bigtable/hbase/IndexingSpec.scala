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
    if (table != null) table.close
    hbase.dropTable(tableName)
  }

  "HBase" should {
    "get the put" in {
      table.put("row1", "cf1", "c1", "v1")
      new String(table("row1", "cf1", "c1").get, utf8) === "v1"
      table.delete("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
    }
  }
}
