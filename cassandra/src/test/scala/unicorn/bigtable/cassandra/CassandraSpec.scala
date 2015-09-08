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

package unicorn.bigtable.cassandra

import java.util.Properties

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable._
import unicorn.util.utf8

/**
 * @author Haifeng Li
 */
class CassandraSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val cassandra = Cassandra("127.0.0.1", 9160)
  val tableName = "unicorn_test"
  var table: CassandraTable = null

  override def beforeAll = {
    val props = new Properties
    props.put("class", "org.apache.cassandra.locator.SimpleStrategy")
    props.put("replication_factor", "1")
    cassandra.createTable(tableName, props, "cf1", "cf2")
    table = cassandra(tableName)
  }

  override def afterAll = {
    if (table != null) table.close
    cassandra.dropTable(tableName)
  }

  "Cassandra" should {
    "get the put" in {
      table.put("row1", "cf1", "c1", "v1")
      new String(table.get("row1", "cf1", "c1").get, utf8) === "v1"
      table.delete("row1", "cf1", "c1")
      table.get("row1", "cf1", "c1") === None
    }

    "get the family" in {
      table.put("row1".getBytes(utf8), "cf1".getBytes(utf8), Column("c1".getBytes(utf8), "v1".getBytes(utf8)), Column("c2".getBytes(utf8), "v2".getBytes(utf8)))
      val columns = table.get("row1", "cf1")
      columns.size === 2
      new String(columns(0).value, utf8) === "v1"
      new String(columns(1).value, utf8) === "v2"

      table.delete("row1", "cf1")
      val empty = table.get("row1", "cf1")
      empty.size === 0
    }

    "get empty family" in {
      val columns = table.get("row1", "cf1")
      columns.size === 0
    }

    "get nonexistent family" in {
      table.get("row1", "cf5") must throwA[Exception]
    }

    "get the row" in {
      table.put("row1".getBytes(utf8),
        ColumnFamily("cf1".getBytes(utf8), Seq(Column("c1".getBytes(utf8), "v1".getBytes(utf8)), Column("c2".getBytes(utf8), "v2".getBytes(utf8)))),
        ColumnFamily("cf2".getBytes(utf8), Seq(Column("c3".getBytes(utf8), "v3".getBytes(utf8))))
      )
      val families = table.get("row1")
      families.size === 2
      families(0).columns.size === 1
      families(1).columns.size === 2
      new String(families(0).family, utf8) === "cf2"
      new String(families(1).family, utf8) === "cf1"

      new String(families(0).columns(0).value, utf8) === "v3"
      new String(families(1).columns(0).value, utf8) === "v1"
      new String(families(1).columns(1).value, utf8) === "v2"

      table.delete("row1", "cf1")
      val cf1 = table.get("row1", "cf1")
      cf1.size === 0

      table.get("row1").size === 1
      val cf2 = table.get("row1", "cf2")
      cf2.size === 1

      table.delete("row1")
      table.get("row1").size === 0
    }

    "get nonexistent row" in {
      val families = table.get("row5")
      families.size === 0
    }

    "get multiple rows" in {
      val row1 = Row("row1".getBytes(utf8),
        Seq(ColumnFamily("cf1".getBytes(utf8), Seq(Column("c1".getBytes(utf8), "v1".getBytes(utf8)), Column("c2".getBytes(utf8), "v2".getBytes(utf8)))),
          ColumnFamily("cf2".getBytes(utf8), Seq(Column("c3".getBytes(utf8), "v3".getBytes(utf8))))))

      val row2 = Row("row2".getBytes(utf8),
        Seq(ColumnFamily("cf1".getBytes(utf8), Seq(Column("c1".getBytes(utf8), "v1".getBytes(utf8)), Column("c2".getBytes(utf8), "v2".getBytes(utf8))))))

      table.put(row1, row2)

      val keys = Seq("row1", "row2").map(_.getBytes(utf8))
      val rows = table.get(keys)
      rows.size === 2
      rows(0).families.size === 2
      rows(1).families.size === 1

      table.delete(keys)
      table.get(keys).size === 0
    }
  }
}
