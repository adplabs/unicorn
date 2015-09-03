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
import unicorn.bigtable._, BigTable.charset

/**
 * @author Haifeng Li
 */
class HBaseSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val hbase = HBase()
  val tableName = "unicorn_test"
  var table: HBaseTable = null

  override def beforeAll = {
    hbase.createTable(tableName, "cf1", "cf2")
    table = hbase(tableName)
  }

  override def afterAll = {
    if (table != null) table.close
    hbase.dropTable(tableName)
  }

  "HBase" should {
    "get the put" in {
      table.put("row1", "cf1", "c1", "v1")
      new String(table.get("row1", "cf1", "c1").get, charset) === "v1"
      table.delete("row1", "cf1", "c1")
      table.get("row1", "cf1", "c1") === None
    }

    "get the family" in {
      table.put("row1".getBytes(charset), "cf1".getBytes(charset), Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset)))
      val columns = table.get("row1", "cf1")
      columns.size === 2
      new String(columns(0).value, charset) === "v1"
      new String(columns(1).value, charset) === "v2"

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
      table.put("row1".getBytes(charset),
        ColumnFamily("cf1".getBytes(charset), Seq(Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset)))),
        ColumnFamily("cf2".getBytes(charset), Seq(Column("c3".getBytes(charset), "v3".getBytes(charset))))
      )
      val families = table.get("row1")
      families.size === 2
      families(0).columns.size === 2
      families(1).columns.size === 1
      new String(families(0).family, charset) === "cf1"
      new String(families(1).family, charset) === "cf2"

      new String(families(0).columns(0).value, charset) === "v1"
      new String(families(0).columns(1).value, charset) === "v2"
      new String(families(1).columns(0).value, charset) === "v3"

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
      val row1 = Row("row1".getBytes(charset),
        Seq(ColumnFamily("cf1".getBytes(charset), Seq(Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset)))),
           ColumnFamily("cf2".getBytes(charset), Seq(Column("c3".getBytes(charset), "v3".getBytes(charset))))))

      val row2 = Row("row2".getBytes(charset),
        Seq(ColumnFamily("cf1".getBytes(charset), Seq(Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset))))))

      table.put(row1, row2)

      val keys = Seq("row1", "row2").map(_.getBytes(charset))
      val rows = table.get(keys)
      rows.size === 2
      rows(0).families.size === 2
      rows(1).families.size === 1

      table.delete(keys)
      table.get(keys).size === 0
    }

    "scan" in {
      val row1 = Row("row1".getBytes(charset),
        Seq(ColumnFamily("cf1".getBytes(charset), Seq(Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset)))),
          ColumnFamily("cf2".getBytes(charset), Seq(Column("c3".getBytes(charset), "v3".getBytes(charset))))))

      val row2 = Row("row2".getBytes(charset),
        Seq(ColumnFamily("cf1".getBytes(charset), Seq(Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset))))))

      val row3 = Row("row3".getBytes(charset),
        Seq(ColumnFamily("cf1".getBytes(charset), Seq(Column("c1".getBytes(charset), "v1".getBytes(charset)), Column("c2".getBytes(charset), "v2".getBytes(charset))))))

      table.put(row1, row2, row3)

      val scanner = table.scan("row1", "row3")
      val r1 = scanner.next
      new String(r1.row, charset) === "row1"
      val r2 = scanner.next
      new String(r2.row, charset) === "row2"
      scanner.hasNext === false
      scanner.close

      val keys = Seq("row1", "row2", "row3").map(_.getBytes(charset))
      table.delete(keys)
      table.get(keys).size === 0
    }

    "rollback" in {
      table.put("row1", "cf1", "c1", "v1")
      table.put("row1", "cf1", "c1", "v2")
      new String(table.get("row1", "cf1", "c1").get, charset) === "v2"
      table.rollback("row1", "cf1", "c1")
      new String(table.get("row1", "cf1", "c1").get, charset) === "v1"
      table.rollback("row1", "cf1", "c1")
      table.get("row1", "cf1", "c1") === None
      table.delete("row1", "cf1", "c1")
      table.get("row1", "cf1", "c1") === None
    }

    "append" in {
      table.put("row1", "cf1", "c1", "v1")
      table.append("row1", "cf1", "c1", "v2".getBytes(charset))
      new String(table.get("row1", "cf1", "c1").get, charset) === "v1v2"
      table.delete("row1", "cf1", "c1")
      table.get("row1", "cf1", "c1") === None
    }

    "counter" in {
      table.getCounter("row1", "cf1", "counter") === 0
      table.addCounter("row1", "cf1", "counter", 10)
      table.getCounter("row1", "cf1", "counter") === 10
      table.addCounter("row1", "cf1", "counter", -5)
      table.getCounter("row1", "cf1", "counter") === 5
      table.delete("row1", "cf1", "counter")
      table.get("row1", "cf1", "counter") === None
    }
  }
}
