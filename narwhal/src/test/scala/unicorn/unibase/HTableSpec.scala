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

package unicorn.narwhal

import java.util.Date
import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable.hbase.HBase
import unicorn.json._

/**
 * @author Haifeng Li
 */
class HTableSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential

  val bigtable = HBase()
  val db = new Narwhal(bigtable)
  val tableName = "unicorn_unibase_test"
  val json = JsonParser(
    """
      |{
      |  "owner": "Rich",
      |  "phone": "123-456-7890",
      |  "address": {
      |    "street": "1 ADP Blvd.",
      |    "city": "Roseland",
      |    "state": "NJ"
      |  },
      |  "store": {
      |    "books": 10C,
      |    "book": [
      |      {
      |        "category": "reference",
      |        "author": "Nigel Rees",
      |        "title": "Sayings of the Century",
      |        "price": 8.95
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "Evelyn Waugh",
      |        "title": "Sword of Honour",
      |        "price": 12.99
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "Herman Melville",
      |        "title": "Moby Dick",
      |        "isbn": "0-553-21311-3",
      |        "price": 8.99
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "J. R. R. Tolkien",
      |        "title": "The Lord of the Rings",
      |        "isbn": "0-395-19395-8",
      |        "price": 22.99
      |      }
      |    ],
      |    "bicycle": {
      |      "color": "red",
      |      "price": 19.95
      |    }
      |  }
      |}
    """.stripMargin).asInstanceOf[JsObject]

  override def beforeAll = {
    db.createTable(tableName)
  }

  override def afterAll = {
    db.dropTable(tableName)
  }

  "HTable" should {
    "insert" in {
      val bucket = db(tableName)
      val key = bucket.upsert(json)
      key === json("_id")
      val obj = bucket(key)
      obj.get === json

      bucket.insert(json) must throwA[IllegalArgumentException]
      bucket.delete(key)
      bucket(key) === None
    }
    "inc" in {
      val bucket = db(tableName)
      val key = bucket.upsert(json)

      val update = JsonParser(
        """
          | {
          |   "$inc": {
          |     "store.books": 10
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      update("_id") = key
      bucket.update(update)

      val doc = bucket(key).get
      doc.store.books === JsCounter(20)

      val subtraction = JsonParser(
        """
          | {
          |   "$inc": {
          |     "store.books": -30
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      subtraction("_id") = key
      bucket.update(subtraction)

      val doc2 = bucket(key).get
      doc2.store.books === JsCounter(-10)
    }
    "rollback set" in {
      val bucket = db(tableName)
      val key = bucket.upsert(json)

      val update = JsonParser(
        """
          | {
          |   "$set": {
          |     "owner": "Poor",
          |     "gender": "M",
          |     "store.book.0.price": 9.95
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      update("_id") = key
      bucket.update(update)

      val doc = bucket(key, "owner", "store.book.0").get
      doc.owner === JsString("Poor")
      doc.gender === JsString("M")
      doc.store.book(0).price === JsDouble(9.95)

      val rollback = JsonParser(
        """
          | {
          |   "$rollback": {
          |     "owner": 1,
          |     "gender": 1,
          |     "store.book.0.price": 1
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      rollback("_id") = key
      bucket.update(rollback)

      val old = bucket(key, "owner", "store.book.0").get
      old.owner === JsString("Rich")
      old.gender === JsUndefined
      old.store.book(0).price === JsDouble(8.95)
    }
    "rollback unset" in {
      val bucket = db(tableName)
      val key = bucket.upsert(json)

      val update = JsonParser(
        """
          | {
          |   "$unset": {
          |     "owner": 1,
          |     "address": 1,
          |     "store.book.0": 1
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      update("_id") = key
      bucket.update(update)

      val doc = bucket(key, "owner", "store.book.0").get
      doc.owner === JsUndefined
      doc.address === JsUndefined
      doc.store.book(0) === JsUndefined

      val rollback = JsonParser(
        """
          | {
          |   "$rollback": {
          |     "owner": 1,
          |     "address": 1,
          |     "store.book.0": 1
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      rollback("_id") = key
      bucket.update(rollback)

      val old = bucket(key, "owner", "store.book.0").get
      old.owner === JsString("Rich")
      old.address === json.address
      old.store.book(0).price === JsDouble(8.95)
    }
    "time travel" in {
      val bucket = db(tableName)
      val key = bucket.upsert(json)

      val asOfDate = new Date

      val update = JsonParser(
        """
          | {
          |   "$set": {
          |     "owner": "Poor",
          |     "gender": "M",
          |     "store.book.0.price": 9.95
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]
      update("_id") = key
      bucket.update(update)

      val old = bucket(asOfDate, key)
      old.get.owner === JsString("Rich")

      val now = bucket(key)
      now.get.owner === JsString("Poor")
    }
    "find" in {
      val bucket = db(tableName)
      bucket.upsert("""{"name":"Tom","age":30,"state":"NY"}""".parseJsObject)
      bucket.upsert("""{"name":"Mike","age":40,"state":"NJ"}""".parseJsObject)
      bucket.upsert("""{"name":"Chris","age":30,"state":"NJ"}""".parseJsObject)

      val tom = bucket.find(JsObject("name" -> JsString("Tom")))
      tom.next.name === JsString("Tom")
      tom.hasNext === false

      val age = bucket.find(JsObject("age" -> JsInt(30)))
      age.next.age === JsInt(30)
      age.next.age === JsInt(30)
      age.hasNext === false

      val and  = bucket.find(JsObject("age" -> 30, "state" -> "NJ"))
      and.next.age === JsInt(30)
      and.hasNext === false

      val or  = bucket.find(JsObject("$or" -> JsArray(JsObject("age" -> JsObject("$gt" -> JsInt(30))), JsObject("state" -> JsString("NJ")))))
      val first = or.next
      (first.age == JsInt(40) || first.state == JsString("NJ")) === true
      val second = or.next
      (second.age == JsInt(40) || second.state == JsString("NJ")) === true
      or.hasNext === false
    }
    "multi-tenancy" in {
      val bucket = db(tableName)
      bucket.tenant = "IBM"
      bucket.upsert("""{"name":"Tom","age":30,"state":"NY"}""".parseJsObject)

      bucket.tenant = "ADP"
      bucket.upsert("""{"name":"Mike","age":40,"state":"NJ"}""".parseJsObject)
      bucket.upsert("""{"name":"Chris","age":30,"state":"NJ"}""".parseJsObject)

      bucket.find(json"""{"state": "NY"}""").size === 0
      bucket.tenant = "IBM"
      bucket.find(json"""{"state": "NY"}""").size === 1

      bucket.find(json"""{"state": "NJ"}""").size === 0
      bucket.tenant = "ADP"
      bucket.find(json"""{"state": "NJ"}""").size === 2
    }
    "spark" in {
      import org.apache.spark._

      val conf = new SparkConf().setAppName("unicorn").setMaster("local[4]")
      val sc = new SparkContext(conf)
      val db = new Narwhal(HBase())

      val table = db(tableName)
      table.tenant = "ADP"
      val rdd = table.rdd(sc)
      rdd.count() === 2

      val rdd30 = table.rdd(sc, json"""{"age": {"$$gt": 30}}""")
      rdd30.count() === 1

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val workers = rdd.map { js => Worker(js.name, js.age) }
      val df = sqlContext.createDataFrame(workers)
      df.show

      df.registerTempTable("worker")
      val sql = sqlContext.sql("SELECT * FROM worker WHERE age > 30")
      sql.show
      sql.count() === 1
    }
  }
}

case class Worker(name: String, age: Int)
