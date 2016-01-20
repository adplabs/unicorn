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

package unicorn.unibase

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable.accumulo.Accumulo
import unicorn.json._

/**
 * @author Haifeng Li
 */
class BucketSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val bigtable = Accumulo()
  val db = new UniBase(bigtable)
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
    db.createBucket(tableName)
  }

  override def afterAll = {
    db.dropBucket(tableName)
  }

  "Bucket" should {
    "upsert, get, remove" in {
      val bucket = db(tableName)
      val key = bucket.upsert(json)
      key === json("_id")
      val obj = bucket(key)
      obj.get === json

      bucket.delete(key)
      bucket(key) === None
    }
    "insert" in {
      val bucket = db(tableName)
      bucket.upsert(json)
      bucket.insert(json) must throwA[IllegalArgumentException]
    }
    "locality" in {
      val locality = Map("_id" -> "id", "store" -> "store").withDefaultValue("doc")
      db.createBucket("unibase_test_locality", Seq("id", "doc", "store"), locality)
      val bucket = db("unibase_test_locality")
      val key = bucket.upsert(json)
      key === json("_id")
      val obj = bucket(key)
      obj.get === json

      bucket.get(JsObject("_id" -> key, "owner" -> 1, "address" -> 1)).get === JsObject("_id" -> key, "owner" -> json.owner, "phone" -> json.phone, "address" -> json.address)
      bucket(key, "store").get === JsObject("_id" -> key, "store" -> json.store)

      bucket.delete(key)
      bucket(key) === None

      db.dropBucket("unibase_test_locality")
      bigtable.tableExists("unibase_test_locality") === false
    }
    "update.set" in {
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
    }
    "update.unset" in {
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
    }
    "append only" in {
      db.createBucket("unicorn_append_only", appendOnly = true)
      val bucket = db("unicorn_append_only")
      bucket.delete(JsString("key")) must throwA[UnsupportedOperationException]
      bucket.update(JsObject("a" -> JsInt(1))) must throwA[UnsupportedOperationException]
      db.dropBucket("unicorn_append_only")
      1 === 1
    }
    "multi-tenancy" in {
      val bucket = db(tableName)
      bucket.tenant = Some("IBM")
      val key = bucket.upsert(json)
      bucket.tenant = Some("ADP")
      bucket(key) === None
    }
  }
}
