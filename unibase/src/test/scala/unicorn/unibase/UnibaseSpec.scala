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
import unicorn.bigtable.hbase.HBase
import unicorn.json._
import unicorn.util.utf8

/**
 * @author Haifeng Li
 */
class UnibaseSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  //val hbase = HBase()
  val accumulo = Accumulo()
  val db = new Unibase(accumulo)
  val tableName = "unicorn_unibase_test"
  val json = JsonParser(
    """
      |{
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

  "Unibase" should {
    "get the put" in {
      val bucket = db(tableName)
      bucket.upsert(json)
      val obj = bucket(json("_id"))
      obj.get === json

      bucket.delete(json("_id"))
      bucket(json("_id")) === None
    }
    "append only" in {
      val bucket = db.createBucket("unicorn_append_only", appendOnly = true)
      bucket.delete(JsString("key")) must throwA[UnsupportedOperationException]
      bucket.update(JsObject("a" -> JsInt(1))) must throwA[UnsupportedOperationException]
      db.dropBucket("unicorn_append_only")
      1 === 1
    }
  }
}
