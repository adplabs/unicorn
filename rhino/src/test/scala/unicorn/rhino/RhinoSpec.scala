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

package unicorn.rhino

import org.specs2.mutable.{BeforeAfter, Specification}
import spray.testkit.Specs2RouteTest
import spray.http.{HttpEntity, HttpRequest}
import spray.http.HttpMethods._
import spray.http.StatusCodes._

import unicorn.json._
import unicorn.bigtable.hbase.HBase
import unicorn.unibase.Unibase

class RhinoSpec extends Specification with Specs2RouteTest with BeforeAfter with Rhino {
  // makes test execution sequential and prevents conflicts that may occur when the data is
  // changed simultaneously in the database
  sequential

  val table = "unicorn_rhino_test"
  val db = new Unibase(HBase())

  var key: JsValue = JsUndefined

  override def before = {
    db.createTable(table)
    val bucket = db(table)
    key = bucket.upsert(json)
  }

  override def after= {
    db.dropTable(table)
  }

  // connects the DSL to the test ActorSystem
  override def actorRefFactory = system

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

  "Rhino" should {
    "post" in {
      HttpRequest(POST, s"/$table", entity = HttpEntity(json.toString)) ~> apiRoute ~> check {
        response.status === OK
      }
    }

    "put" in {
      HttpRequest(PUT, s"/$table", entity = HttpEntity(json.toString)) ~> apiRoute ~> check {
        json("_id") = java.util.UUID.randomUUID
        response.status === OK
      }
    }

    "patch" in {
      HttpRequest(PATCH, s"/$table", entity = HttpEntity(update.toString)) ~> apiRoute ~> check {
        response.status === OK
      }
    }

    "delete" in {
      Delete(s"/$table/$key") ~> apiRoute ~> check {
        response.status === OK
      }
    }

    "get" in {
      Get(s"/$table/$key") ~> apiRoute ~> check {
        JsonParser(responseAs[String]) === json
      }
    }
  }
}
