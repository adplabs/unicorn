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

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.ExecutionContext, ExecutionContext.Implicits.global

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._

import unicorn._, json._
import unicorn.bigtable.BigTable
import unicorn.bigtable.hbase.HBase
import unicorn.oid.BsonObjectId
import unicorn.unibase._


/**
 * @author Haifeng Li
 */
class RhinoActor extends Actor with Rhino {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(apiRoute ~ staticRoute)
}


// this trait defines our service behavior independently from the service actor
trait Rhino extends HttpService {
  val unibase = new HUniBase(HBase())

  def rawJson = extract { _.request.entity.asString}

  val staticRoute = {
    get {
      path("") {
        getFromResource("web/index.html")
      } ~ {
        getFromResourceDirectory("web")
      }
    }
  }

  val apiRoute = {
    path(Segment / Segment) { (bucket, id) =>
      get {
        _get(bucket, id)
      } ~
      delete {
        remove(bucket, id)
      }
    } ~
    path(Segment) { bucket =>
      rawJson { doc =>
        post {
          upsert(bucket, doc)
        } ~
        put {
          insert(bucket, doc)
        } ~
        patch {
          update(bucket, doc)
        }
      }
    }
  }

  private def _id(id: String): JsValue = {
    id.split(":") match {
      case Array(_id, "UUID") => JsUUID(UUID.fromString(_id))
      case Array(_id, "BSONObjectId") => JsObjectId(BsonObjectId(_id))
      case Array(_id, "Long") => JsLong(_id.toLong)
      case Array(_id, "Int") => JsInt(_id.toInt)
      case _ => JsString(id)
    }
  }

  private def json(doc: String) = JsonParser(doc).asInstanceOf[JsObject]

  // name it "get" will conflict with spray routing "get"
  private def _get(bucket: String, id: String, fields: Option[String] = None)(implicit ec: ExecutionContext) = {
    onSuccess(Future(unibase(bucket)(_id(id)))) { doc =>
      respondWithMediaType(`application/json`) {
        complete(doc match {
          case None => StatusCodes.NotFound
          case Some(doc) => doc.prettyPrint
        })
      }
    }
  }

  private def upsert(bucket: String, doc: String) = {
    onSuccess(Future(unibase(bucket).upsert(json(doc)))) { Unit =>
      respondWithMediaType(`application/json`) {
        complete("{}")
      }
    }
  }

  private def insert(bucket: String, doc: String) = {
    onSuccess(Future(unibase(bucket).insert(json(doc)))) { Unit =>
      respondWithMediaType(`application/json`) {
        complete("{}")
      }
    }
  }

  private def update(bucket: String, doc: String) = {
    onSuccess(Future(unibase(bucket).update(json(doc)))) { Unit =>
      respondWithMediaType(`application/json`) {
        complete("{}")
      }
    }
  }

  def remove(bucket: String, id: String) = {
    onSuccess(Future(unibase(bucket).delete(_id(id)))) { Unit =>
      respondWithMediaType(`application/json`) {
        complete("{}")
      }
    }
  }

}
