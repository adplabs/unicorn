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

import scala.concurrent.Future

import spray.routing._
import spray.http._
import spray.util.LoggingContext
import MediaTypes._
import com.typesafe.config.ConfigFactory

import unicorn._, json._
import unicorn.bigtable._, accumulo._, cassandra._, hbase._
import unicorn.unibase._
import unicorn.util.Logging

/**
 * @author Haifeng Li
 */
class RhinoActor extends HttpServiceActor with HttpService with Logging {
  import context.dispatcher

  implicit def exceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e @ (_: IllegalArgumentException | _: UnsupportedOperationException) =>
        requestInstance { request =>
          log.error("{} encountered while handling request: {}", e, request)
          complete(StatusCodes.BadRequest, e.toString)
        }
    }

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  override def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(staticRoute ~ apiRoute)

  val config = ConfigFactory.load().getConfig("unicorn.rhino")

  val unibase = config.getString("bigtable") match {
    case "hbase" => Unibase(HBase())
    case "accumulo" =>
      Unibase(Accumulo(
        config.getString("accumulo.instance"), config.getString("accumulo.zookeeper"),
        config.getString("accumulo.user"), config.getString("accumulo.password")))
    case "cassandra" =>
      Unibase(Cassandra(config.getString("cassandra.host"), config.getInt("cassandra.port")))
    case bigtable =>
      log.error(s"Unknown BigTable setting: $bigtable, try HBase")
      Unibase(HBase())
  }

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

  def hv(name: String) = headerValueByName(name)
  def ohv(name: String) = optionalHeaderValueByName(name)

  val apiRoute = {
    path("table" / Segment / Segment) { (table, id) =>
      ohv("tenant") { tenantId =>
        implicit val tenant = tenantId.map(_.parseJson)
        get {
          $get(table, s""""$id"""".parseJson)
        } ~
        delete {
          $delete(table, s""""$id"""".parseJson)
        }
      }
    } ~
    path("table" / Segment) { table =>
      ohv("tenant") { tenantId =>
        implicit val tenant = tenantId.map(_.parseJson)
        rawJson { doc =>
          get {
            $get(table, doc.parseJson)
          } ~
          delete {
            $delete(table, doc.parseJson)
          } ~
          post {
            $upsert(table, doc)
          } ~
          put {
            $insert(table, doc)
          } ~
          patch {
            $update(table, doc)
          }
        }
      }
    } ~
    path("list" / "tables") {
      $list
    }
  }

  private def json(doc: String) = doc.parseJsObject

  private def bucket(table: String, tenant: Option[JsValue]): Table = {
    val db = unibase(table)
    tenant match {
      case Some(tenant) => db.tenant = tenant
      case None => ()
    }
    db
  }

  // name it "get" will conflict with spray routing "get"
  private def $get(table: String, id: JsValue)(implicit tenant: Option[JsValue]) = {
    onSuccess(Future {
      val db = bucket(table, tenant)
      db(id)
    }) { doc =>
      respondWithMediaType(`application/json`) {
        complete(doc match {
          case None => StatusCodes.NotFound
          case Some(doc) => doc.prettyPrint
        })
      }
    }
  }

  private def $upsert(table: String, doc: String)(implicit tenant: Option[JsValue]) = {
    onSuccess(Future {
      val db = bucket(table, tenant)
      db.upsert(json(doc))
    }) { key =>
      respondWithMediaType(`application/json`) {
        val response = JsObject("_id" -> key)
        complete(response.toString)
      }
    }
  }

  private def $insert(table: String, doc: String)(implicit tenant: Option[JsValue]) = {
    onSuccess(Future {
      val db = bucket(table, tenant)
      db.insert(json(doc))
    }) { Unit =>
      complete(StatusCodes.OK)
    }
  }

  private def $update(table: String, doc: String)(implicit tenant: Option[JsValue]) = {
    onSuccess(Future {
      val db = bucket(table, tenant)
      db.update(json(doc))
    }) { Unit =>
      complete(StatusCodes.OK)
    }
  }

  def $delete(table: String, id: JsValue)(implicit tenant: Option[JsValue]) = {
    onSuccess(Future {
      val db = bucket(table, tenant)
      db.delete(id)
    }) { Unit =>
      complete(StatusCodes.OK)
    }
  }

  def $list = {
    onSuccess(Future {
      val tables: JsArray = unibase.tables.toSeq
      tables
    }) { tables =>
      complete(tables.toString)
    }
  }
}
