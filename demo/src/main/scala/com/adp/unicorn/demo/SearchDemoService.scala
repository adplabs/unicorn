package com.adp.unicorn.demo

import akka.actor.Actor
import spray.routing._
import spray.http._
import spray.http.HttpHeaders.RawHeader
import spray.json._
import MediaTypes._

import com.adp.unicorn._
import com.adp.unicorn.store.cassandra.CassandraServer
import com.adp.unicorn.search.TextSearch


// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class SearchDemoServiceActor extends Actor with SearchDemoService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(apiRoute ~ staticRoute)
}


// this trait defines our service behavior independently from the service actor
trait SearchDemoService extends HttpService {
  val host = System.getProperty("adp.unicorn.demo.cassandra.host", "localhost")
  val port = System.getProperty("adp.unicorn.demo.cassandra.port", "9160").toInt
  val server = CassandraServer(host, port)

  val db = server.dataset(System.getProperty("adp.unicorn.demo.database", "dbpedia"))

  val numDocs = 4004478
  val pagerank = new Document("unicorn.text.corpus.text.page_rank", "text_index").from(db)
  val pr = math.log(0.85 / numDocs)
  val suffix = "##abstract"

  val index = TextSearch(db, numDocs)

  val staticRoute = {
    get {
      getFromResourceDirectory("web")
    }
  }

  val apiRoute = get {
    path("doc" / Segment) { id =>
      getDocument(id)
    } ~
    path("link" / Segment) { id =>
      getLink(id)
    } ~
    path("search" / Segment) { query =>
      search(query)
    }
  }

  def getDocument(id: String) = {
    val doc = db.get(id)
    respondWithMediaType(`text/html`) {
        complete(html.doc(doc).toString)
    }
  }

  def getLink(id: String) = {
    respondWithMediaType(`application/json`) {
      complete {
        import com.adp.unicorn.JsonValueImplicits._
        val doc = db.get(id)
        pagerank.select((doc.links.map { case ((_, target), _) => target + suffix }.toArray :+ (id + suffix)): _*)

        var idx = 0
        val rank = pagerank(id + suffix) match {
          case JsonDouble(value) => math.log(value)
          case _ => pr
        }
        val center = JsonObject(
          "id" -> id,
          "index" -> 0,
          "rank" -> rank
        )

        val nodes = center +: doc.links.map{ case ((_, target), value) =>
          idx += 1
          val rank = pagerank(target + suffix) match {
            case JsonDouble(value) => math.log(value)
            case _ => pr
          }
          JsonObject(
            "id" -> target,
            "index" -> idx,
            "rank" -> rank
          )
        }.toArray

        idx = 0
        val links = doc.links.map { case ((_, target), value) =>
          val weight: Double = value
          idx += 1
          JsonObject(
            "source" -> 0,
            "target" -> idx,
            "weight" -> weight
          )
        }.toArray

        JsonObject(
          "nodes" -> JsonArray(nodes: _*),
          "links" -> JsonArray(links: _*)
        ).prettyPrint
      }
    }
  }

  def search(query: String) = {
    val hits = index.search(query.split("\\s+"): _*).map(_._1._1)
    respondWithMediaType(`text/html`) {
      complete(html.search(hits).toString)
    }
  }
}
