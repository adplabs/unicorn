package com.adp.unicorn.demo

import akka.actor.Actor
import spray.routing._
import spray.http._
import spray.http.HttpHeaders.RawHeader
import spray.json._
import MediaTypes._

import com.adp.unicorn.Document
import com.adp.unicorn.store.cassandra.CassandraServer

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
  val server = CassandraServer("127.0.0.1", 9160)
  val db = server.dataset("dbpedia")

  val pagerank = new Document("unicorn.text.corpus.text.page_rank", "text_index").from(db)
  val pr = math.log(0.85 / 4004478)
  val suffix = "##abstract"


  val staticRoute = {
    get {
      getFromResourceDirectory("web")
    }
  }

  val apiRoute = get {
    complete("done")
    /*
    path("doc" / Segment) { id =>
      getDocument(id)
    } ~
    path("link" / Segment) { id =>
      getLink(oid)
    } ~
    path("search" / Segment) { key =>
      search(key)
    }
    */
  }
/*
  def getDocument(id: String) = {
    complete {
      val doc = db.get(id)
    }
  }

  def getLink(id: String) = {
    respondWithMediaType(`application/json`) {
      complete {
        val doc = db.get(id)
      }
    }
  }

  def search(key: String) = {

  }
  */
}
