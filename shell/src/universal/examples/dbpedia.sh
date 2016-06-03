#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

// import dbpedia to unibase

import scala.io.Source
import unicorn.json._
import unicorn.bigtable.rocksdb._
import unicorn.unibase._
import unicorn.unibase.graph._

val db = Unibase(RocksDB.create("/tmp/unicorn-dbpedia"))
db.createGraph("dbpedia")
val dbpedia = db.graph("dbpedia", new Snowflake(0))

def triple(line: String): (String, String, String) = {
  val tokens = line.split(" ", 3)
  val subject = java.net.URLDecoder.decode(tokens(0).replace("<http://dbpedia.org/resource/", ""), "UTF-8").replace(">", "")
  val predicate = java.net.URLDecoder.decode(tokens(1), "UTF-8").split("/").last.split("#").last.replace(">", "")
  var obj = if (tokens(2).startsWith("<")) java.net.URLDecoder.decode(tokens(2), "UTF-8")
    else org.apache.commons.lang.StringEscapeUtils.unescapeJava(tokens(2))

  if (obj.endsWith(" ."))
    obj = obj.substring(0, obj.length-2)

  if (obj.endsWith("@en"))
    obj = obj.substring(0, obj.length-3)

  (subject, predicate, obj.replaceAll("^\"|\"$", ""))
}

def labels(graph: Graph, files: String*): Unit = {
  files.foreach { file =>
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        graph.addVertex(nt._1, JsObject("label" -> JsString(nt._3)))
      }
    }
  }
}

labels(dbpedia, "../../data/dbpedia/labels_en.nt")

def links(graph: Graph, files: String*): Unit = {
  files.foreach { file =>
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        graph.addEdge(nt._1, "e", nt._3, JsNull)
      }
    }
  }
}

links(dbpedia, "../../data/dbpedia/page_links_en.nt")
