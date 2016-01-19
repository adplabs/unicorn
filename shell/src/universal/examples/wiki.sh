#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

// import wikipedia dump to unibase

import java.util._
import scala.collection.mutable.Stack
import scala.io.Source
import scala.xml.pull._
import unicorn.json._
import unicorn.bigtable._
import unicorn.bigtable.accumulo._
import unicorn.bigtable.hbase._
import unicorn.unibase._

def wikipedia(bucket: HBaseBucket, files: String*): Unit = {
  files.foreach { xmlFile =>
    val xml = new XMLEventReader(Source.fromFile(xmlFile))

    var field: String = null
    val doc = Stack[JsObject]()
    for (event <- xml) {
      event match {
        case EvElemStart(_, "page", _, _) => {
          doc.push(JsObject())
        }
        case EvElemEnd(_, "page") => {
          if (!doc.isEmpty) {
            val d = doc.pop
            assert(doc.isEmpty)

            d("ns") match {
              case JsString(value) if value == "0" =>
                val title = d("title") match {
                  case JsString(value) => value
                  case _ => ""
                }

                if (title != "") {
                  d("_id") = d.id.toString.toInt
                  println(d("_id"))
                  bucket.upsert(d)
                }
              case _ =>
            }
          }
        }
        case e @ EvElemStart(_, tag, _, _) => {
          if (!doc.isEmpty) {
            if (field != null) {
              val child = JsObject()
              val parent = doc.top
              parent(field) = child
              doc.push(child)
            }
            field = tag
          }
        }
        case e @ EvElemEnd(_, tag) => {
          if (field == null) {
            if (!doc.isEmpty) doc.pop
          }
          else field = null
        }
        case EvText(t) => {
          if (!doc.isEmpty && field != null) {
            doc.top(field) = t
          }
        }
        case _ => // ignore
      }
    }
  }
}

val hbase = UniBase(HBase())
hbase.createBucket("wiki")
val bucket = hbase("wiki")

wikipedia(bucket, "/Users/lihb/data/wiki/enwikinews-20140410-pages-articles-multistream.xml")
