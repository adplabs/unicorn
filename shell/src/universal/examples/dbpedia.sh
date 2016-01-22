#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

// import dbpedia to unibase

import java.util._
import scala.collection.mutable.Stack
import scala.io.Source
import scala.xml.pull._
import unicorn.json._
import unicorn.bigtable._
import unicorn.bigtable.accumulo._
import unicorn.bigtable.hbase._
import unicorn.unibase._

val hbase = Unibase(HBase())
//hbase.createTable("dbpedia")
val bucket = hbase("dbpedia")

def triple(line: String): (String, String, String, Boolean, Boolean) = {
  val tokens = line.split(" ", 3)
  val subject = java.net.URLDecoder.decode(tokens(0).replace("<http://dbpedia.org/resource/", ""), "UTF-8").replace(">", "")
  val predicate = java.net.URLDecoder.decode(tokens(1), "UTF-8").split("/").last.split("#").last.replace(">", "")
  var obj = if (tokens(2).startsWith("<")) java.net.URLDecoder.decode(tokens(2), "UTF-8")
    else org.apache.commons.lang.StringEscapeUtils.unescapeJava(tokens(2))

  if (obj.endsWith(" ."))
    obj = obj.substring(0, obj.length-2)

  if (obj.endsWith("@en"))
    obj = obj.substring(0, obj.length-3)

  obj = obj.replace("^^<http://www.w3.org/2001/XMLSchema#date>", "")
  obj = obj.replace("^^<http://dbpedia.org/datatype/usDollar>", " USD")
  obj = obj.replace("^^<http://dbpedia.org/datatype/euro>", " EUR")
  obj = obj.replace("^^<http://dbpedia.org/datatype/poundSterling>", " GBP")
  if (obj.indexOf("^^<http://dbpedia.org/datatype/") != -1) {
    obj = obj.replace("^^<http://dbpedia.org/datatype/", "_break_datatype_")
    val s = obj.split("_break_datatype_")

    if (s(0).startsWith("\"") && s(0).endsWith("\""))
      s(0) = s(0).substring(1, s(0).length-1)

    obj = s(0) + " " + Character.toUpperCase(s(1).charAt(0)) + s(1).substring(1)
    if (obj.endsWith(">")) obj = obj.substring(0, obj.length-1)
  }

  var isFloat = false
  var isInt = false

  if (obj.indexOf("^^<http://www.w3.org/2001/XMLSchema#gYear>") != -1) {
    obj = obj.replace("^^<http://www.w3.org/2001/XMLSchema#gYear>", "")
    isInt = true
  }

  if (obj.indexOf("^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>") != -1) {
    obj = obj.replace("^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>", "")
    isInt = true
  }

  if (obj.indexOf("^^<http://www.w3.org/2001/XMLSchema#integer>") != -1) {
    obj = obj.replace("^^<http://www.w3.org/2001/XMLSchema#integer>", "")
    isInt = true
  }

  if (obj.indexOf("^^<http://www.w3.org/2001/XMLSchema#float>") != -1) {
    obj = obj.replace("^^<http://www.w3.org/2001/XMLSchema#float>", "")
    isFloat = true
  }

  if (obj.indexOf("^^<http://www.w3.org/2001/XMLSchema#double>") != -1) {
    obj = obj.replace("^^<http://www.w3.org/2001/XMLSchema#double>", "")
    isFloat = true
  }

  if (obj.startsWith("<") && obj.endsWith(">")) {
    obj = obj.split("/").last.split("#").last
    obj = obj.substring(0, obj.length-1)
  }
  
  if (obj.startsWith("\"") && obj.endsWith("\""))
    obj = obj.substring(1, obj.length-1)

  if (obj.endsWith(">"))
    obj = obj.substring(0, obj.length-1)

  (subject, predicate, obj, isInt, isFloat)
}

def types(table: HBaseBucket, files: String*): Unit = {
  files.foreach { file =>
    var doc = JsObject()
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        if (nt._1 != doc(Unibase.$id).toString) {
          if (doc(Unibase.$id) != JsUndefined) {
            println(doc)
            bucket.upsert(doc)
          }
          doc = JsObject(Unibase.$id -> JsString(nt._1))
        } else {
          doc(nt._3) = "type"
        }
      }
    }
  }
}

types(bucket, "../../data/dbpedia/instance_types_en.nt")

def properties(table: HBaseBucket, files: String*): Unit = {
  files.foreach { file =>
    var doc = JsObject()
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        if (nt._1 != doc.id) {
          println(doc.id)
          table.update(doc)
          doc = JsObject(Unibase.$id -> JsString(nt._1))
        } else {
          val obj = nt._3
          val isInt = nt._4
          val isFloat = nt._5
          val value: JsonValue =
           if (isInt) {
            try {
              JsonIntValue(Integer.valueOf(obj))
            } catch {
              case _ : Throwable => JsonLongValue(obj.toLong)
            }
           } else if (isFloat) JsonDoubleValue(obj.toDouble)
           else JsonStringValue(obj)

          doc(nt._2) match {
            case JsonUndefinedValue => doc(nt._2) = nt._3
            case JsonArrayValue(array) => doc(nt._2) = (array :+ value)
            case first =>
              val array = Array[JsonValue](first, value)
              doc(nt._2) = JsonArrayValue(array)
          }
        }
      }
    }
  }
}

properties(bucket, "../../data/dbpedia/mappingbased_properties_cleaned_en.nt")

def abstracts(table: HBaseBucket, files: String*): Unit = {
  val corpus = TextIndexBuilder(table)
  //var start = false
  files.foreach { file =>
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        //if (nt._1 == "Ignatius_Bryanchaninov") start = true
        //if (!nt._3.isEmpty && start) {
        if (!nt._3.isEmpty) {
          val doc = Document(nt._1)
          doc("abstract") = nt._3
          println(doc.id)
          doc into table
          corpus.add(doc.id, "abstract", nt._3)
        }
      }
    }
  }
}

abstracts(bucket, "../../data/dbpedia/long_abstracts_en.nt")

def titles(table: HBaseBucket, files: String*): Unit = {
  val corpus = TextIndexBuilder(table)
  files.foreach { file =>
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        if (!nt._1.contains("/") && !nt._3.isEmpty) {
          val doc = Document(nt._1)
          //doc("title") = nt._3.replaceAll("(?<=\\p{Ll})(?=\\p{Lu})|(?<=\\p{L})(?=\\p{Lu}\\p{Ll})|_", " ")
          doc("title") = nt._3.replace("_", " ")
          println(doc.id)
          doc into table
          corpus.addTitle(doc.id, "abstract", nt._3)
        }
      }
    }
  }
}

titles(bucket, "../../data/dbpedia/labels_en.nt")

def geo(table: HBaseBucket, files: String*): Unit = {
  files.foreach { file =>
    var doc: Document = null
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        if (doc == null) doc = Document(nt._1)
        if (nt._1 != doc.id) {
          println(doc.id)
          doc into table
          doc = Document(nt._1)
        } else {
          if (nt._2 == "lat" || nt._2 == "long") {
            val obj = nt._3
            val value: JsonValue = JsonDoubleValue(obj.toDouble)
            doc(nt._2) = value
          }
        }
      }
    }
  }
}

geo(bucket, "../../data/dbpedia/geo_coordinates_en.nt")

def link(table: HBaseBucket, files: String*): Unit = {
  files.foreach { file =>
    var doc: Document = null
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = triple(line)
        if (doc == null) doc = Document(nt._1)
        if (nt._1 != doc.id) {
          println(doc.id)
          doc into table
          doc = Document(nt._1)
        } else {
          doc("link", nt._3) = true
        }
      }
    }
  }
}

link(bucket, "../../data/dbpedia/page_links_en.nt")