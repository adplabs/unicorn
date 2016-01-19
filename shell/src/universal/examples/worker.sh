#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

import java.util._
import unicorn.json._
import unicorn.bigtable._
import unicorn.bigtable.accumulo._
import unicorn.bigtable.hbase._
import unicorn.unibase._

// measure running time of a function/block 
def time[A](f: => A) = {
  val s = System.nanoTime
  val ret = f
  if (ret.isInstanceOf[JsValue]) println(ret.asInstanceOf[JsValue].prettyPrint)
  else println(ret)
  println("time: " + (System.nanoTime - s)/1e6 + " ms")
  ret
}

// connect to Accumulo mock
val accumulo = UniBase(Accumulo())
time { accumulo.createBucket("worker") }
val bucket = accumulo("worker")

// Read a non-existing row. It is the pure time of round trip.
time { bucket("row1") }

// Create a document 
val person = JsObject(
  "name" -> "Haifeng",
  "gender" -> "Male",
  "salary" -> 1.0,
  "address" -> JsObject(
    "street" -> "135 W. 18th ST",
    "city" -> "New York",
    "state" -> "NY",
    "zip" -> 10011
  ),
  "project" -> JsArray("HCM", "Analytics"),
  "graph" -> JsObject(
    "work with" -> JsObject(
      "Jim" -> JsObject(
        "id" -> "Jim",
        "data" -> 1
      ),
      "Mike" -> JsObject(
        "id" -> "Mike",
        "data" -> 1
      )
    ),
    "report to" -> JsObject(
      "Tom" -> JsObject(
        "id" -> "Tome",
        "data" -> 1
      )
    )
  )
)


// save document into a dataset
val key = time { bucket.upsert(person) }

val worker = time { bucket(key).get }
worker.prettyPrint

// Read partially a document
val doc = time { bucket(key, "name").get }
doc.prettyPrint

val update = """
                 {
                   "$set": {
                     "salary": 100.0,
                     "address.street": "1 ADP Blvd"
                   },
                   "$unset": {
                     "gender": 1
                   }
                 }
             """.parseJson.asInstanceOf[JsObject]

update(UniBase.$id) = key
time { bucket.update(update) }

val updated = time { bucket(key, "name").get }
updated.prettyPrint

// HBase
val hbase = UniBase(HBase())

time { hbase.createBucket("worker") }
val hbucket = hbase("worker")

time { hbucket.upsert(person) }

val asOfDate = new Date

time { hbucket.update(update) }

val old = time { hbucket(asOfDate, key).get }
old.prettyPrint

val latest = time { hbucket(key).get }
latest.prettyPrint

// delete the bucket
time { hbase.dropBucket("worker") }
