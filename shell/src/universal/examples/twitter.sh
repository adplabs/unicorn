#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

// import dbpedia to unibase

import scala.io.Source
import unicorn.json._
import unicorn.bigtable.rocksdb._
import unicorn.unibase._
import unicorn.unibase.graph._

val db = Unibase(RocksDB.create("/tmp/unicorn-twitter"))
db.createGraph("twitter")
val twitter = db.graph("twitter", new Snowflake(0))

// Please download and unzip first.
twitter.csv("http://an.kaist.ac.kr/~haewoon/release/twitter_social_graph/twitter_rv.zip", longVertexId = true)