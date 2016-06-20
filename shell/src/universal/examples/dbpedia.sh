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

dbpedia.rdf("../../data/dbpedia/page_links_en.nt")