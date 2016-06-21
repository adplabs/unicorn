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

// Although we can parse .gz file directly, we don't support bz2 compressed files.
// Please download and unzip first.
dbpedia.rdf("http://downloads.dbpedia.org/2015-10/core-i18n/en/page_links_en.ttl.bz2")