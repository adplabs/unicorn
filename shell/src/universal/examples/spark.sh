#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

import java.util._
import unicorn.json._
import unicorn.bigtable._
import unicorn.bigtable.hbase._
import unicorn.unibase._
import unicorn.narwhal._
import unicorn.graph._
import org.apache.spark._
//import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val conf = new SparkConf().setAppName("unicorn").setMaster("local[4]")
val sc = new SparkContext(conf)
val db = new Narwhal(HBase())
val rdd = db.rdd(sc, "narwhal")
rdd.count()