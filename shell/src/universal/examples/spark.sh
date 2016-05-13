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
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val conf = new SparkConf().setAppName("unicorn").setMaster("local[4]")
val sc = new SparkContext(conf)
val db = new Narwhal(HBase())

val table = db("worker")
table.tenant = "IBM"
val rdd = table.rdd(sc)
rdd.count()

val table = db("narwhal")
val rdd = table.rdd(sc, json"""
                          {
                            "$$or": [
                              {
                                "age": {"$$gt": 30}
                              },
                              {
                                "home_based": false
                              }
                            ]
                          }
                        """)
rdd.count()


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

case class Worker(name: String, age: Int)
val workers = rdd.map { js => Worker(js.name, js.age) }
val df = sqlContext.createDataFrame(workers)
df.show

df.registerTempTable("worker")
sqlContext.sql("SELECT * FROM worker WHERE age > 30").show