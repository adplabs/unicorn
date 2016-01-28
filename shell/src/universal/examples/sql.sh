#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

import unicorn.json._
import unicorn.bigtable.hbase.HBase
import unicorn.unibase._
import unicorn.sql.hunibase2SQL

val hbase = Unibase(HBase())
hbase.createTable("unicorn_sql")
val table = hbase("unicorn_sql")

val haifeng = JsObject(
  "name" -> "Haifeng",
  "salary" -> 1
)

val roberto = JsObject(
  "name" -> "Roberto",
  "salary" -> 3
)

val jerome = JsObject(
  "name" -> "Jerome",
  "salary" -> 2
)

val keith = JsObject(
  "name" -> "Keith",
  "salary" -> 2
)

val amit = JsObject(
  "name" -> "Amit",
  "salary" -> 3
)

val stuart = JsObject(
  "name" -> "Stuart",
  "salary" -> 4
)

val don = JsObject(
  "name" -> "Don",
  "salary" -> 4
)

val carlos = JsObject(
  "name" -> "Carlos",
  "salary" -> 5
)

table.upsert(haifeng)
table.upsert(roberto)
table.upsert(jerome)
table.upsert(keith)
table.upsert(amit)
table.upsert(stuart)
table.upsert(don)
table.upsert(carlos)

val it = hbase.sql("select * from unicorn_sql where name = 'Haifeng'")
it.foreach(println(_))

hbase.dropTable("unicorn_sql")