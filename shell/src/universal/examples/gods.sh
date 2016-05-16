#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

import java.util._
import unicorn.json._
import unicorn.bigtable._
import unicorn.bigtable.accumulo._
import unicorn.bigtable.hbase._
import unicorn.unibase._
import unicorn.unibase.idgen._
import unicorn.unibase.graph._

val db = Unibase(HBase())
db.createGraph("gods")
val gods = db.graph("gods", Some(new Snowflake(0)))

val saturn = gods.addVertex(json"""{"label": "titan", "name": "saturn", "age": 10000}""")
val sky = gods.addVertex(json"""{"label": "location", "name": "sky"}""")
val sea = gods.addVertex(json"""{"label": "location", "name": "sea"}""")
val jupiter = gods.addVertex(json"""{"label": "god", "name": "jupiter", "age": 5000}""")
val neptune = gods.addVertex(json"""{"label": "god", "name": "neptune", "age": 4500}""")
val hercules = gods.addVertex(json"""{"label": "demigod", "name": "hercules", "age": 30}""")
val alcmene = gods.addVertex(json"""{"label": "human", "name": "alcmene", "age": 45}""")
val pluto = gods.addVertex(json"""{"label": "god", "name": "pluto", "age": 4000}""")
val nemean = gods.addVertex(json"""{"label": "monster", "name": "nemean"}""")
val hydra = gods.addVertex(json"""{"label": "monster", "name": "hydra"}""")
val cerberus = gods.addVertex(json"""{"label": "monster", "name": "cerberus"}""")
val tartarus = gods.addVertex(json"""{"label": "location", "name": "tartarus"}""")

gods.addEdge(jupiter, "father", saturn)
gods.addEdge(jupiter, "lives", sky, json"""{"reason": "loves fresh breezes"}""")
gods.addEdge(jupiter, "brother", neptune)
gods.addEdge(jupiter, "brother", pluto)

gods.addEdge(neptune, "lives", sea, json"""{"reason": "loves waves"}""")
gods.addEdge(neptune, "brother", jupiter)
gods.addEdge(neptune, "brother", pluto)

gods.addEdge(hercules, "father", jupiter)
gods.addEdge(hercules, "mother", alcmene)
gods.addEdge(hercules, "battled", nemean, json"""{"time": 1, "place": {"latitude": 38.1, "longitude": 23.7}}""")
gods.addEdge(hercules, "battled", hydra, json"""{"time": 2, "place": {"latitude": 37.7, "longitude": 23.9}}""")
gods.addEdge(hercules, "battled", cerberus, json"""{"time": 12, "place": {"latitude": 39.0, "longitude": 22.0}}""")

gods.addEdge(pluto, "brother", jupiter)
gods.addEdge(pluto, "brother", neptune)
gods.addEdge(pluto, "lives", tartarus, json"""{"reason": "no fear of death"}""")
gods.addEdge(pluto, "pet", cerberus)

gods.addEdge(cerberus, "lives", tartarus)
