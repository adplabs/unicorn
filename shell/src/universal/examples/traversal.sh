#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

// Google+
val hbase = Unibase(HBase())
val gplus = hbase("gplus")

def trace(vertex: JsValue, edge: Edge[JsValue, (String, JsValue)], hops: Int) {
  if (hops > 0) println(s"111065108889012087599 --$hops--> $vertex")
}

val dan = JsString("111065108889012087599")

val visitor = new UnibaseVisitor(gplus, 2)
visitor.relationships = Some(Set("follows"))
visitor.addVisitHook(trace)

val danCircle = Unigraph(dan, visitor)
danCircle.topologicalSort
danCircle.dijkstra


// Make a small org chart for A* search
hbase.createTable("astar")
val astar = hbase("astar")

val haifeng = JsObject(
  $id -> "Haifeng",
  "rank" -> 1
)
graph(haifeng)("works with", "Jerome") = 1
graph(haifeng)("reports to", "Roberto") = 2

val roberto = JsObject(
  $id -> "Roberto",
  "rank" -> 3
)
graph(roberto)("works with", "Keith") = 1
graph(roberto)("reports to", "Stuart") = 2

val jerome = JsObject(
  $id -> "Jerome",
  "rank" -> 2
)
graph(jerome)("works with", "Roberto") = 1
graph(jerome)("reports to", "Don") = 2

val keith = JsObject(
  $id -> "Keith",
  "rank" -> 2
)
graph(keith)("works with", "Roberto") = 1
graph(keith)("works with", "Amit") = 1
graph(keith)("reports to", "Stuart") = 2

val amit = JsObject(
  $id -> "Amit",
  "rank" -> 3
)
graph(amit)("works with", "Roberto") = 1
graph(amit)("works with", "Keith") = 1
graph(amit)("reports to", "Stuart") = 2

val stuart = JsObject(
  $id -> "Stuart",
  "rank" -> 4
)
graph(stuart)("works with", "Don") = true
graph(stuart)("reports to", "Carlos") = true

val don = JsObject(
  $id -> "Don",
  "rank" -> 4
)
graph(don)("works with", "Stuart") = true
graph(don)("reports to", "Carlos") = true

val carlos = JsObject(
  $id -> "Carlos",
  "rank" -> 5
)

astar.insert(haifeng)
astar.insert(roberto)
astar.insert(jerome)
astar.insert(keith)
astar.insert(amit)
astar.insert(stuart)
astar.insert(don)
astar.insert(carlos)

val graphOps = new GraphOps[JsObject, (String, JsValue)]()
val path = graphOps.dijkstra(haifeng, carlos,
 (doc: JsObject) => {
    doc($graph).asInstanceOf[JsObject].fields.toSeq.flatMap { case (relationship, links) =>
      links.asInstanceOf[JsObject].fields.toSeq.map { case (_, link) =>
        val vertex = astar(link($id)).get
        val edge = (relationship, link($data))
        (vertex, edge)
      }
    }.iterator
  }
)

path.map {
  case (doc, Some(edge)) => edge._1 + " --> " + doc($id)
  case (doc, None) => doc($id)
}.mkString(" -- ")

val shortPath = graphOps.astar(haifeng, carlos,
  (doc: JsObject) => {
    doc($graph).asInstanceOf[JsObject].fields.toSeq.flatMap { case (relationship, links) =>
      links.asInstanceOf[JsObject].fields.toSeq.map { case (_, link) =>
        val vertex = astar(link($id)).get
        val edge = (relationship, link($data))
        (vertex, edge)
      }
    }.iterator
  },
  (a: JsObject, b: JsObject) => (a.rank, b.rank) match {
    case (ar: JsInt, br: JsInt) => Math.abs(ar.value - br.value)
    case _ => 100
  },
  (a: JsObject, b: JsObject, e: (String, JsValue)) => e._2 match {
    case JsInt(weight) => weight.toDouble
    case _ => 3.0
  }
)

shortPath.map {
  case (doc, Some(edge)) => edge._1 + " --> " + doc($id)
  case (doc, None) => doc($id)
}.mkString(" -- ")


hbase.dropTable("astar")