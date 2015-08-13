import unicorn._
import unicorn.json._
import unicorn.store._

// measure running time of a function/block 
def time[A](f: => A) = {
  val s = System.nanoTime
  val ret = f
  println("time: " + (System.nanoTime - s)/1e6 + " ms")
  ret
}

// connect to Accumulo server
val server = AccumuloServer("local-poc", "127.0.0.1:2181", "tester", "adpadp")
// Use table "small" 
val table = server.dataset("small", "public", "public")

// HBase
val server = HBaseServer()
val table = server.dataset("small")

// Cassandra
val server = CassandraServer("127.0.0.1", 9160)
val table = server.dataset("small")

// Read a non-existing row. It is the pure time of round trip.
val doc = time { table get "row1" }

// Create a JsObject
val person = JsObject(
  "name" -> "Haifeng",
  "gender" -> "Male",
  "salary" -> 1.0,
  "zip" -> 10011
)
// Create a document 
val person = Document("293050")
person("name") = "Haifeng"
person("gender") = "Male"
person("salary") = 1.0
person("zip") = 10011

// Create another document 
val address = Document("293050")
address.street = "135 W. 18th ST"
address.city = "New York"
address.state = "NY"
address.zip = person.zip

// add a doucment into another one
person.address = address
// add an array into a document
person.projects = Array("GHCM", "Analytics")

person("work with", "Jim") = true
person("work with", "Mike") = true
person("report to", "Jerome") = true

person.relationships("Jim")
person.neighbors("work with")
person.neighbors("work with", "report to")
person("report to", "Jim")
person("report to", "Jerome")

// save document into a dataset
time { person into table }

// save it again. should be in no time.
time { person into table }

// Read back the document
val haifeng = time { table get "293050" }

// Read partially a document
val partial = time { Document("293050").from(table).select("name", "zip") }

// Remove a field
partial remove "zip"
partial commit

// Let's check if "zip" was deleted
val onlyname = time { Document("293050").from(table).select("name", "zip") }

// Turn on the cache
table cacheOn

val once = time { table get "293050" }
haifeng.name = "Haifeng Li"
haifeng.gender = null
haifeng commit

val twice = time { table get "293050" }

// wiki
val server = CassandraServer("127.0.0.1", 9160)
val wiki = cluster.dataset("wiki", "public")
wiki get "81859"

// Google+
val gplus = server.dataset("gplus", "public")
gplus cacheOn
val dan = gplus get "111065108889012087599"

//val graph = DocumentGraph(dan, 2, "follows")

class SimpleDocumentVisitor(maxHops: Int, relationships: String*) extends AbstractDocumentVisitor(maxHops, relationships) {
  val graph = new GraphOps[Document, (String, JsonValue)]()
  var doc: Document = null

  def bfs(doc: Document) {
    this.doc = doc
    graph.bfs(doc, this)
  }

  def dfs(doc: Document) {
    this.doc = doc
    graph.dfs(doc, this)
  }

  def visit(node: Document, edge: Edge[Document, (String, JsonValue)], hops: Int) {
    node.loadRelationships
    if (hops > 0) println(doc.id + "--" + hops + "-->" + node.id)
  }
}

val visitor = new SimpleDocumentVisitor(3, "follows")
visitor.dfs(dan)
visitor.bfs(dan)

val astroph = server.dataset("astroph", "public")
astroph cacheOn
val author = astroph get "63225"
val visitor = new SimpleDocumentVisitor(2, "works with")
visitor.dfs(author)
visitor.bfs(author)
val graph = DocumentGraph(author, 2, "works with")
graph.topologicalSort
graph.dijkstra


// Make a small org chart for A* search
val server = CassandraServer("127.0.0.1", 9160)
val table = server.dataset("small")

val haifeng = Document("Haifeng")
haifeng.rank = 1
haifeng("works with", "Roberto") = true
haifeng("reports to", "Jerome") = true

val roberto = Document("Roberto")
roberto.rank = 3
roberto("works with", "Keith") = true
roberto("reports to", "Mike") = true

val jerome = Document("Jerome")
jerome.rank = 2
jerome("works with", "Roberto") = true
jerome("reports to", "Mike") = true

val keith = Document("Keith")
keith.rank = 3
keith("works with", "Roberto") = true
keith("reports to", "Mike") = true

val mike = Document("Mike")
mike.rank = 4
mike("works with", "Jim") = true
mike("reports to", "Jerome") = true

haifeng into table
roberto into table
jerome  into table
keith   into table
mike    into table

val graphOps = new GraphOps[Document, (String, JsonValue)]()
val path = graphOps.dijkstra(haifeng, mike,
  (doc: Document) => {
    val neighbors = doc.neighbors("works with", "reports to")
    neighbors.foreach { case (doc, _) => doc.loadRelationships }
    neighbors.iterator
  }
)

path.map {
  case (doc, Some(edge)) => edge._1 + " --> " + doc.id
  case (doc, None) => doc.id
}.mkString(" -- ")

val path = graphOps.astar(haifeng, mike,
  (doc: Document) => {
    val neighbors = doc.neighbors("works with", "reports to")
    neighbors.foreach { case (doc, _) => doc.loadRelationships }
    neighbors.iterator
  },
  (a: Document, b: Document) => (a.rank, b.rank) match {
    case (ar: JsonIntValue, br: JsonIntValue) => math.abs(ar.value - br.value)
    case _ => 100
  },
  (a: Document, b: Document, e: (String, JsonValue)) => e._1 match {
    case "works with" => 1.
    case "reports to" => 2.
    case _ => 3.
  }
)

path.map {
  case (doc, Some(edge)) => edge._1 + " --> " + doc.id
  case (doc, None) => doc.id
}.mkString(" -- ")

val path = graphOps.astar(haifeng, mike,
  (doc: Document) => {
    val neighbors = doc.neighbors("reports to")
    neighbors.foreach { case (doc, _) => doc.loadRelationships }
    neighbors.iterator
  },
  (a: Document, b: Document) => (a.rank, b.rank) match {
    case (ar: JsonIntValue, br: JsonIntValue) => math.abs(ar.value - br.value)
    case _ => 100
  }
)

path.map {
  case (doc, Some(edge)) => edge._1 + " --> " + doc.id
  case (doc, None) => doc.id
}.mkString(" -- ")

val path = graphOps.astar(haifeng, roberto,
  (doc: Document) => {
    val neighbors = doc.neighbors("reports to")
    neighbors.foreach { case (doc, _) => doc.loadRelationships }
    neighbors.iterator
  },
  (a: Document, b: Document) => (a.rank, b.rank) match {
    case (ar: JsonIntValue, br: JsonIntValue) => math.abs(ar.value - br.value)
    case _ => 100
  }
)

path.map {
  case (doc, Some(edge)) => edge._1 + " --> " + doc.id
  case (doc, None) => doc.id
}.mkString(" -- ")

// full text search
val server = CassandraServer("127.0.0.1", 9160)
val wiki = server.dataset("wiki")
val index = TextSearch(wiki)
val news = index.search("football")
news.foreach { case ((doc, field), score) =>
  doc.select("title")
  println(doc.id + " = " + score)
  println(doc("title"))
}

// Prefix tree example
val trie = new smile.nlp.Trie[Character, String]
news.foreach { case ((doc, field), score) =>
  doc.select("title")
  val title: String = doc.title
  trie.put(title.toCharArray, title)
}
