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

  def visit(node: Document, edge: Option[(String, JsonValue)], hops: Int) {
    node.refresh
    println(doc.id + "--" + hops + "-->" + node.id)
  }
}

val server = AccumuloServer("poc", "cdldvtitavap015:2181,cdldvtitavap016:2181,cdldvtitavap017:2181", "tester", "adpadp")
val table = server.dataset("astroph", "public")

val doc = "10000" of table
val visitor = new SimpleDocumentVisitor(2, "works with")
visitor.dfs(doc)
visitor.bfs(doc)
