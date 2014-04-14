class SimpleDocumentVisitor(maxHops: Int, relationships: String*) extends AbstractDocumentVisitor(maxHops, relationships) {
  def visit(doc: Document, edge: Option[(String, JsonValue)], hops: Int) {
    doc.refresh
    println(doc)
  }
}

val server = AccumuloServer("poc", "cdldvtitavap015:2181,cdldvtitavap016:2181,cdldvtitavap017:2181", "tester", "adpadp")
val table = server.dataset("astroph", "public")

val graph = new GraphOps[Document, (String, JsonValue)]()
val doc = "10000" of table
val visitor = new SimpleDocumentVisitor(3, "works with")
graph.dfs(doc, visitor)
graph.bfs(doc, visitor)
