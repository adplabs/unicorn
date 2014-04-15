package com.adp.cdg.graph.document

import com.adp.cdg._
import com.adp.cdg.graph._
import smile.graph._

class DocumentGraph(val nodes: Array[Document], graph: Graph) {
  def topologicalSort: Array[Document] = {
    val order = graph.sortdfs
    val docs = new Array[Document](nodes.length)
    for (i <- 0 until nodes.length) docs(i) = nodes(order(i))
    docs
  }
  
  def dijkstra = graph.dijkstra
}

object DocumentGraph {
  val graphOps = new GraphOps[Document, (String, JsonValue)]()
  
  class Builder(maxHops: Int, relationships: Seq[String]) extends AbstractDocumentVisitor(maxHops, relationships) {
    val nodes = scala.collection.mutable.Map[Document, Int]()
    val edges = scala.collection.mutable.Map[(Int, Int), Double]()

    def bfs(doc: Document) {
      graphOps.bfs(doc, this)
    }

    def dfs(doc: Document) {
      graphOps.dfs(doc, this)
    }

    def visit(node: Document, edge: Edge[Document, (String, JsonValue)], hops: Int) {
      if (hops < maxHops) node.refreshRelationships
      if (!nodes.contains(node)) nodes(node) = nodes.size
      if (edge != null) {
        val weight = edge.data match {
          case Some((label: String, data: JsonIntValue)) => data.value
          case Some((label: String, data: JsonLongValue)) => data.value
          case Some((label: String, data: JsonDoubleValue)) => data.value
          case _ => 1.0
        }
        edges((nodes(edge.source), nodes(edge.target))) = weight
      }
    }
  }

  def apply(doc: Document, maxHops: Int, relationships: String*): DocumentGraph = {
    val builder = new Builder(maxHops, relationships)
    builder.dfs(doc)
    
    val nodes = new Array[Document](builder.nodes.size)
    builder.nodes.foreach { case (doc, index) => nodes(index) = doc }
    
    val graph = new AdjacencyList(nodes.length, true)
    builder.edges.foreach { case (key, weight) => graph.addEdge(key._1, key._2, weight)}
    
    new DocumentGraph(nodes, graph)
  }
}
