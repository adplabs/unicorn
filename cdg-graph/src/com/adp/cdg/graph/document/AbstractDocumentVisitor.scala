package com.adp.cdg.graph.document

import com.adp.cdg.Document
import com.adp.cdg.JsonValue
import com.adp.cdg.graph.GraphOps
import com.adp.cdg.graph.Edge
import com.adp.cdg.graph.Visitor

abstract class AbstractDocumentVisitor(maxHops: Int, relationships: Seq[String]) extends Visitor[Document, (String, JsonValue)] {
  
  override def edges(node: Document, hops: Int): Iterator[Edge[Document, (String, JsonValue)]] = {
    var links = List[Edge[Document, (String, JsonValue)]]()
      
    if (hops < maxHops) {
      node.neighbors(relationships: _*).foreach { case (doc, edge) =>
        val link = new Edge[Document, (String, JsonValue)](node, doc, Some((edge._1, edge._2)))
        links = link :: links
      }
    }
    
    links.iterator
  }
}
