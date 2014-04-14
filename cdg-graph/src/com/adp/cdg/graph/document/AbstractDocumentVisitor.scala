package com.adp.cdg.graph.document

import com.adp.cdg.Document
import com.adp.cdg.JsonValue
import com.adp.cdg.graph.GraphOps
import com.adp.cdg.graph.Relationship
import com.adp.cdg.graph.Visitor

abstract class AbstractDocumentVisitor(maxHops: Int, relationships: Seq[String]) extends Visitor[Document, (String, JsonValue)] {
  
  override def edges(node: Document, hops: Int): Iterator[Relationship[Document, (String, JsonValue)]] = {
    var links = List[Relationship[Document, (String, JsonValue)]]()
      
    if (hops < maxHops) {
      node.neighbors(relationships: _*).foreach { case (doc, edge) =>
        val link = new Relationship[Document, (String, JsonValue)](node, doc, Some(edge._1, edge._2))
        links = link :: links
      }
    }
    
    links.iterator
  }
}
