package com.adp.cdg.graph.document

import com.adp.cdg.Document
import com.adp.cdg.JsonValue
import com.adp.cdg.graph.Relationship
import com.adp.cdg.graph.Visitor

abstract class AbstractDocumentVisitor(maxHops: Int, relationships: Seq[String]) extends Visitor[Document, JsonValue] {
  
  override def edges(node: Document, hops: Int): Iterator[Relationship[Document, JsonValue]] = {
    var neighbors = List[Relationship[Document, JsonValue]]()
      
    if (hops < maxHops) {
      node.neighbors(relationships: _*).foreach { case (doc, edge) =>
        neighbors = (new Relationship[Document, JsonValue](node, doc, edge._1, edge._2)) :: neighbors
      }
    }
    
    neighbors.iterator
  }
}