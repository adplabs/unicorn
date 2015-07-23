/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.graph.document

import com.adp.unicorn._, json._
import com.adp.unicorn.graph.Edge
import com.adp.unicorn.graph.Visitor

/**
 * Abstract document graph visitor.
 * 
 * @author Haifeng Li (293050)
 */
abstract class AbstractDocumentVisitor(maxHops: Int, relationships: Seq[String]) extends Visitor[Document, (String, JsValue)] {
  
  override def edges(node: Document, hops: Int): Iterator[Edge[Document, (String, JsValue)]] = {
    var links = List[Edge[Document, (String, JsValue)]]()
      
    if (hops < maxHops) {
      node.neighbors(relationships: _*).foreach { case (doc, edge) =>
        val link = new Edge[Document, (String, JsValue)](node, doc, Some((edge._1, edge._2)))
        links = link :: links
      }
    }
    
    links.iterator
  }
}
