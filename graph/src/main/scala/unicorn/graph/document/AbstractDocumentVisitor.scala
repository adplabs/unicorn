/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn.graph.document

import unicorn._, json._
import unicorn.doc.Document
import unicorn.graph.Edge
import unicorn.graph.Visitor

/**
 * Abstract document graph visitor.
 * 
 * @author Haifeng Li
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
