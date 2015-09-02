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

package com.adp.unicorn.graph.document

import com.adp.unicorn.Document
import com.adp.unicorn.JsonValue
import com.adp.unicorn.graph.GraphOps
import com.adp.unicorn.graph.Edge
import com.adp.unicorn.graph.Visitor

/**
 * Abstract document graph visitor.
 * 
 * @author Haifeng Li
 */
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
