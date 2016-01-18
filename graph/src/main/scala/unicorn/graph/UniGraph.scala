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

package unicorn.graph

import smile.graph._
import unicorn.json._

/** Document graph.
  * 
  * @author Haifeng Li
  */
class UniGraph(val nodes: Array[JsValue], graph: Graph) {
  def topologicalSort: Array[JsValue] = {
    val order = graph.sortdfs
    val docs = new Array[JsValue](nodes.length)
    for (i <- 0 until nodes.length) docs(i) = nodes(order(i))
    docs
  }
  
  def dijkstra = graph.dijkstra
}

object UniGraph {
  val graphOps = new GraphOps[JsValue, (String, JsValue)]()

  /** Returns an in-memory graph built by given visitor from the starting node.
    * @param start the staring node of graph traversal.
    * @param visitor the visitor object.
    * @return an in-memory graph for heavy graph computation.
    */
  def apply(start: JsValue, visitor: UniBaseVisitor): UniGraph = {
    graphOps.dfs(start, visitor)
    
    val nodes = new Array[JsValue](visitor.nodes.size)
    visitor.nodes.foreach { case (doc, index) => nodes(index) = doc }
    
    val graph = new AdjacencyList(nodes.length, true)
    visitor.weights.foreach { case (key, weight) => graph.addEdge(key._1, key._2, weight)}
    
    new UniGraph(nodes, graph)
  }
}
