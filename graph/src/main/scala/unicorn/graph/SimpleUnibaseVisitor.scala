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

import unicorn.json._
import unicorn.unibase.{Table, Unibase}, Unibase.{$id, $graph}
import unicorn.util.Logging

/** Simple Unibase graph visitor. The default implementation of edges returns
  * an adjacency list by checking $._graph in the document.  The field $._graph
  * is an object (usually stored in a separate column family)
  * of which fields are relationships and values are target node IDs.
  * Each target node can have any data associate with.
  * For example,
  *
  * {{{
  *   {
  *     "_graph": {
  *       "work with": {
  *         "Tom": {  // The field name is the string representation of _id.
  *           "_id": "Tom",
  *           "data": {
  *             "project": "Call of Duty",
  *             "since": 2015
  *           }
  *         }
  *       },
  *       "report to": {
  *         "Bob": {
  *           "_id": "Bob",
  *           "data": {
  *             "since": 2010
  *           }
  *         }
  *       }
  *     }
  *   }
  * }}}
  *
  * @author Haifeng Li
  */
class SimpleUnibaseVisitor(table: Table, maxHops: Int = 3) extends Visitor[JsValue, (String, JsValue)] with Logging {
  /** Cache of graph nodes in the database. */
  val cache = collection.mutable.Map[JsValue, JsObject]()

  /** Relationship of interest. Only neighbors with given relationship will be visited. */
  var relationships: Option[Set[String]] = None

  /** Map node to an index. */
  val nodes = scala.collection.mutable.Map[JsValue, Int]()

  /** Map edge to a weight. */
  val weights = scala.collection.mutable.Map[(Int, Int), Double]()

  type VisitHook = (JsValue, Edge[JsValue, (String, JsValue)], Int) => Unit

  /** Extra action when visiting a vertex. This default implementation does nothing.
    * Applications needing customized visit action should override it. */
  val visitHooks = collection.mutable.ArrayBuffer[VisitHook]()

  def addVisitHook(hook: VisitHook): Unit = {
    visitHooks += hook
  }

  override def visit(vertex: JsValue, edge: Edge[JsValue, (String, JsValue)], hops: Int): Unit = {
    if (!nodes.contains(vertex)) nodes(vertex) = nodes.size
    
    if (edge != null) {
      val weight = edge.data match {
        case Some((_, data: JsInt)) => data.value
        case Some((_, data: JsLong)) => data.value
        case Some((_, data: JsDouble)) => data.value
        case _ => 1.0
      }
      
      weights((nodes(edge.source), nodes(edge.target))) = weight
    }

    visitHooks.foreach { hook =>
      hook(vertex, edge, hops)
    }
  }

  override def edges(vertex: JsValue, hops: Int): Iterator[Edge[JsValue, (String, JsValue)]] = {
    if (hops >= maxHops) return Seq.empty.iterator

    val doc = get(vertex)
    if (doc.isEmpty) {
      log.info(s"Document $vertex doesn't exist in table ${table.name}")
      return Seq.empty.iterator
    }

    val neighbors = relationships match {
      case None => links(doc.get)
      case Some(relationships) => links(doc.get).filter { link =>
        relationships.contains(link._1)
      }
    }

    neighbors.flatMap { case (relationship, neighbor) =>
      neighbor.asInstanceOf[JsObject].fields.map { case (_, link) =>
        new Edge(vertex, link($id), Some((relationship, link($data))))
      }
    }.iterator
  }

  /** Returns the document of given id. */
  private def get(id: JsValue): Option[JsObject] = {
    cache.get(id) match {
      case node: Some[JsObject] => node
      case None =>
        $doc(id) match {
          case None => None
          case doc: Some[JsObject] => cache(id) = doc.get; doc
        }
    }
  }

  /** Gets a node/document from the database. By default, we retrieve only the
    * $.graph for adjacency list.
    * This may be not sufficient and applications should override this
    * to retrieve the needed data.
    * @param id document id.
    * @return the document or None if it doesn't exist.
    */
  def $doc(id: JsValue): Option[JsObject] = {
    table(id, $graph)
  }

  /** Returns the adjacency list of a document. By default, we assume that
    * each document has a field "graph" of adjacency list.
    *
    * @param doc
    * @return
    */
  def links(doc: JsObject): Seq[(String, JsValue)] = {
    doc($graph) match {
      case JsObject(fields) => fields.toSeq
      case _ => Seq.empty
    }
  }
}
