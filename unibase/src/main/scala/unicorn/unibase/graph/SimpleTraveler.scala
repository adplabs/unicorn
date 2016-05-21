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

package unicorn.unibase.graph

import VertexColor._

/** Simple graph visitor with cache management.
  * In DFS and BFS, the user should create a sub class overriding
  * the `apply` method, which is nop by default.
  *
  * @param graph The graph to visit.
  * @param relationships Relationship of interest. Only neighbors with given
  *                      relationship will be visited. Empty set means all
  *                      relationships.
  * @param maxHops Maximum number of hops during graph traversal.
  * @param outgoing If true, traverse the graph with outgoing edges
  *                 at each vertex. Otherwise, follow the incoming edges.
  *
  * @author Haifeng Li
  */
class SimpleTraveler(val graph: ReadOnlyGraph, val relationships: Set[String] = Set.empty, val maxHops: Int = 3, val outgoing: Boolean = true) extends Traveler {
  /** The color mark if a vertex was already visited. */
  private val mark = collection.mutable.Map[Long, VertexColor]().withDefaultValue(White)

  /** The cache of vertices. */
  private val cache = collection.mutable.Map[Long, Vertex]()

  /** User defined vertex visit function. The default implementation is nop.
    * The user should create a sub class overriding this method.
    *
    * @param vertex the vertex on visiting.
    * @param edge the incoming arc (None for starting vertex).
    * @param hops the number of hops from the starting vertex to this vertex.
    */
  def apply(vertex: Vertex, edge: Option[Edge], hops: Int): Unit = {

  }

  /** Resets the vertex color to unvisited and clean up the cache. */
  def reset: Unit = {
    mark.clear
    cache.clear
  }

  override def v(vertex: Long): Vertex = {
    cache.get(vertex) match {
      case Some(node) => node
      case None =>
        val node = graph(vertex)
        cache(vertex) = node
        node
    }
  }

  override def color(vertex: Long): VertexColor = mark(vertex)

  override def visit(vertex: Vertex, edge: Option[Edge], hops: Int): Unit = {
    apply(vertex, edge, hops)

    val black = (if (outgoing) vertex.out else vertex.in).forall { case (_, edges) =>
      edges.forall { edge =>
        if (outgoing)
          mark.contains(edge.target)
        else
          mark.contains(edge.source)
      }
    }

    mark(vertex.id) = if (black) Black else Gray
  }

  override def neighbors(vertex: Vertex, hops: Int): Iterator[(Long, Edge)] = {
    if (hops >= maxHops) return Seq.empty.iterator

    vertex.edges.filter { edge =>
      if (outgoing && edge.target == vertex.id) false
      else if (!outgoing && edge.source == vertex.id) false
      else if (relationships.isEmpty) true
      else relationships.contains(edge.label)
    }.map { edge =>
      val neighbor = if (outgoing) edge.target else edge.source
      (neighbor, edge)
    }.iterator
  }
}
