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

import unicorn.bigtable.{BigTable, Column}
import unicorn.unibase.Unibase
import unicorn.json._
import unicorn.unibase.idgen.LongIdGenerator

/** Graphs are mathematical structures used to model pairwise relations
  * between objects. A graph is made up of vertices (nodes) which are
  * connected by edges (arcs or lines). A graph may be undirected, meaning
  * that there is no distinction between the two vertices associated with
  * each edge, or its edges may be directed from one vertex to another.
  * Directed graphs are also called digraphs and directed edges are also
  * called arcs or arrows.
  *
  * A multigraph is a graph which is permitted to have multiple edges
  * (also called parallel edges), that is, edges that have the same end
  * nodes. The ability to support parallel edges simplifies modeling
  * scenarios where there can be multiple relationships (e.g., co-worker
  * and friend) between the same vertices.
  *
  * In a property graph, the generic mathematical graph is often extended
  * to support user defined objects attached to each vertex and edge.
  * The edges also have associated labels denoting the relationships,
  * which are important in a multigraph.
  *
  * Unicorn supports directed property multigraphs. Documents from different
  * tables can be added as vertices to a multigraph. It is also okay to add
  * vertices without corresponding to documents. Each relationship/edge
  * has a label and optional data (any valid JsValue, default value JsInt(1)).
  *
  * Unicorn stores graphs in adjacency lists. That is, a graph
  * is stored as a BigTable whose rows are vertices with their adjacency list.
  * The adjacency list of a vertex contains all of the vertex’s incident edges
  * (in and out edges are in different column families).
  *
  * Because large graphs are usually very sparse, an adjacency list is
  * significantly more space-efficient than an adjacency matrix.
  * Besides, the neighbors of each vertex may be listed efficiently with
  * an adjacency list, which is important in graph traversals.
  * With our design, it is also possible to
  * test whether two vertices are adjacent to each other
  * for a given relationship in constant time.
  *
  * @param table Graph adjacency list table.
  * @param docVertexTable Document vertex lookup table for translate tuple (document table, tenant, document key) to vertex id.
  * @param idgen 64-bit ID generator for vertex id.
  *
  * @author Haifeng Li
  */
class Graph(table: BigTable, docVertexTable: Option[BigTable], idgen: Option[LongIdGenerator]) {
  import Unibase.$id
  import Unibase.{GraphVertexColumnFamily, GraphInEdgeColumnFamily, GraphOutEdgeColumnFamily}

  /** Document serializer. */
  val serializer = new GraphSerializer()

  /** The column qualifier of \$id field. */
  val idColumnQualifier = serializer.jsonPathBytes($id)

  /** The table name. */
  val name = table.name

  /** Adds a vertex.
    *
    * @param id The unique vertex id. Throws exception if the vertex id exists.
    * @param data Any vertex property data.
    */
  def addVertex(id: Long, data: JsObject): Unit = {
    data($id) = id

    val key = serializer.serialize(id)
    require(table.apply(key, GraphVertexColumnFamily, idColumnQualifier).isEmpty, s"Vertex $id already exists in graph ${table.name}")

    val columns = serializer.serializeVertex(data)

    table.put(key, GraphVertexColumnFamily, columns: _*)
  }

  /** Creates a new vertex with automatic generated ID.
    * ID generator must be set up.
    *
    * @param data Any vertex property data.
    * @return Vertex ID.
    */
  def addVertex(data: JsObject): Long = {
    require(idgen.isDefined, "Vertex ID generator was not setup")

    val id = idgen.get.next
    addVertex(id, data)
    id
  }

  /** Adds a directed edge. If the edge exists, the associated data will be overwritten.
    *
    * @param source source vertex id.
    * @param label relationship label.
    * @param target target vertex id.
    * @param data optional data associated with the edge.
    */
  def addEdge(source: Long, label: String, target: Long, data: JsValue = JsInt(1)): Unit = {
    val sourceKey = serializer.serialize(source)
    require(table.apply(sourceKey, GraphVertexColumnFamily, idColumnQualifier).isDefined, s"Vertex $source doesn't exist in graph ${table.name}")

    val targetKey = serializer.serialize(target)
    require(table.apply(targetKey, GraphVertexColumnFamily, idColumnQualifier).isDefined, s"Vertex $target doesn't exist in graph ${table.name}")

    val columnPrefix = serializer.toBytes(label)
    val value = serializer.serializeEdge(data)

    table.put(sourceKey, GraphOutEdgeColumnFamily, Column(serializer.serialize(columnPrefix, target), value))
    table.put(targetKey, GraphInEdgeColumnFamily, Column(serializer.serialize(columnPrefix, source), value))
  }
}
