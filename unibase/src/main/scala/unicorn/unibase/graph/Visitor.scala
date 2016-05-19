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

/** Graph traversal visitor.
  *
  * @author Haifeng Li
  */
trait Visitor {
  /** Visit a vertex during graph traversal.
    *
    * @param vertex the vertex on visiting.
    * @param edge the incoming arc (null for starting vertex).
    * @param hops the number of hops from the starting vertex to this vertex.
    */
  def visit(vertex: Vertex, edge: Edge, hops: Int): Unit

  /** Returns an iterator of the outgoing edges of a vertex.
    *
    * @param vertex the vertex on visiting.
    * @param hops the number of hops from starting vertex, which may be used for early termination.
    * @return an iterator of the outgoing edges
    */
  def edges(vertex: Vertex, hops: Int): Iterator[Edge]
}
