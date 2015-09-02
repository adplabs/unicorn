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

/**
 * Graph traversal visitor.
 * 
 * @author Haifeng Li
 */
trait Visitor[V, E] {
  /**
   * Visit a vertex during graph traversal. The edge is the incoming
   * arc (null for starting vertex). The hops is the number of hops
   * from the starting vertex to this vertex.
   */
  def visit(vertex: V, edge: Edge[V, E], hops: Int): Unit
  /**
   * Returns an iterator of the outgoing edges of a vertex. The input parameters
   * hops (# of hops from starting vertex) may be used for early termination.
   */
  def edges(vertex: V, hops: Int): Iterator[Edge[V, E]]
}
