/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.graph

/**
 * Graph traversal visitor.
 * 
 * @author Haifeng Li (293050)
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