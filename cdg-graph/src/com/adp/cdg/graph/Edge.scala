package com.adp.cdg.graph

/**
 * An edge between two nodes/vertices in a graph
 * with optional data of type E associated with the edge.
 */
class Edge[V, E](val source: V, val target: V, val data: Option[E]) {

}
