package com.adp.cdg.graph

/**
 * An edge between two nodes in a graph with optional associated data.
 */
class Edge[V, E](val source: V, val target: V, val data: Option[E]) {

}
