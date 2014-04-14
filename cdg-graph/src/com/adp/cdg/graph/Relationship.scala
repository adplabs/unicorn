package com.adp.cdg.graph

/**
 * Relationship represents an edge between two nodes/vertices in a graph
 * with optional data of type E associated with the edge.
 */
class Relationship[V, E](val source: V, val target: V, val data: Option[E]) {

}
