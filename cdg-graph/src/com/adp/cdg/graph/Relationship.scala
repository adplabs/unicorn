package com.adp.cdg.graph

/**
 * Relationship represents an edge between two nodes/vertices in a graph with a string label and
 * any data of type E associated with the edge.
 */
class Relationship[V, E](val source: V, val target: V, val label: String, val data: E) {

}
