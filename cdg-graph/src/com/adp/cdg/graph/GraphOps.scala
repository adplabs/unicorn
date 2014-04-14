package com.adp.cdg.graph

/**
 * Graph operations on graphs of vertex type V and edge type E.
 */
class GraphOps[V, E] {

  /** Mask if vertex has been visited */
  lazy val mark = collection.mutable.Map[V, Boolean]()
  
    /**
     * Depth-first search of graph.
     * @param node the current node to visit
     * @param edge the tuple of incoming edge label and any other data
     * @param visitor a visitor object to process the current node and also to return
     * an iterator of edges of interest associated with the node. Note that the visitor
     * may not return all edges in the graph. For example, we may be only interested in
     * "work with" relationships and would like to filter out "reports to" relationships.
     * @param mark a set of visited nodes.
     * @param hops the number of hops to reach this node from the starting node.
     */
    private def dfs(node: V, edge: (String, E), visitor: Visitor[V, E], mark: collection.mutable.Set[V], hops: Int) {
      visitor.visit(node, edge, hops)
      mark add node
      visitor.edges(node, hops).foreach { edge =>
        if (!mark.contains(edge.target))
          dfs(edge.target, (edge.label, edge.data), visitor, mark, hops + 1)
      }
    }

    /**
     * Depth-first search of graph.
     */
    def dfs(node: V, visitor: Visitor[V, E]) {
      val mark = collection.mutable.Set[V]()
      dfs(node, null, visitor, mark, 0)
    }

    /**
     * Breadth-first search of graph.
     * @param node the current node to visit
     * @param visitor a visitor object to process the current node and also to return
     * an iterator of edges of interest associated with the node. Note that the visitor
     * may not return all edges in the graph. For example, we may be only interested in
     * "work with" relationships and would like to filter out "reports to" relationships.
     */
    def bfs(node: V, visitor: Visitor[V, E]) {
      val mark = collection.mutable.Set[V]()
      val queue = collection.mutable.Queue[(Relationship[V, E], Int)]()
      queue += ((null, 0))
      while (queue.size > 0) {
        val (edge, hops) = queue.front
        visitor.visit(edge.target, (edge.label, edge.data), hops)
        mark add node
        visitor.edges(node, hops).foreach { edge =>
          if (!mark.contains(edge.target))
            queue += ((edge, hops + 1))
        }
      }
    }
 }