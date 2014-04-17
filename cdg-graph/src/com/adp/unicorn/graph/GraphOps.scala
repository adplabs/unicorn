/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.graph

/**
 * Graph operations on graphs of vertex type V and edge type E.
 * 
 * @author Haifeng Li (293050)
 */
class GraphOps[V, E] {
  
    /**
     * Depth-first search of graph.
     * @param node the current node to visit
     * @param edge optional incoming edge
     * @param visitor a visitor object to process the current node and also to return
     * an iterator of edges of interest associated with the node. Note that the visitor
     * may not return all edges in the graph. For example, we may be only interested in
     * "work with" relationships and would like to filter out "reports to" relationships.
     * @param mark a set of visited nodes.
     * @param hops the number of hops to reach this node from the starting node.
     */
    private def dfs(node: V, edge: Edge[V, E], visitor: Visitor[V, E], mark: collection.mutable.Set[V], hops: Int) {
      visitor.visit(node, edge, hops)
      mark add node
      visitor.edges(node, hops).foreach { e =>
        if (!mark.contains(e.target))
          dfs(e.target, e, visitor, mark, hops + 1)
      }
    }

    /**
     * Depth-first search of graph.
     * @param node the starting node
     * @param visitor a visitor object to process the current node and also to return
     * an iterator of edges of interest associated with the node. Note that the visitor
     * may not return all edges in the graph. For example, we may be only interested in
     * "work with" relationships and would like to filter out "reports to" relationships.
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
      val queue = collection.mutable.Queue[(Edge[V, E], Int)]()
      
      visitor.visit(node, null, 0)
      mark add node
      visitor.edges(node, 0).foreach { edge =>
        if (!mark.contains(edge.target))
          queue += ((edge, 1))
      }
      
      while (!queue.isEmpty) {
        val (edge, hops) = queue.dequeue
        visitor.visit(edge.target, edge, hops)
        mark add node
        visitor.edges(node, hops).foreach { edge =>
          if (!mark.contains(edge.target))
            queue += ((edge, hops + 1))
        }
      }
    }
    
    /**
     * Helper ordering object in A* for priority queue
     */
    private object NodeOrdering extends scala.math.Ordering[(V, Double)] {
      def compare(x: (V, Double), y: (V, Double)): Int = {
        x._2.compare(y._2)
      }
    }

    /**
     * A* search algorithm for path finding and graph traversal.
     * It is an extension of Dijkstra algorithm and achieves better performance by using heuristics.
     * 
     * @param start  the start node
     * @param goal   the goal node
     * @param g      the past path-cost function, which is the known distance
     *               from the starting node to the current node.
     * @param h      the future path-cost function, which is an admissible
     *               "heuristic estimate" of the distance from the current node to the goal.
     *               Note that the heuristic function must be monotonic.
     * @return       the path from source to goal
     */
    def astar(start: V, goal: V, g: (V, V, E) => Double, h: (V, V) => Double, neighbors: V => Iterator[(V, E)]): List[(V, Option[E])] = {
      // The queue to find node with lowest f score
      // Note that Scala priority queue maintains largest value on the top.
      // So we will use negative f score in the queue.
      val openQueue = new scala.collection.mutable.PriorityQueue[(V, Double)]()(NodeOrdering)
      openQueue.enqueue((start, -h(start, goal)))
      
      // The set of tentative nodes to be evaluated.
      val openSet = scala.collection.mutable.Set[V](start)
      
      // The set of nodes already evaluated.
      val closedSet = scala.collection.mutable.Set[V]()

      // The map of navigated nodes
      val cameFrom = scala.collection.mutable.Map[V, (V, E)]()

      // Cost from start along best known path.
      val gScore = scala.collection.mutable.Map[V, Double]()
      gScore(start) = 0.0
        
      // Estimated total cost from start to goal through y.
      val fScore = scala.collection.mutable.Map[V, Double]()
      fScore(start) = h(start, goal)

      while (!openQueue.isEmpty) {
        val (current, _) = openQueue.dequeue
        
        if (current == goal) return reconstructPath(cameFrom, goal).reverse

        openSet.remove(current)
        closedSet.add(current)

        neighbors(current).foreach {
          case (neighbor, _) if (closedSet.contains(neighbor)) => ()
          case (neighbor, edge) =>
            val gScoreUpdated = gScore(current) + g(current, neighbor, edge)
 
            if (!openSet.contains(neighbor) || gScoreUpdated < gScore(neighbor)) { 
              cameFrom(neighbor) = (current, edge)
              gScore(neighbor) = gScoreUpdated
              val f = -gScore(neighbor) - h(neighbor, goal)
              fScore(neighbor) = f
              if (!openSet.contains(neighbor)) {
                openSet.add(neighbor)
                openQueue.enqueue((neighbor, f))
              }
            }
        }
      }

      // Fail. No path exists between the start node and the goal.
      return List[(V, Option[E])]()
    }
    
    /**
     * Reconstructs the A* search path.
     */
    private def reconstructPath(cameFrom: scala.collection.mutable.Map[V, (V, E)], current: V): List[(V, Option[E])] = {
      if (cameFrom.contains(current)) {
        val (from, edge) = cameFrom(current)
        val path = reconstructPath(cameFrom, from)
        return (current, Some(edge)) :: path
      } else {
        return List[(V, Option[E])]((current, None))
      }
    }
 }