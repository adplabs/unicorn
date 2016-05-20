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

import unicorn.json.JsObject

/** Advanced graph operations.
  *
  * @author Haifeng Li
  */
object GraphOps {
  
    /** Depth-first search of graph.
      * @param vertex the current vertex to visit
      * @param edge optional incoming edge
      * @param visitor the visitor object which processes a vertex and emits its neighbors.
      * @param hops the number of hops to reach this vertex from the starting vertex.
      */
    private def dfs(vertex: Long, edge: Option[Edge], visitor: Visitor, hops: Int) {
      val node = visitor.v(vertex)
      visitor.visit(node, edge, hops)
      visitor.neighbors(node, hops).foreach { case (neighbor, edge) =>
        dfs(neighbor, Some(edge), visitor, hops + 1)
      }
    }

    /** Depth-first search of graph.
      * @param vertex the starting vertex
      * @param visitor the visitor object which processes a vertex and emits its neighbors.
      */
    def dfs(vertex: Long, visitor: Visitor) {
      dfs(vertex, None, visitor, 0)
    }

    /** Breadth-first search of graph.
      * @param vertex the current vertex to visit
      * @param visitor the visitor object which processes a vertex and emits its neighbors.
      */
    def bfs(vertex: Long, visitor: Visitor) {
      val queue = collection.mutable.Queue[(Long, Option[Edge], Int)]()

      queue.enqueue((vertex, None, 0))

      while (!queue.isEmpty) {
        val (vertex, edge, hops) = queue.dequeue
        val node = visitor.v(vertex)
        visitor.visit(node, edge, hops)
        visitor.neighbors(node, hops).foreach { case (neighbor, edge) =>
          queue.enqueue((neighbor, Some(edge), hops + 1))
        }
      }
    }
    
    /** Helper ordering object in A* for priority queue. */
    private object NodeOrdering extends scala.math.Ordering[(Long, Double, Int)] {
      def compare(x: (Long, Double, Int), y: (Long, Double, Int)): Int = {
        x._2.compare(y._2)
      }
    }
    
    /** Dijkstra shortest path search algorithm.
      *
      * @param start  the start vertex
      * @param goal   the goal vertex
      * @param visitor the visitor object which emits neighbors and also evaluates the weight of an edge.
      * @return       the path from source to goal
      */
    def dijkstra(start: Long, goal: Long, visitor: Visitor): List[(JsObject, Option[Edge])] = {

      val queue = new scala.collection.mutable.PriorityQueue[(Long, Double, Int)]()(NodeOrdering)
      queue.enqueue((start, 0.0, 0))
      
      val dist = scala.collection.mutable.Map[Long, Double]().withDefaultValue(Double.PositiveInfinity)
      dist(start) = 0.0

      // The map of navigated vertices
      val cameFrom = scala.collection.mutable.Map[Long, (Long, JsObject, Edge)]()

      while (!queue.isEmpty) {
        val (current, distance, hops) = queue.dequeue
        if (current == goal) return reconstructPath(cameFrom, goal, visitor).reverse

        val node = visitor.v(current)
        visitor.neighbors(node, hops).foreach { case (neighbor, edge) =>
          val neighbor = 0
          val alt = distance + visitor.weight(edge)
          if (alt < dist(neighbor)) {
            dist(neighbor) = alt
            cameFrom(neighbor) = (current, node.properties, edge)
            queue.enqueue((neighbor, alt, hops + 1))
          }
        }
      }

      // Fail. No path exists between the start vertex and the goal.
      return List[(JsObject, Option[Edge])]()
    }
    
    /** A* search algorithm for path finding and graph traversal.
      * It is an extension of Dijkstra algorithm and achieves better performance by using heuristics.
      *
      * @param start  the start vertex
      * @param goal   the goal vertex
      * @param visitor the visitor object which emits neighbors and also evaluates the weight of an edge.
      * @param h      the future path-cost function, which is an admissible
      *               "heuristic estimate" of the distance from the current vertex to the goal.
      *               Note that the heuristic function must be monotonic.
      * @return       the path from source to goal
      */
    def astar(start: Long, goal: Long, visitor: Visitor, h: (Vertex, Vertex) => Double): List[(JsObject, Option[Edge])] = {
      val startVertex = visitor.v(start)
      val goalVertex = visitor.v(goal)

      val guess = h(startVertex, goalVertex)

      // The queue to find vertex with lowest f score
      // Note that Scala priority queue maintains largest value on the top.
      // So we will use negative f score in the queue.
      val openQueue = new scala.collection.mutable.PriorityQueue[(Long, Double, Int)]()(NodeOrdering)
      openQueue.enqueue((start, -guess, 0))
      
      // The set of tentative vertices to be evaluated.
      val openSet = scala.collection.mutable.Set[Long](start)
      
      // The set of vertices already evaluated.
      val closedSet = scala.collection.mutable.Set[Long]()

      // The map of navigated vertices
      val cameFrom = scala.collection.mutable.Map[Long, (Long, JsObject, Edge)]()

      // Cost from start along best known path.
      val gScore = scala.collection.mutable.Map[Long, Double]()
      gScore(start) = 0.0
        
      // Estimated total cost from start to goal through y.
      val fScore = scala.collection.mutable.Map[Long, Double]()
      fScore(start) = guess

      while (!openQueue.isEmpty) {
        val (current, _, hops) = openQueue.dequeue
        
        if (current == goal) return reconstructPath(cameFrom, goal, visitor).reverse

        openSet.remove(current)
        closedSet.add(current)

        val node = visitor.v(current)
        visitor.neighbors(node, hops).foreach {
          case (neighbor, _) if (closedSet.contains(neighbor)) => ()
          case (neighbor, edge) =>
            val alt = gScore(current) + visitor.weight(edge)
 
            if (!openSet.contains(neighbor) || alt < gScore(neighbor)) { 
              cameFrom(neighbor) = (current, node.properties, edge)
              gScore(neighbor) = alt
              val f = -gScore(neighbor) - h(visitor.v(neighbor), goalVertex)
              fScore(neighbor) = f
              if (!openSet.contains(neighbor)) {
                openSet.add(neighbor)
                openQueue.enqueue((neighbor, f, hops + 1))
              }
            }
        }
      }

      // Fail. No path exists between the start vertex and the goal.
      return List[(JsObject, Option[Edge])]()
    }
    
    /** Reconstructs the A* search path. */
    private def reconstructPath(cameFrom: scala.collection.mutable.Map[Long, (Long, JsObject, Edge)], current: Long, visitor: Visitor): List[(JsObject, Option[Edge])] = {
      if (cameFrom.contains(current)) {
        val (from, node, edge) = cameFrom(current)
        val path = reconstructPath(cameFrom, from, visitor)
        return (node, Some(edge)) :: path
      } else {
        return List((visitor.v(current).properties, None))
      }
    }
 }
