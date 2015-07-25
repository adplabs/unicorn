/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.graph

/**
 * An edge between two vertices in a graph with optional associated data.
 * 
 * @author Haifeng Li (293050)
 */
class Edge[V, E](val source: V, val target: V, val data: Option[E]) {

}
