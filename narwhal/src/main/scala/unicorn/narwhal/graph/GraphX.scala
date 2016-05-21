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

package unicorn.narwhal.graph

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext

import unicorn.bigtable.hbase.HBaseTable
import unicorn.json._
import unicorn.unibase.graph.{ReadOnlyGraph, GraphSerializer}

/** Unibase graph specialized for HBase with Spark GraphX supports. */
class GraphX(override val table: HBaseTable) extends ReadOnlyGraph(table) {
  /** Returns a Spark GraphX object.
    *
    * @param sc Spark context object.
    * @return a Spark GraphX Graph.
    */
  def graphx(sc: SparkContext): org.apache.spark.graphx.Graph[JsObject, (String, JsValue)] = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, name)
    conf.setInt(TableInputFormat.SCAN_CACHEDROWS, 500)
    conf.setBoolean(TableInputFormat.SCAN_CACHEBLOCKS, false)

    val rdd = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val rows = rdd.mapPartitions { it =>
      val serializer = new GraphSerializer()
      it.map { tuple =>
        val result = tuple._2
        val row = HBaseTable.getRow(result)
        val id = row.key
        serializer.deserializeVertex(row)
      }
    }

    val vertices = rows.map { vertex =>
      (vertex.id, vertex.properties)
    }

    val edges = rows.flatMap { vertex =>
      vertex.out.valuesIterator.flatMap { edges =>
        edges.map { edge =>
          org.apache.spark.graphx.Edge(edge.source, edge.target, (edge.label, edge.properties))
        }
      }
    }

    org.apache.spark.graphx.Graph(vertices, edges)
  }
}
