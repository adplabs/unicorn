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

import java.nio.ByteBuffer
import unicorn.bigtable.Column
import unicorn.json._
import unicorn.util._

/** Graph serializer. By default, edge label size is up to 256,
  * vertex property size is up to 64KB, overall data size of each edge is up to 10MB.
  *
  * @author Haifeng Li
  */
class GraphSerializer(
  val buffer: ByteBuffer = ByteBuffer.allocate(265),
  val vertexSerializer: ColumnarJsonSerializer = new ColumnarJsonSerializer(ByteBuffer.allocate(65536)),
  val edgeSerializer: BsonSerializer = new BsonSerializer(ByteBuffer.allocate(10485760))) {

  /** Serializes vertex id. */
  def serialize(id: Long): Array[Byte] = {
    buffer.clear
    buffer.putLong(id)
    buffer
  }

  /** Serializes the document vertex lookup table row key. */
  def serialize(table: String, tenant: JsValue, key: JsValue): Array[Byte] = {
    buffer.clear
    edgeSerializer.serialize(buffer, table)
    edgeSerializer.serialize(buffer, tenant)
    edgeSerializer.serialize(buffer, key)
    buffer
  }

  /** Serializes vertex property data. */
  def serializeVertex(json: JsObject): Seq[Column] = {
    vertexSerializer.serialize(json).map { case (path, value) =>
      Column(vertexSerializer.str2Bytes(path), value)
    }.toSeq
  }

  /** Deserializes vertex property data. */
  def deserializeVertex(columns: Seq[Column]): JsObject = {
    val map = columns.map { case Column(qualifier, value, _) =>
      (new String(qualifier, vertexSerializer.charset), value.bytes)
    }.toMap
    vertexSerializer.deserialize(map).asInstanceOf[JsObject]
  }

  /** Serializes an edge column qualifier. */
  def serializeEdgeColumnQualifier(label: Array[Byte], vertex: Long): Array[Byte] = {
    buffer.clear
    buffer.put(label)
    buffer.put(0.toByte)
    buffer.putLong(vertex)
    buffer
  }

  /** Deserializes an edge column qualifier. */
  def deserializeEdgeColumnQualifier(bytes: Array[Byte]): (String, Long) = {
    val buffer = ByteBuffer.wrap(bytes)
    val label = edgeSerializer.cstring(buffer)
    val vertex = buffer.getLong
    (label, vertex)
  }

  /** Serializes edge property data. */
  def serializeEdge(json: JsValue): Array[Byte] = {
    edgeSerializer.clear
    edgeSerializer.put(json)
    edgeSerializer.toBytes
  }

  /** Deserializes edge property data. */
  def deserializeEdge(bytes: Array[Byte]): JsValue = {
    edgeSerializer.deserialize(bytes)
  }
}
