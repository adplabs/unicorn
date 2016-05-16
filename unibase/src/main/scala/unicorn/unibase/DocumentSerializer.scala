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

package unicorn.unibase

import java.nio.ByteBuffer

import unicorn.bigtable.{Column, ColumnFamily}
import unicorn.json._

/** Document serializer. By default, document key size is up to 64KB, column size is up to 10MB.
  *
  * @author Haifeng Li
  */
class DocumentSerializer(
  val keySerializer: BsonSerializer = new BsonSerializer(ByteBuffer.allocate(65536)),
  val valueSerializer: ColumnarJsonSerializer = new ColumnarJsonSerializer(ByteBuffer.allocate(10485760))) extends SerializerHelper {

  /** Assembles the document from multi-column family data. */
  def deserialize(data: Seq[ColumnFamily]): Option[JsObject] = {
    val objects = data.map { case ColumnFamily(family, columns) =>
      val map = columns.map { case Column(qualifier, value, _) =>
        (new String(qualifier, JsonSerializer.charset), value.bytes)
      }.toMap
      val json = valueSerializer.deserialize(map)
      json.asInstanceOf[JsObject]
    }

    if (objects.size == 0)
      None
    else if (objects.size == 1)
      Some(objects(0))
    else {
      val fold = objects.foldLeft(JsObject()) { (doc, family) =>
        doc.fields ++= family.fields
        doc
      }
      Some(fold)
    }
  }

  /** Serialize document data. */
  def serialize(json: JsObject): Seq[Column] = {
    valueSerializer.serialize(json).map { case (path, value) =>
      Column(toBytes(path), value)
    }.toSeq
  }

  /** Serialize document id. */
  def serialize(tenant: JsValue, id: JsValue): Array[Byte] = {
    keySerializer.clear
    keySerializer.put(tenant)
    keySerializer.put(id)
    keySerializer.toBytes
  }

  /** Deserialize document key. */
  def deserialize(key: Array[Byte]): (JsValue, JsValue) = {
    val buffer = ByteBuffer.wrap(key)
    val tenant = keySerializer.deserialize(buffer)
    val id = keySerializer.deserialize(buffer)
    (tenant, id)
  }

  /** Return the row prefix of a tenant. */
  def tenantRowKeyPrefix(tenant: JsValue): Array[Byte] = {
    keySerializer.clear
    keySerializer.put(tenant)
    keySerializer.toBytes
  }
}
