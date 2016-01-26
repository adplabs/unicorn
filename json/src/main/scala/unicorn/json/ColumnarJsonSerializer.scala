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

package unicorn.json

import java.nio.{ByteBuffer, ByteOrder}
import unicorn.util._

/**
 * This JSON Serializer recursively encodes each field to a byte string, which can
 * be saved separately to a column in BigTable. The JsonPath of field can be used
 * as the column name.
 * <p>
 * Not Multi-threading safe. Each thread should have its own ColumnarJsonSerializer instance.
 * <p>
 * ByteBuffer must use BIG ENDIAN to ensure the correct byte string comparison for
 * integers and floating numbers.
 *
 * @author Haifeng Li
 */
class ColumnarJsonSerializer(val buffer: ByteBuffer = ByteBuffer.allocate(16 * 1024 * 1024)) extends JsonSerializer with JsonSerializerHelper {
  require(buffer.order == ByteOrder.BIG_ENDIAN)

  private def jsonPath(parent: String, field: String) = s"%s${JsonSerializer.pathDelimiter}%s".format(parent, field)

  private def jsonPath(parent: String, index: Int) = s"%s${JsonSerializer.pathDelimiter}%d".format(parent, index)

  def serialize(json: JsCounter)(implicit buffer: ByteBuffer): Unit = {
    buffer.putLong(json.value)
  }

  def serialize(json: JsObject, ename: Option[String], map: collection.mutable.Map[String, Array[Byte]])(implicit buffer: ByteBuffer): Unit = {
    require(ename.isDefined)
    buffer.clear
    buffer.put(TYPE_DOCUMENT)
    json.fields.foreach { case (field, _) => cstring(field) }
    map(ename.getOrElse(root)) = buffer

    json.fields.foreach { case (field, value) =>
      buffer.clear
      value match {
        case x: JsBoolean  => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsInt      => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsLong     => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsCounter  => serialize(x); map(jsonPath(ename.get, field)) = buffer
        case x: JsDouble   => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsString   => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsDate     => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsUUID     => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsObjectId => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsBinary   => serialize(x, None); map(jsonPath(ename.get, field)) = buffer
        case x: JsObject   => serialize(x, Some(jsonPath(ename.get, field)), map)
        case x: JsArray    => serialize(x, Some(jsonPath(ename.get, field)), map)
        case JsNull        => buffer.put(TYPE_NULL); map(jsonPath(ename.get, field)) = buffer
        case JsUndefined   => buffer.put(TYPE_UNDEFINED); map(jsonPath(ename.get, field)) = buffer
      }
    }
  }

  def serialize(json: JsArray, ename: Option[String], map: collection.mutable.Map[String, Array[Byte]])(implicit buffer: ByteBuffer): Unit = {
    require(ename.isDefined)
    buffer.clear
    buffer.put(TYPE_ARRAY)
    buffer.putInt(json.elements.size)
    map(ename.getOrElse(root)) = buffer

    json.elements.zipWithIndex.foreach { case (value, index) =>
      buffer.clear
      value match {
        case x: JsBoolean  => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsInt      => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsLong     => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsCounter  => serialize(x); map(jsonPath(ename.get, index)) = buffer
        case x: JsDouble   => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsString   => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsDate     => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsUUID     => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsObjectId => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsBinary   => serialize(x, None); map(jsonPath(ename.get, index)) = buffer
        case x: JsObject   => serialize(x, Some(jsonPath(ename.get, index)), map)
        case x: JsArray    => serialize(x, Some(jsonPath(ename.get, index)), map)
        case JsNull        => buffer.put(TYPE_NULL); map(jsonPath(ename.get, index)) = buffer
        case JsUndefined   => buffer.put(TYPE_UNDEFINED); map(jsonPath(ename.get, index)) = buffer
      }
    }
  }

  override def serialize(json: JsValue, jsonPath: String): Map[String, Array[Byte]] = {
    buffer.clear
    val map = collection.mutable.Map[String, Array[Byte]]()
    json match {
      case x: JsBoolean  => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsInt      => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsLong     => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsCounter  => serialize(x)(buffer); map(jsonPath) = buffer
      case x: JsDouble   => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsString   => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsDate     => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsUUID     => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsObjectId => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsBinary   => serialize(x, None)(buffer); map(jsonPath) = buffer
      case x: JsObject   => serialize(x, Some(jsonPath), map)(buffer)
      case x: JsArray    => serialize(x, Some(jsonPath), map)(buffer)
      case JsNull        => buffer.put(TYPE_NULL); map(jsonPath) = buffer
      case JsUndefined  => buffer.put(TYPE_UNDEFINED); map(jsonPath) = buffer
    }
    map.toMap
  }

  override def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String): JsValue = {
    val bytes = values.get(rootJsonPath)
    if (bytes.isEmpty) return JsUndefined

    implicit val buffer = ByteBuffer.wrap(bytes.get)
    buffer.get match {
      // data type
      case TYPE_BOOLEAN  => boolean
      case TYPE_INT32     => int
      case TYPE_INT64     => long
      case END_OF_DOCUMENT | TYPE_MINKEY if bytes.get.length == 8 => JsCounter(buffer.getLong(0)) // hacking counter
      case TYPE_DOUBLE    => double
      case TYPE_DATETIME  => date
      case TYPE_STRING    => string
      case TYPE_OBJECTID  => objectId
      case TYPE_BINARY    => binary
      case TYPE_NULL      => JsNull
      case TYPE_UNDEFINED => JsUndefined
      case TYPE_DOCUMENT  => val keys = fields(buffer); val kv = keys.map { key => (key, deserialize(values, jsonPath(rootJsonPath, key))) }.filter(_._2 != JsUndefined); JsObject(kv: _*)
      case TYPE_ARRAY     => val size = buffer.getInt; val elements = 0.until(size) map { index => deserialize(values, jsonPath(rootJsonPath, index)) }; JsArray(elements: _*)
      case x              => throw new IllegalStateException("Unsupported JSON type: %02X" format x)
    }
  }

  private def fields(buffer: ByteBuffer): Seq[String] = {
    val keys = collection.mutable.ArrayBuffer[String]()
    while (buffer.hasRemaining) {
      keys += cstring()(buffer)
    }
    keys
  }
}
