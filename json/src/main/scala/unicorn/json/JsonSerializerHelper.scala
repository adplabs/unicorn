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

import java.nio.charset.Charset
import java.nio.ByteBuffer
import unicorn.oid.BsonObjectId

/**
 * @author Haifeng Li
 */
trait JsonSerializerHelper {
  /** End of document */
  val END_OF_DOCUMENT             : Byte = 0x00

  /** End of string */
  val END_OF_STRING               : Byte = 0x00

  /** Type markers */
  val TYPE_DOUBLE                 : Byte = 0x01
  val TYPE_STRING                 : Byte = 0x02
  val TYPE_DOCUMENT               : Byte = 0x03
  val TYPE_ARRAY                  : Byte = 0x04
  val TYPE_BINARY                 : Byte = 0x05
  val TYPE_UNDEFINED              : Byte = 0x06
  val TYPE_OBJECTID               : Byte = 0x07
  val TYPE_BOOLEAN                : Byte = 0x08
  val TYPE_DATETIME               : Byte = 0x09
  val TYPE_NULL                   : Byte = 0x0A
  val TYPE_REGEX                  : Byte = 0x0B
  val TYPE_DBPOINTER              : Byte = 0x0C
  val TYPE_JAVASCRIPT             : Byte = 0x0D
  val TYPE_SYMBOL                 : Byte = 0x0E
  val TYPE_JAVASCRIPT_WITH_SCOPE  : Byte = 0x0F
  val TYPE_INT32                  : Byte = 0x10
  val TYPE_TIMESTAMP              : Byte = 0x11
  val TYPE_INT64                  : Byte = 0x12
  val TYPE_MINKEY                 : Byte = 0xFF.toByte
  val TYPE_MAXKEY                 : Byte = 0x7F

  /** Binary subtypes */
  val BINARY_SUBTYPE_GENERIC      : Byte = 0x00
  val BINARY_SUBTYPE_FUNCTION     : Byte = 0x01
  val BINARY_SUBTYPE_BINARY_OLD   : Byte = 0x02
  val BINARY_SUBTYPE_UUID_OLD     : Byte = 0x03
  val BINARY_SUBTYPE_UUID         : Byte = 0x04
  val BINARY_SUBTYPE_MD5          : Byte = 0x05
  val BINARY_SUBTYPE_USER_DEFINED : Byte = 0x80.toByte

  val TRUE                        : Byte = 0x01
  val FALSE                       : Byte = 0x00

  /** string encoder/decoder */
  val charset = Charset.forName("UTF-8")

  /**
   * Helper function convert ByteBuffer to Array[Byte]
   */
  implicit def buffer2Bytes(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes
  }

  def cstring(string: String)(implicit buffer: ByteBuffer): Unit = {
    val bytes = string.getBytes(charset)
    buffer.put(bytes)
    buffer.put(END_OF_STRING)
  }

  def serialize(json: JsBoolean, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_BOOLEAN)
    if (ename.isDefined) cstring(ename.get)
    buffer.put(if (json.value) TRUE else FALSE)
  }

  def serialize(json: JsInt, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_INT32)
    if (ename.isDefined) cstring(ename.get)
    // We flip the leading bit so that negative values will
    // sort before 0 in ASC order for bit strings. This is
    // important as integers on JVM are all signed.
    buffer.putInt(json.value ^ 0x80000000)
  }

  def serialize(json: JsLong, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_INT64)
    if (ename.isDefined) cstring(ename.get)
    // We flip the leading bit so that negative values will
    // sort before 0 in ASC order for bit strings. This is
    // important as integers on JVM are all signed.
    buffer.putLong(json.value ^ 0x8000000000000000L)
  }

  def serialize(json: JsDouble, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_DOUBLE)
    if (ename.isDefined) cstring(ename.get)
    buffer.putDouble(json.value)
  }

  def serialize(json: JsString, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_STRING)
    if (ename.isDefined) cstring(ename.get)
    val bytes = json.value.getBytes(charset)
    buffer.putInt(bytes.length)
    buffer.put(bytes)
  }

  def serialize(json: JsDate, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_DATETIME)
    if (ename.isDefined) cstring(ename.get)
    buffer.putLong(json.value.getTime)
  }

  def serialize(json: JsObjectId, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_OBJECTID)
    if (ename.isDefined) cstring(ename.get)
    buffer.put(json.value.id)
  }

  def serialize(json: JsUUID, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_BINARY)
    if (ename.isDefined) cstring(ename.get)
    buffer.putInt(16)
    buffer.put(BINARY_SUBTYPE_UUID)
    buffer.putLong(json.value.getMostSignificantBits)
    buffer.putLong(json.value.getLeastSignificantBits)
  }

  def serialize(json: JsBinary, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_BINARY)
    if (ename.isDefined) cstring(ename.get)
    buffer.putInt(json.value.size)
    buffer.put(BINARY_SUBTYPE_GENERIC)
    buffer.put(json.value)
  }

  def cstring()(implicit buffer: ByteBuffer): String = {
    val str = new collection.mutable.ArrayBuffer[Byte](64)
    var b = buffer.get
    while (b != 0) {str += b; b = buffer.get}
    new String(str.toArray)
  }

  def ename()(implicit buffer: ByteBuffer): String = cstring

  def boolean()(implicit buffer: ByteBuffer): JsBoolean = {
    val b = buffer.get
    if (b == 0) JsFalse else JsTrue
  }

  def int()(implicit buffer: ByteBuffer): JsInt = {
    val x = buffer.getInt
    // Remember to flip back the leading bit
    if (x == 0) JsInt.zero else JsInt(x ^ 0x80000000)
  }

  def long()(implicit buffer: ByteBuffer): JsLong = {
    val x = buffer.getLong
    // Remember to flip back the leading bit
    if (x == 0) JsLong.zero else JsLong(x ^ 0x8000000000000000L)
  }

  def double()(implicit buffer: ByteBuffer): JsDouble = {
    val x = buffer.getDouble
    if (x == 0.0) JsDouble.zero else JsDouble(x)
  }

  def date()(implicit buffer: ByteBuffer): JsDate = {
    JsDate(buffer.getLong)
  }

  def objectId()(implicit buffer: ByteBuffer): JsValue = {
    val id = new Array[Byte](BsonObjectId.size)
    buffer.get(id)
    JsObjectId(BsonObjectId(id))
  }

  def string()(implicit buffer: ByteBuffer): JsString = {
    val length = buffer.getInt
    val dst = new Array[Byte](length)
    buffer.get(dst)
    JsString(new String(dst, charset))
  }

  def binary()(implicit buffer: ByteBuffer): JsValue = {
    val length = buffer.getInt
    val subtype = buffer.get
    if (subtype == BINARY_SUBTYPE_UUID) {
      JsUUID(buffer.getLong, buffer.getLong)
    } else {
      val dst = new Array[Byte](length)
      buffer.get(dst)
      JsBinary(dst)
    }
  }
}
