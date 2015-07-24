package com.adp.unicorn.json

import java.nio.{CharBuffer, ByteBuffer}
import java.nio.charset.Charset

/**
 * JSON Serialiizer in BSON format. Not Multi-threading safe.
 * Data size limit to 16MB.
 *
 * @author Haifeng Li
 */
class BsonSerializer extends JsonSerializer {
  /** End of document */
  val END_OF_DOCUMENT: Byte = 0x00

  /** End of string */
  val END_OF_STRING: Byte = 0x00

  /** Type markers */
  val TYPE_DOUBLE: Byte = 0x01
  val TYPE_STRING: Byte = 0x02
  val TYPE_DOCUMENT: Byte = 0x03
  val TYPE_ARRAY: Byte = 0x04
  val TYPE_BINARY: Byte = 0x05
  val TYPE_UNDEFINED: Byte = 0x06
  val TYPE_OBJECTID: Byte = 0x07
  val TYPE_BOOLEAN: Byte = 0x08
  val TYPE_DATETIME: Byte = 0x09
  val TYPE_NULL: Byte = 0x0A
  val TYPE_REGEX: Byte = 0x0B
  val TYPE_DBPOINTER: Byte = 0x0C
  val TYPE_JAVASCRIPT: Byte = 0x0D
  val TYPE_SYMBOL: Byte = 0x0E
  val TYPE_JAVASCRIPT_WITH_SCOPE: Byte = 0x0F
  val TYPE_INT32: Byte = 0x10
  val TYPE_TIMESTAMP: Byte = 0x11
  val TYPE_INT64: Byte = 0x12
  val TYPE_MINKEY: Byte = 0xFF.toByte
  val TYPE_MAXKEY: Byte = 0x7F

  /** Binary subtypes */
  val BINARY_SUBTYPE_GENERIC: Byte = 0x00
  val BINARY_SUBTYPE_FUNCTION: Byte = 0x01
  val BINARY_SUBTYPE_BINARY_OLD: Byte = 0x02
  val BINARY_SUBTYPE_UUID: Byte = 0x03
  val BINARY_SUBTYPE_MD5: Byte = 0x05
  val BINARY_SUBTYPE_USER_DEFINED: Byte = 0x80.toByte

  val TRUE: Byte = 0x00
  val FALSE: Byte = 0x00

  // string encoder/decoder
  val charset = Charset.forName("UTF-8")
  val encoder = charset.newEncoder
  val decoder = charset.newDecoder

  // working buffer
  val buffer = ByteBuffer.allocate(16 * 1024 * 1024)

  /**
   * Helper function convert ByteBuffer to Array[Byte]
   */
  private def getBytes(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes
  }

  private def putString(buffer: ByteBuffer, string: String): Unit = {
    buffer.putInt(string.length)
    encoder.encode(CharBuffer.wrap(string), buffer, true)
  }

  private def putCString(buffer: ByteBuffer, string: String): Unit = {
    encoder.encode(CharBuffer.wrap(string), buffer, true)
    buffer.put(END_OF_STRING)
  }

  override def serialize(json: JsTopLevel): Array[Byte] = {
    buffer.position(0)
    serialize(json, buffer)
    getBytes(buffer)
  }

  private def serialize(json: JsTopLevel, buffer: ByteBuffer): Unit = {
    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    val fields = json match {
      case Left(obj) => obj.fields.toSeq
      case Right(arr) => arr.elements.zipWithIndex.map{case (x, i) => (i.toString, x)}
    }
    fields.foreach { case (field, value) => value match {
      case JsBoolean(x) =>
        buffer.put(TYPE_BOOLEAN)
        putCString(buffer, field)
        buffer.put(if (x) TRUE else FALSE)
      case JsInt(x) =>
        buffer.put(TYPE_INT32)
        putCString(buffer, field)
        buffer.putInt(x)
      case JsLong(x) =>
        buffer.put(TYPE_INT64)
        putCString(buffer, field)
        buffer.putLong(x)
      case JsDate(x, _) =>
        buffer.put(TYPE_DATETIME)
        putCString(buffer, field)
        buffer.putLong(x.getTime)
      case JsDouble(x) =>
        buffer.put(TYPE_DOUBLE)
        putCString(buffer, field)
        buffer.putDouble(x)
      case JsString(x) =>
        buffer.put(TYPE_STRING)
        putCString(buffer, field)
        putString(buffer, x)
      case JsBinary(x) =>
        buffer.put(TYPE_BINARY)
        buffer.put(BINARY_SUBTYPE_GENERIC)
        putCString(buffer, field)
        buffer.put(x)
      case JsNull =>
        buffer.put(TYPE_NULL)
        putCString(buffer, field)
      case JsUndefined => ()
      case obj: JsObject =>
        buffer.put(TYPE_DOCUMENT)
        putCString(buffer, field)
        serialize(obj)
      case arr: JsArray =>
        buffer.put(TYPE_ARRAY)
        putCString(buffer, field)
        serialize(arr)
    }}

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  override def deserialize(bytes: Array[Byte]): JsTopLevel = {
    val buffer = ByteBuffer.wrap(bytes)
    val json = JsObject()
    val size = buffer.getInt // document size
    deserialize(buffer, json)
    json
  }

  private def getCString(buffer: ByteBuffer): String = {
    ""
  }

  private def deserialize(buffer: ByteBuffer, json: JsObject): Unit = {
    val magic = buffer.get
    magic match {
      case END_OF_DOCUMENT => ()
      case TYPE_BOOLEAN => json(getCString(buffer)) = JsBoolean(buffer.get)
      case _ => throw new IllegalStateException("Unsupported BSON type: %02X" format magic)
    }
    /*
    if (value.startsWith(JsonString.prefix)) JsonString(value)
    else if (value.startsWith(JsonInt.prefix)) JsonInt(value)
    else if (value.startsWith(JsonDouble.prefix)) JsonDouble(value)
    else if (value.startsWith(JsonBool.prefix)) JsonBool(value)
    else if (value.startsWith(JsonLong.prefix)) JsonLong(value)
    else if (value.startsWith(JsonString.prefix)) JsonString(value)
    else if (value.startsWith(JsonObject.prefix)) {
      val child = collection.mutable.Map[String, JsonValue]()

      val fields = JsonObject(value)
      fields.foreach { field =>
        val fieldkey = s"$key.$field"
        child(field) = parse(fieldkey, kv(fieldkey), kv)
      }

      new JsonObject(child)
    } else if (value.startsWith(JsonArray.prefix)) {
      val size = JsonArray(value)
      val array = new Array[JsonValue](size)

      for (i <- 0 until size) {
        val fieldkey = s"$key[$i]"
        array(i) = parse(fieldkey, kv(fieldkey), kv)
      }

      JsonArray(array: _*)
    }
    else JsonBlob(value)
    */
  }
}
