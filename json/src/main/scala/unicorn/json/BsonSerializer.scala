package unicorn.json

import java.nio.{CharBuffer, ByteBuffer}
import java.nio.charset.Charset
import unicorn.util.Logging

/**
 * JSON Serialiizer in BSON format as defined by http://bsonspec.org/spec.html.
 * This is not fully compatible with BSON spec, where the root must be a document/JsObject.
 * In contrast, the root can be any JsValue in our implementation.
 *
 * Not Multi-threading safe. Data size limit to 16MB.
 *
 * @author Haifeng Li
 */
class BsonSerializer extends JsonSerializer with Logging {
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
  protected implicit def buffer2Bytes(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes
  }

  protected def string(string: String)(implicit buffer: ByteBuffer): Unit = {
    buffer.putInt(string.length)
    encoder.encode(CharBuffer.wrap(string), buffer, true)
  }

  protected def cstring(string: String)(implicit buffer: ByteBuffer): Unit = {
    encoder.encode(CharBuffer.wrap(string), buffer, true)
    buffer.put(END_OF_STRING)
  }

  protected def serialize(json: JsBoolean, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_BOOLEAN)
    if (ename.isDefined) cstring(ename.get)
    buffer.put(if (json.value) TRUE else FALSE)
  }

  protected def serialize(json: JsInt, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_INT32)
    if (ename.isDefined) cstring(ename.get)
    buffer.putInt(json.value)
  }

  protected def serialize(json: JsLong, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_INT64)
    if (ename.isDefined) cstring(ename.get)
    buffer.putLong(json.value)
  }

  protected def serialize(json: JsDouble, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_DOUBLE)
    if (ename.isDefined) cstring(ename.get)
    buffer.putDouble(json.value)
  }

  protected def serialize(json: JsDate, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_DATETIME)
    if (ename.isDefined) cstring(ename.get)
    buffer.putLong(json.value.getTime)
  }

  protected def serialize(json: JsString, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_STRING)
    if (ename.isDefined) cstring(ename.get)
    string(json.value)
  }

  protected def serialize(json: JsBinary, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_BINARY)
    if (ename.isDefined) cstring(ename.get)
    buffer.putInt(json.value.size)
    buffer.put(BINARY_SUBTYPE_GENERIC)
    buffer.put(json.value)
  }

  protected def serialize(json: JsObject, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_DOCUMENT)
    if (ename.isDefined) cstring(ename.get)

    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    json.fields.foreach { case (ename, value) => value match {
      case x: JsBoolean => serialize(x, Some(ename))
      case x: JsInt     => serialize(x, Some(ename))
      case x: JsLong    => serialize(x, Some(ename))
      case x: JsDouble  => serialize(x, Some(ename))
      case x: JsDate    => serialize(x, Some(ename))
      case x: JsString  => serialize(x, Some(ename))
      case x: JsBinary  => serialize(x, Some(ename))
      case x: JsObject  => serialize(x, Some(ename))
      case x: JsArray   => serialize(x, Some(ename))
      case JsNull       => buffer.put(TYPE_NULL); cstring(ename)
      case JsUndefined  => ()
    }}

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  protected def serialize(json: JsArray, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_ARRAY)
    if (ename.isDefined) cstring(ename.get)
    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    json.elements.zipWithIndex.foreach { case (value, index) => value match {
      case x: JsBoolean => serialize(x, Some(ename.toString))
      case x: JsInt     => serialize(x, Some(ename.toString))
      case x: JsLong    => serialize(x, Some(ename.toString))
      case x: JsDouble  => serialize(x, Some(ename.toString))
      case x: JsDate    => serialize(x, Some(ename.toString))
      case x: JsString  => serialize(x, Some(ename.toString))
      case x: JsBinary  => serialize(x, Some(ename.toString))
      case x: JsObject  => serialize(x, Some(ename.toString))
      case x: JsArray   => serialize(x, Some(ename.toString))
      case JsNull       => buffer.put(TYPE_NULL); cstring(ename.toString)
      case JsUndefined  => () // impossible
    }}

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  override def serialize(json: JsValue, jsonPath: String): List[(String, Array[Byte])] = {
    buffer.position(0)
    json match {
      case x: JsBoolean => serialize(x, None)(buffer)
      case x: JsInt     => serialize(x, None)(buffer)
      case x: JsLong    => serialize(x, None)(buffer)
      case x: JsDouble  => serialize(x, None)(buffer)
      case x: JsDate    => serialize(x, None)(buffer)
      case x: JsString  => serialize(x, None)(buffer)
      case x: JsBinary  => serialize(x, None)(buffer)
      case x: JsObject  => serialize(x, None)(buffer)
      case x: JsArray   => serialize(x, None)(buffer)
      case JsNull       => buffer.put(TYPE_NULL)
      case JsUndefined  => ()
    }
    List((jsonPath, buffer2Bytes(buffer)))
  }

  protected def string()(implicit buffer: ByteBuffer): String = {
    val length = buffer.getInt - 1 // length includes the trailing '\X00'
    val dst = new Array[Byte](length)
    buffer.get(dst)
    buffer.get // the trailing '\X00'
    new String(dst)
  }

  protected def cstring()(implicit buffer: ByteBuffer): String = {
    val str = new collection.mutable.ArrayBuffer[Byte](64)
    var b = buffer.get
    while (b != 0) {str += b; b = buffer.get}
    new String(str.toArray)
  }

  protected def ename()(implicit buffer: ByteBuffer): String = cstring

  protected def binary()(implicit buffer: ByteBuffer): JsBinary = {
    val length = buffer.getInt
    val subtype = buffer.get
    val dst = new Array[Byte](length)
    buffer.get(dst)
    JsBinary(dst)
  }

  override def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String): JsValue = {
    val bytes = values.get(rootJsonPath)
    if (bytes.isEmpty) throw new IllegalArgumentException("root value doesn't exist")

    implicit val buffer = ByteBuffer.wrap(bytes.get)
    buffer.get match { // data type
      case TYPE_BOOLEAN   => JsBoolean(buffer.get)
      case TYPE_INT32     => JsInt(buffer.getInt)
      case TYPE_INT64     => JsLong(buffer.getLong)
      case TYPE_DOUBLE    => JsDouble(buffer.getDouble)
      case TYPE_DATETIME  => JsDate(buffer.getLong)
      case TYPE_TIMESTAMP => JsDate(buffer.getLong)
      case TYPE_STRING    => JsString(string)
      case TYPE_BINARY    => binary
      case TYPE_NULL      => JsNull
      case TYPE_UNDEFINED => JsUndefined // should not happen
      case TYPE_DOCUMENT  => val doc = JsObject(); deserialize(doc)
      case TYPE_ARRAY     => val doc = JsObject(); deserialize(doc); JsArray(doc.fields.map{case (k, v) => (k.toInt, v)}.toSeq.sortBy(_._1).map(_._2))
      case x => throw new IllegalStateException("Unsupported BSON type: %02X" format x)
    }
  }

  protected def deserialize(json: JsObject)(implicit buffer: ByteBuffer): JsObject = {
    val size = buffer.getInt // document size

    match {
      case END_OF_DOCUMENT => ()
      case TYPE_BOOLEAN => json(ename) = JsBoolean(buffer.get)
      case TYPE_INT32 => json(ename) = JsInt(buffer.getInt)
      case TYPE_INT64 => json(ename) = JsLong(buffer.getLong)
      case TYPE_DOUBLE => json(ename) = JsDouble(buffer.getDouble)
      case TYPE_DATETIME => json(ename) = JsDate(buffer.getLong)
      case TYPE_TIMESTAMP => json(ename) = JsDate(buffer.getLong)
      case TYPE_STRING => json(ename) = JsString(string)
      case TYPE_BINARY => buffer.get; json(cstring) = binary
      case TYPE_NULL => json(ename) = JsNull
      case TYPE_UNDEFINED => ename()
      case TYPE_DOCUMENT => val doc = JsObject(); json(ename) = deserialize(doc)
      case TYPE_ARRAY => val doc = JsObject(); val field = ename(); deserialize(doc); json(field) = JsArray(doc.fields.map{case (k, v) => (k.toInt, v)}.toSeq.sortBy(_._1).map(_._2))
      case x => throw new IllegalStateException("Unsupported BSON type: %02X" format x)
    }

    if (buffer.position != size)
      log.warn(s"BSON size $size but deserialize finishs at ${buffer.position}")

    json
  }
}
