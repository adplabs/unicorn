/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.Charset

/**
 * JSON value. Note that JavaScript doesn't distinguish
 * integers and floats and doesn't have date type.
 * 
 * @author Haifeng Li (293050)
 */
abstract class JsonValue {
  /**
   * Converts this value to a byte array.
   */
  def bytes: Array[Byte]
  
  /**
   * Helper function convert ByteBuffer to Array[Byte]
   */
  protected def getBytes(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes  
  }
}

object JsonValue {
  val charset = Charset.forName("UTF-8")
  val encoder = charset.newEncoder
  val decoder = charset.newDecoder

}

case object JsonUndefinedValue extends JsonValue {
  override def toString = "undefined"
  override def bytes = throw new UnsupportedOperationException
}

case class JsonBoolValue(value: Boolean) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonBoolValue.prefix)
    buffer.put(JsonBoolValue.byte(value))
    getBytes(buffer)
  }
}

object JsonBoolValue {
  val one: Byte = 1
  val zero: Byte = 0
  def byte(b: Boolean): Byte = if (b) JsonBoolValue.one else JsonBoolValue.zero
  
  val prefix = "B=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonBoolValue = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonBoolValue(buffer.get != 0)
  }
}

case class JsonIntValue(value: Int) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonIntValue.prefix)
    buffer.putInt(value)
    getBytes(buffer)
  }
}

object JsonIntValue {
  val prefix = "I=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonIntValue = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonIntValue(buffer.getInt)
  }
}

case class JsonLongValue(value: Long) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonLongValue.prefix)
    buffer.putLong(value)
    getBytes(buffer)
  }
}

object JsonLongValue {
  val prefix = "L=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonLongValue = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonLongValue(buffer.getLong)
  }
}

case class JsonDoubleValue(value: Double) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonDoubleValue.prefix)
    buffer.putDouble(value)
    getBytes(buffer)
  }
}

object JsonDoubleValue {
  val prefix = "D=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonDoubleValue = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonDoubleValue(buffer.getDouble)
  }
}

case class JsonStringValue(value: String) extends JsonValue {
  override def toString = "\"" + value.replace("\"", "\\\"") + "\""
  override def bytes = {
    val buffer = ByteBuffer.allocate(2 + 2 * value.length)
    buffer.put(JsonStringValue.prefix)
    JsonValue.encoder.encode(CharBuffer.wrap(value), buffer, true)
    getBytes(buffer)
  }
}

object JsonStringValue {
  val prefix = "S=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonStringValue = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonStringValue(JsonValue.decoder.decode(buffer).toString)
  }
}

case class JsonBlobValue(value: Array[Byte]) extends JsonValue {
  override def toString = "BLOB"
  override def bytes = value
}

object JsonBlobValue {
  def apply(value: Array[Int]): JsonBlobValue = {
    val bytes = new Array[Byte](4 * value.length)
    val buffer = ByteBuffer.wrap(bytes)
    value.foreach { i => buffer.putInt(i) }
    JsonBlobValue(bytes)
  }
}

case class JsonObjectValue(value: collection.mutable.Map[String, JsonValue]) extends JsonValue{
  private val DefaultIndent = "  "
  private val DefaultSeparator = ", "
  override def toString() = toString("", DefaultSeparator)
  
  def toString(indent: String, separator: String): String = {
    val moreIndent = indent + "  "
    "\n" + indent + "{\n" +
    (value.map { case (key, value) => moreIndent + '"' + key + "\": " + 
      (value match {
        case obj: JsonObjectValue => obj.toString(moreIndent + DefaultIndent, separator)
        case _ => value.toString }
      )}
    ).mkString(separator) +
    "\n" + indent + "}"
  }

  override def bytes = {
    val keys = value.keySet.mkString(",")
    val buffer = ByteBuffer.allocate(2 + 2 * keys.length)
    buffer.put(JsonObjectValue.prefix)
    JsonValue.encoder.encode(CharBuffer.wrap(keys), buffer, true)
    getBytes(buffer)
  }
}

object JsonObjectValue {
  val prefix = "O=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): Array[String] = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonValue.decoder.decode(buffer).toString.split(",")
  }
}

case class JsonArrayValue(value: Array[JsonValue]) extends JsonValue {
  override def toString = "[" + value.deep.mkString(", ") + "]"
  override def bytes = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonArrayValue.prefix)
    buffer.putInt(value.length)
    getBytes(buffer)
  }
}

object JsonArrayValue {
  val prefix = "A=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): Int = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    buffer.getInt
  }
}
