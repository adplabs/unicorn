/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.Charset
import scala.language.implicitConversions

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

  implicit def toBool(json: JsonValue): Boolean = json match {
    case JsonBoolValue(value) => value
    case JsonIntValue(value) => value != 0
    case JsonLongValue(value) => value != 0
    case JsonDoubleValue(value) => value != 0
    case JsonStringValue(value) => !value.isEmpty
    case JsonUndefinedValue => false
    case JsonBlobValue(value) => value.length != 0
  }

  implicit def toInt(json: JsonValue): Int = json match {
    case JsonBoolValue(value) => if (value) 1 else 0
    case JsonIntValue(value) => value
    case JsonLongValue(value) => value.toInt
    case JsonDoubleValue(value) => value.toInt
    case JsonStringValue(value) => value.toInt
    case JsonUndefinedValue => 0
    case JsonBlobValue(_) => throw new UnsupportedOperationException("convert BLOB to int")
  }

  implicit def toLong(json: JsonValue): Long = json match {
    case JsonBoolValue(value) => if (value) 1 else 0
    case JsonIntValue(value) => value
    case JsonLongValue(value) => value
    case JsonDoubleValue(value) => value.toLong
    case JsonStringValue(value) => value.toLong
    case JsonUndefinedValue => 0
    case JsonBlobValue(_) => throw new UnsupportedOperationException("convert BLOB to long")
  }

  implicit def toDouble(json: JsonValue): Double = json match {
    case JsonBoolValue(value) => if (value) 1 else 0
    case JsonIntValue(value) => value
    case JsonLongValue(value) => value
    case JsonDoubleValue(value) => value
    case JsonStringValue(value) => value.toDouble
    case JsonUndefinedValue => 0
    case JsonBlobValue(_) => throw new UnsupportedOperationException("convert BLOB to double")
  }

  implicit def toString(json: JsonValue): String = json match {
    case JsonBoolValue(value) => value.toString
    case JsonIntValue(value) => value.toString
    case JsonLongValue(value) => value.toString
    case JsonDoubleValue(value) => value.toString
    case JsonStringValue(value) => value.toString
    case JsonUndefinedValue => "undefined"
    case JsonBlobValue(value) => value.toString
  }
}

object JsonValueImplicits {
  implicit def Bool2JsonValue(value: Boolean) = JsonBoolValue(value)
  implicit def Int2JsonValue(value: Int) = JsonIntValue(value)
  implicit def Long2JsonValue(value: Long) = JsonLongValue(value)
  implicit def Double2JsonValue(value: Double) = JsonDoubleValue(value)
  implicit def String2JsonValue(value: String) = JsonStringValue(value)
  implicit def ByteArray2JsonValue(value: Array[Byte]) = JsonBlobValue(value)
  implicit def Array2JsonValue(value: Array[JsonValue]) = JsonArrayValue(value)
  implicit def Map2JsonValue(value: collection.mutable.Map[String, JsonValue]) = JsonObjectValue(value)
  implicit def Document2JsonValue(value: Document) = value.json
  implicit def BoolArray2JsonValue(values: Array[Boolean]): JsonArrayValue = JsonArrayValue(values.map {e => JsonBoolValue(e) })
  implicit def IntArray2JsonValue(values: Array[Int]): JsonArrayValue = JsonArrayValue(values.map {e => JsonIntValue(e) })
  implicit def LongArray2JsonValue(values: Array[Long]): JsonArrayValue = JsonArrayValue(values.map {e => JsonLongValue(e) })
  implicit def DoubleArray2JsonValue(values: Array[Double]): JsonArrayValue = JsonArrayValue(values.map {e => JsonDoubleValue(e) })
  implicit def StringArray2JsonValue(values: Array[String]): JsonArrayValue = JsonArrayValue(values.map {e => JsonStringValue(e) })
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
