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

  def compactPrint = CompactPrinter(this)
  def prettyPrint = PrettyPrinter(this)

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
    case JsonBool(value) => value
    case JsonInt(value) => value != 0
    case JsonLong(value) => value != 0
    case JsonDouble(value) => value != 0
    case JsonString(value) => !value.isEmpty
    case JsonNull => false
    case JsonUndefined => false
    case JsonBlob(value) => value.length != 0
  }

  implicit def toInt(json: JsonValue): Int = json match {
    case JsonBool(value) => if (value) 1 else 0
    case JsonInt(value) => value
    case JsonLong(value) => value.toInt
    case JsonDouble(value) => value.toInt
    case JsonString(value) => value.toInt
    case JsonNull => 0
    case JsonUndefined => 0
    case JsonBlob(_) => throw new UnsupportedOperationException("convert BLOB to int")
  }

  implicit def toLong(json: JsonValue): Long = json match {
    case JsonBool(value) => if (value) 1 else 0
    case JsonInt(value) => value
    case JsonLong(value) => value
    case JsonDouble(value) => value.toLong
    case JsonString(value) => value.toLong
    case JsonNull => 0
    case JsonUndefined => 0
    case JsonBlob(_) => throw new UnsupportedOperationException("convert BLOB to long")
  }

  implicit def toDouble(json: JsonValue): Double = json match {
    case JsonBool(value) => if (value) 1.0 else 0.0
    case JsonInt(value) => value
    case JsonLong(value) => value
    case JsonDouble(value) => value
    case JsonString(value) => value.toDouble
    case JsonNull => 0.0
    case JsonUndefined => 0.0
    case JsonBlob(_) => throw new UnsupportedOperationException("convert BLOB to double")
  }

  implicit def toString(json: JsonValue): String = json.toString
}

object JsonValueImplicits {
  implicit def bool2JsonValue(value: Boolean) = JsonBool(value)
  implicit def int2JsonValue(value: Int) = JsonInt(value)
  implicit def long2JsonValue(value: Long) = JsonLong(value)
  implicit def double2JsonValue(value: Double) = JsonDouble(value)
  implicit def string2JsonValue(value: String) = JsonString(value)
  implicit def byteArray2JsonValue(value: Array[Byte]) = JsonBlob(value)
  implicit def array2JsonValue(value: Array[JsonValue]) = JsonArray(value: _*)
  implicit def map2JsonValue(value: collection.mutable.Map[String, JsonValue]) = JsonObject(value)
  implicit def boolArray2JsonValue(values: Array[Boolean]): JsonArray = JsonArray(values.map {e => JsonBool(e)}: _*)
  implicit def intArray2JsonValue(values: Array[Int]): JsonArray = JsonArray(values.map {e => JsonInt(e)}: _*)
  implicit def longArray2JsonValue(values: Array[Long]): JsonArray = JsonArray(values.map {e => JsonLong(e)}: _*)
  implicit def doubleArray2JsonValue(values: Array[Double]): JsonArray = JsonArray(values.map {e => JsonDouble(e)}: _*)
  implicit def stringArray2JsonValue(values: Array[String]): JsonArray = JsonArray(values.map {e => JsonString(e)}: _*)
}

case object JsonNull extends JsonValue {
  override def toString = "null"
  override def bytes = throw new UnsupportedOperationException
}

case object JsonUndefined extends JsonValue {
  override def toString = "undefined"
  override def bytes = throw new UnsupportedOperationException
}

case class JsonBool(value: Boolean) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonBool.prefix)
    buffer.put(JsonBool.byte(value))
    getBytes(buffer)
  }
}

object JsonBool {
  val one: Byte = 1
  val zero: Byte = 0
  def byte(b: Boolean): Byte = if (b) JsonBool.one else JsonBool.zero
  
  val prefix = "B=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonBool = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonBool(buffer.get != 0)
  }
}

case class JsonInt(value: Int) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonInt.prefix)
    buffer.putInt(value)
    getBytes(buffer)
  }
}

object JsonInt {
  val prefix = "I=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonInt = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonInt(buffer.getInt)
  }
}

case class JsonLong(value: Long) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonLong.prefix)
    buffer.putLong(value)
    getBytes(buffer)
  }
}

object JsonLong {
  val prefix = "L=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonLong = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonLong(buffer.getLong)
  }
}

case class JsonDouble(value: Double) extends JsonValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonDouble.prefix)
    buffer.putDouble(value)
    getBytes(buffer)
  }
}

object JsonDouble {
  val prefix = "D=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonDouble = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonDouble(buffer.getDouble)
  }
}

case class JsonString(value: String) extends JsonValue {
  override def toString = value.toString
  override def bytes = {
    val buffer = ByteBuffer.allocate(2 + 2 * value.length)
    buffer.put(JsonString.prefix)
    JsonValue.encoder.encode(CharBuffer.wrap(value), buffer, true)
    getBytes(buffer)
  }
}

object JsonString {
  val prefix = "S=".getBytes(JsonValue.charset)
  def apply(bytes: Array[Byte]): JsonString = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonString(JsonValue.decoder.decode(buffer).toString)
  }
}

case class JsonBlob(value: Array[Byte]) extends JsonValue {
  override def toString = value.map("%02X" format _).mkString
  override def bytes = value
}

object JsonBlob {
  def apply(value: Array[Int]): JsonBlob = {
    val bytes = new Array[Byte](4 * value.length)
    val buffer = ByteBuffer.wrap(bytes)
    value.foreach { i => buffer.putInt(i) }
    JsonBlob(bytes)
  }
}

case class JsonObject(fields: collection.mutable.Map[String, JsonValue]) extends JsonValue {
  override def toString = compactPrint
  override def bytes = {
    val keys = fields.keySet.mkString(",")
    val buffer = ByteBuffer.allocate(2 + 2 * keys.length)
    buffer.put(JsonObject.prefix)
    JsonValue.encoder.encode(CharBuffer.wrap(keys), buffer, true)
    getBytes(buffer)
  }

  def apply(key: String): JsonValue = {
    if (fields.contains(key))
      fields(key)
    else
      JsonUndefined
  }

  def selectDynamic(key: String): JsonValue = {
    apply(key)
  }

  def remove(key: String): Option[JsonValue] = {
    val value = fields.remove(key)
    value
  }

  def update(key: String, value: JsonValue): JsonObject = {
    fields(key) = value
    this
  }

  def updateDynamic(key: String)(value: Any) {
    import JsonValueImplicits._
    value match {
      case value: String => update(key, JsonString(value))
      case value: Int => update(key, value)
      case value: Double => update(key, value)
      case value: Boolean => update(key, value)
      case value: Long => update(key, value)
      case value: JsonValue => update(key, value)
      case value: Array[String] => update(key, value)
      case value: Array[Int] => update(key, value)
      case value: Array[Double] => update(key, value)
      case value: Array[Boolean] => update(key, value)
      case value: Array[Long] => update(key, value)
      case value: Array[JsonValue] => update(key, value)
      case Some(value: String) => update(key, value)
      case Some(value: Int) => update(key, value)
      case Some(value: Double) => update(key, value)
      case Some(value: Boolean) => update(key, value)
      case Some(value: Long) => update(key, value)
      case Some(value: JsonValue) => update(key, value)
      case null | None => remove(key)
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
  }
}

object JsonObject {
  def apply(fields: (String, JsonValue)*) = new JsonObject(collection.mutable.Map(fields: _*))
  val prefix = "O=".getBytes(JsonValue.charset)
  // returns the list of field names
  def apply(bytes: Array[Byte]): Array[String] = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsonValue.decoder.decode(buffer).toString.split(",")
  }
}

case class JsonArray(elements: collection.mutable.ArrayBuffer[JsonValue]) extends JsonValue {
  override def toString = compactPrint
  override def bytes = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsonArray.prefix)
    buffer.putInt(elements.length)
    getBytes(buffer)
  }

  def apply(index: Int): JsonValue = {
    elements(index)
  }

  def remove(index: Int): JsonValue = {
    elements.remove(index)
  }

  def update(index: Int, value: JsonValue): JsonArray = {
    elements(index) = value
    this
  }

  def updateDynamic(index: Int)(value: Any) {
    import JsonValueImplicits._
    value match {
      case value: String => update(index, value)
      case value: Int => update(index, value)
      case value: Double => update(index, value)
      case value: Boolean => update(index, value)
      case value: Long => update(index, value)
      case value: JsonValue => update(index, value)
      case value: Array[String] => update(index, value)
      case value: Array[Int] => update(index, value)
      case value: Array[Double] => update(index, value)
      case value: Array[Boolean] => update(index, value)
      case value: Array[Long] => update(index, value)
      case value: Array[JsonValue] => update(index, value)
      case Some(value: String) => update(index, value)
      case Some(value: Int) => update(index, value)
      case Some(value: Double) => update(index, value)
      case Some(value: Boolean) => update(index, value)
      case Some(value: Long) => update(index, value)
      case Some(value: JsonValue) => update(index, value)
      case null | None => remove(index)
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
  }

  /**
   * Appends a single element to this array and returns
   * the identity of the array. It takes constant amortized time.
   *
   * @param elem  the element to append.
   * @return      the updated array.
   */
  def +=(value: Any): JsonArray = {
    import JsonValueImplicits._
    value match {
      case value: String => elements += value
      case value: Int => elements += value
      case value: Double => elements += value
      case value: Boolean => elements += value
      case value: Long => elements += value
      case value: JsonValue => elements += value
      case value: Array[String] => elements += value
      case value: Array[Int] => elements += value
      case value: Array[Double] => elements += value
      case value: Array[Boolean] => elements += value
      case value: Array[Long] => elements += value
      case value: Array[JsonValue] => elements += value
      case Some(value: String) => elements += value
      case Some(value: Int) => elements += value
      case Some(value: Double) => elements += value
      case Some(value: Boolean) => elements += value
      case Some(value: Long) => elements += value
      case Some(value: JsonValue) => elements += value
      case null => elements += JsonNull
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
    this
  }
}

object JsonArray {
  def apply(elements: JsonValue*) = new JsonArray(collection.mutable.ArrayBuffer(elements: _*))
  val prefix = "A=".getBytes(JsonValue.charset)
  // return the array size
  def apply(bytes: Array[Byte]): Int = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    buffer.getInt
  }
}
