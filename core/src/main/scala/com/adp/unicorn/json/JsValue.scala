/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.json

import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.Charset

import scala.language.implicitConversions

/**
 * JSON value. Note that JavaScript doesn't distinguish
 * integers and floats and doesn't have date type.
 *
 * @author Haifeng Li
 */
sealed abstract class JsValue {
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

object JsValue {
  val charset = Charset.forName("UTF-8")
  val encoder = charset.newEncoder
  val decoder = charset.newDecoder

  implicit def toBool(json: JsValue): Boolean = json match {
    case JsBool(value) => value
    case JsInt(value) => value != 0
    case JsLong(value) => value != 0
    case JsDouble(value) => value != 0
    case JsString(value) => !value.isEmpty
    case JsNull => false
    case JsUndefined => false
    case _ => throw new UnsupportedOperationException("convert JsValue to int")
  }

  implicit def toInt(json: JsValue): Int = json match {
    case JsBool(value) => if (value) 1 else 0
    case JsInt(value) => value
    case JsLong(value) => value.toInt
    case JsDouble(value) => value.toInt
    case JsString(value) => value.toInt
    case JsNull => 0
    case JsUndefined => 0
    case _ => throw new UnsupportedOperationException("convert JsValue to int")
  }

  implicit def toLong(json: JsValue): Long = json match {
    case JsBool(value) => if (value) 1 else 0
    case JsInt(value) => value
    case JsLong(value) => value
    case JsDouble(value) => value.toLong
    case JsString(value) => value.toLong
    case JsNull => 0
    case JsUndefined => 0
    case _ => throw new UnsupportedOperationException("convert JsValue to long")
  }

  implicit def toDouble(json: JsValue): Double = json match {
    case JsBool(value) => if (value) 1.0 else 0.0
    case JsInt(value) => value
    case JsLong(value) => value
    case JsDouble(value) => value
    case JsString(value) => value.toDouble
    case JsNull => 0.0
    case JsUndefined => 0.0
    case _ => throw new UnsupportedOperationException("convert JsValue to double")
  }

  implicit def toString(json: JsValue): String = json.toString

  /**
   * Parses the byte array to a JSON value.
   */
  /*
  def apply(value: Array[Byte])(implicit jsonPath: String): JsonValue = {
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
  }
  */
}

object JsValueImplicits {
  implicit def bool2JsonValue(value: Boolean) = JsBool(value)
  implicit def int2JsonValue(value: Int) = JsInt(value)
  implicit def long2JsonValue(value: Long) = JsLong(value)
  implicit def double2JsonValue(value: Double) = JsDouble(value)
  implicit def string2JsonValue(value: String) = JsString(value)
  implicit def byteArray2JsonValue(value: Array[Byte]) = JsBinary(value)
  implicit def array2JsonValue(value: Array[JsValue]) = JsArray(value: _*)
  implicit def map2JsonValue(value: collection.mutable.Map[String, JsValue]) = JsObject(value)
  implicit def boolArray2JsonValue(values: Array[Boolean]): JsArray = JsArray(values.map {e => JsBool(e)}: _*)
  implicit def intArray2JsonValue(values: Array[Int]): JsArray = JsArray(values.map {e => JsInt(e)}: _*)
  implicit def longArray2JsonValue(values: Array[Long]): JsArray = JsArray(values.map {e => JsLong(e)}: _*)
  implicit def doubleArray2JsonValue(values: Array[Double]): JsArray = JsArray(values.map {e => JsDouble(e)}: _*)
  implicit def stringArray2JsonValue(values: Array[String]): JsArray = JsArray(values.map {e => JsString(e)}: _*)
}

case object JsNull extends JsValue {
  override def toString = "null"
  override def bytes = throw new UnsupportedOperationException
}

case object JsUndefined extends JsValue {
  override def toString = "undefined"
  override def bytes = throw new UnsupportedOperationException
}

case class JsBool(value: Boolean) extends JsValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsBool.prefix)
    buffer.put(JsBool.byte(value))
    getBytes(buffer)
  }
}

object JsBool {
  val one: Byte = 1
  val zero: Byte = 0
  def byte(b: Boolean): Byte = if (b) JsBool.one else JsBool.zero

  val prefix = "B=".getBytes(JsValue.charset)
  def apply(bytes: Array[Byte]): JsBool = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsBool(buffer.get != 0)
  }
}

case class JsInt(value: Int) extends JsValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsInt.prefix)
    buffer.putInt(value)
    getBytes(buffer)
  }
}

object JsInt {
  val zero = JsInt(0)
  val prefix = "I=".getBytes(JsValue.charset)
  def apply(bytes: Array[Byte]): JsInt = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsInt(buffer.getInt)
  }
}

case class JsLong(value: Long) extends JsValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsLong.prefix)
    buffer.putLong(value)
    getBytes(buffer)
  }
}

object JsLong {
  val zero = JsLong(0L)
  val prefix = "L=".getBytes(JsValue.charset)
  def apply(bytes: Array[Byte]): JsLong = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsLong(buffer.getLong)
  }
}

case class JsDouble(value: Double) extends JsValue {
  override def toString = value.toString
  override def bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsDouble.prefix)
    buffer.putDouble(value)
    getBytes(buffer)
  }
}

object JsDouble {
  val zero = JsDouble(0.0)
  val prefix = "D=".getBytes(JsValue.charset)
  def apply(bytes: Array[Byte]): JsDouble = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsDouble(buffer.getDouble)
  }
}

case class JsString(value: String) extends JsValue {
  override def toString = value.toString
  override def bytes = {
    val buffer = ByteBuffer.allocate(2 + 2 * value.length)
    buffer.put(JsString.prefix)
    JsValue.encoder.encode(CharBuffer.wrap(value), buffer, true)
    getBytes(buffer)
  }
}

object JsString {
  val empty = JsString("")
  val prefix = "S=".getBytes(JsValue.charset)
  def apply(bytes: Array[Byte]): JsString = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsString(JsValue.decoder.decode(buffer).toString)
  }
}

case class JsBinary(value: Array[Byte]) extends JsValue {
  override def toString = value.map("%02X" format _).mkString
  override def bytes = value
}

object JsBinary {
  def apply(value: Array[Int]): JsBinary = {
    val bytes = new Array[Byte](4 * value.length)
    val buffer = ByteBuffer.wrap(bytes)
    value.foreach { i => buffer.putInt(i) }
    JsBinary(bytes)
  }
}

case class JsObject(fields: collection.mutable.Map[String, JsValue]) extends JsValue {
  override def toString = compactPrint
  override def bytes = {
    val keys = fields.keySet.mkString(",")
    val buffer = ByteBuffer.allocate(2 + 2 * keys.length)
    buffer.put(JsObject.prefix)
    JsValue.encoder.encode(CharBuffer.wrap(keys), buffer, true)
    getBytes(buffer)
  }

  def apply(key: String): JsValue = {
    if (fields.contains(key))
      fields(key)
    else
      JsUndefined
  }

  def selectDynamic(key: String): JsValue = {
    apply(key)
  }

  def remove(key: String): Option[JsValue] = {
    val value = fields.remove(key)
    value
  }

  def update(key: String, value: JsValue): JsObject = {
    fields(key) = value
    this
  }

  def updateDynamic(key: String)(value: Any) {
    import JsValueImplicits._
    value match {
      case value: String => update(key, value)
      case value: Int => update(key, value)
      case value: Double => update(key, value)
      case value: Boolean => update(key, value)
      case value: Long => update(key, value)
      case value: JsValue => update(key, value)
      case value: Array[String] => update(key, value)
      case value: Array[Int] => update(key, value)
      case value: Array[Double] => update(key, value)
      case value: Array[Boolean] => update(key, value)
      case value: Array[Long] => update(key, value)
      case value: Array[JsValue] => update(key, value)
      case Some(value: String) => update(key, value)
      case Some(value: Int) => update(key, value)
      case Some(value: Double) => update(key, value)
      case Some(value: Boolean) => update(key, value)
      case Some(value: Long) => update(key, value)
      case Some(value: JsValue) => update(key, value)
      case null | None => remove(key)
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
  }
}

object JsObject {
  def apply(fields: (String, JsValue)*) = new JsObject(collection.mutable.Map(fields: _*))
  def apply(map: Map[String, JsValue]) = new JsObject(collection.mutable.Map() ++ map)

  val prefix = "O=".getBytes(JsValue.charset)
  // returns the list of field names
  def apply(bytes: Array[Byte]): Array[String] = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    JsValue.decoder.decode(buffer).toString.split(",")
  }
}

case class JsArray(elements: collection.mutable.ArrayBuffer[JsValue]) extends JsValue {
  override def toString = compactPrint
  override def bytes = {
    val buffer = ByteBuffer.allocate(10)
    buffer.put(JsArray.prefix)
    buffer.putInt(elements.length)
    getBytes(buffer)
  }

  def apply(index: Int): JsValue = {
    elements(index)
  }

  def remove(index: Int): JsValue = {
    elements.remove(index)
  }

  def update(index: Int, value: JsValue): JsArray = {
    elements(index) = value
    this
  }

  def updateDynamic(index: Int)(value: Any) {
    import JsValueImplicits._
    value match {
      case value: String => update(index, value)
      case value: Int => update(index, value)
      case value: Double => update(index, value)
      case value: Boolean => update(index, value)
      case value: Long => update(index, value)
      case value: JsValue => update(index, value)
      case value: Array[String] => update(index, value)
      case value: Array[Int] => update(index, value)
      case value: Array[Double] => update(index, value)
      case value: Array[Boolean] => update(index, value)
      case value: Array[Long] => update(index, value)
      case value: Array[JsValue] => update(index, value)
      case Some(value: String) => update(index, value)
      case Some(value: Int) => update(index, value)
      case Some(value: Double) => update(index, value)
      case Some(value: Boolean) => update(index, value)
      case Some(value: Long) => update(index, value)
      case Some(value: JsValue) => update(index, value)
      case null | None => remove(index)
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
  }

  /**
   * Appends a single element to this array and returns
   * the identity of the array. It takes constant amortized time.
   *
   * @param value  the element to append.
   * @return      the updated array.
   */
  def +=(value: Any): JsArray = {
    import JsValueImplicits._
    value match {
      case value: String => elements += value
      case value: Int => elements += value
      case value: Double => elements += value
      case value: Boolean => elements += value
      case value: Long => elements += value
      case value: JsValue => elements += value
      case value: Array[String] => elements += value
      case value: Array[Int] => elements += value
      case value: Array[Double] => elements += value
      case value: Array[Boolean] => elements += value
      case value: Array[Long] => elements += value
      case value: Array[JsValue] => elements += value
      case Some(value: String) => elements += value
      case Some(value: Int) => elements += value
      case Some(value: Double) => elements += value
      case Some(value: Boolean) => elements += value
      case Some(value: Long) => elements += value
      case Some(value: JsValue) => elements += value
      case null => elements += JsNull
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
    this
  }
}

object JsArray {
  def apply(elements: JsValue*) = new JsArray(collection.mutable.ArrayBuffer(elements: _*))
  val prefix = "A=".getBytes(JsValue.charset)
  // return the array size
  def apply(bytes: Array[Byte]): Int = {
    val buffer = ByteBuffer.wrap(bytes, prefix.length, bytes.length - prefix.length)
    buffer.getInt
  }
}
