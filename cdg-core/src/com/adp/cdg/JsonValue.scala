package com.adp.cdg

import java.util.Date

/**
 * JSON value. Note that JavaScript doesn't distinguish
 * integers and floats and doesn't have date type.
 */
abstract class JsonValue {
  /**
   * Converts this value to a byte array.
   */
  def bytes: Array[Byte]
}

case class JsonUndefinedValue extends JsonValue {
  override def toString = "undefined"
  override def bytes = throw new UnsupportedOperationException()
}

case class JsonBoolValue(value: Boolean) extends JsonValue {
  override def toString = value.toString
  override def bytes = (JsonBoolValue.prefix + value.toString).getBytes("UTF-8")
}

object JsonBoolValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "B="
  def apply(s: String): JsonBoolValue = JsonBoolValue(s.substring(prefix.length).toBoolean)
}

case class JsonDateValue(value: Date) extends JsonValue {
  override def toString = value.toString
  override def bytes = (JsonDateValue.prefix + value.getTime.toString).getBytes("UTF-8")
}

object JsonDateValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "T="
  def apply(s: String): JsonDateValue = JsonDateValue(new Date(s.substring(prefix.length).toLong))
}

case class JsonIntValue(value: Int) extends JsonValue {
  override def toString = value.toString
  override def bytes = (JsonIntValue.prefix + value.toString).getBytes("UTF-8")
}

object JsonIntValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "I="
  def apply(s: String): JsonIntValue = JsonIntValue(s.substring(prefix.length).toInt)
}

case class JsonDoubleValue(value: Double) extends JsonValue {
  override def toString = value.toString
  override def bytes = (JsonDoubleValue.prefix + value.toString).getBytes("UTF-8")
}

object JsonDoubleValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "D="
  def apply(s: String): JsonDoubleValue = JsonDoubleValue(s.substring(prefix.length).toDouble)
}

case class JsonStringValue(value: String) extends JsonValue {
  override def toString = "\"" + value.replace("\"", "\\\"") + "\""
  override def bytes = (JsonStringValue.prefix + value).getBytes("UTF-8")
}

object JsonStringValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "S="
  def valueOf(s: String) = JsonStringValue(s.substring(prefix.length))
}

case class JsonObjectValue(value: collection.mutable.Map[String, JsonValue]) extends JsonValue{
  private val DefaultIndent = "  "
  private val DefaultSeparator = ", "
  override def toString() = toString("", DefaultSeparator)
  
  def toString(indent: String, separator: String): String = {
    val moreIndent = indent + "  "
    "\n" + indent + "{\n" +
    (value.map { case (key, value) => moreIndent + key + ": " + 
      (value match {
        case obj: JsonObjectValue => obj.toString(moreIndent + DefaultIndent, separator)
        case _ => value.toString }
      )}
    ).mkString(separator) +
    "\n" + indent + "}"
  }

  override def bytes = (JsonObjectValue.prefix + value.keySet.mkString(",")).getBytes("UTF-8")
}

object JsonObjectValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "O="
  def apply(s: String): Array[String] = s.split(",")
}

case class JsonArrayValue(value: Array[JsonValue]) extends JsonValue {
  override def toString = "[" + value.deep.mkString(", ") + "]"
  override def bytes = (JsonArrayValue.prefix + value.length).getBytes("UTF-8")
}

object JsonArrayValue {
  /** Prefix of byte value. Must be size of 2 and be unique for different value types. */
  val prefix = "A="
  def apply(s: String): Int = s.substring(prefix.length).toInt
}
