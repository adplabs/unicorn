/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.json

import scala.language.dynamics
import scala.language.implicitConversions

/**
 * JSON value.
 *
 * @author Haifeng Li
 */
sealed abstract class JsValue extends Dynamic {
  override def toString = compactPrint
  def compactPrint = CompactPrinter(this)
  def prettyPrint = PrettyPrinter(this)

  def apply(key: String): JsValue = {
    throw new UnsupportedOperationException
  }

  def apply(index: Int): JsValue = {
    throw new UnsupportedOperationException
  }

  def selectDynamic(key: String): JsValue = {
    throw new UnsupportedOperationException
  }

  def remove(key: String): Option[JsValue] = {
    throw new UnsupportedOperationException
  }

  def remove(index: Int): JsValue = {
    throw new UnsupportedOperationException
  }

  def update(key: String, value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def update(index: Int, value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def updateDynamic(key: String)(value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def updateDynamic(index: Int)(value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def +=(value: JsValue): JsArray = {
    throw new UnsupportedOperationException
  }
}

case object JsNull extends JsValue {
  override def toString = "null"
}

case object JsUndefined extends JsValue {
  override def toString = "undefined"
}

case class JsBoolean(value: Boolean) extends JsValue {
  override def toString = value.toString
}

object JsBoolean {
  def apply(b: Byte) = new JsBoolean(b != 0)
  def apply(b: Int)  = new JsBoolean(b != 0)
}

case class JsInt(value: Int) extends JsValue {
  override def toString = value.toString
}

object JsInt {
  val zero = JsInt(0)
}

case class JsLong(value: Long) extends JsValue {
  override def toString = value.toString
}

object JsLong {
  val zero = JsLong(0L)
}

case class JsDouble(value: Double) extends JsValue {
  override def toString = value.toString
}

object JsDouble {
  val zero = JsDouble(0.0)
}

case class JsString(value: String) extends JsValue {
  override def toString = value.toString
}

object JsString {
  val empty = JsString("")
}

case class JsBinary(value: Array[Byte]) extends JsValue {
  override def toString = value.map("%02X" format _).mkString
}

case class JsObject(fields: collection.mutable.Map[String, JsValue]) extends JsValue {
  override def apply(key: String): JsValue = {
    if (fields.contains(key))
      fields(key)
    else
      JsUndefined
  }

  override def selectDynamic(key: String): JsValue = {
    apply(key)
  }

  override def remove(key: String): Option[JsValue] = {
    fields.remove(key)
  }

  override def update(key: String, value: JsValue): JsValue = {
    fields(key) = value
    value
  }

  def updateDynamic(key: String)(value: Int): JsValue = update(key, value)
  def updateDynamic(key: String)(value: Long): JsValue = update(key, value)
  def updateDynamic(key: String)(value: String): JsValue = update(key, value)
  /*override def updateDynamic(key: String)(value: JsValue): JsValue = update(key, value) value match {
    case null => update(key, JsNull)
    case JsUndefined | None => remove(key).getOrElse(JsUndefined)

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
/*
    case value: Seq[String] => update(key, value)
    case value: Seq[Int] => update(key, value)
    case value: Seq[Double] => update(key, value)
    case value: Seq[Boolean] => update(key, value)
    case value: Seq[Long] => update(key, value)
    case value: Seq[JsValue] => update(key, value)
*/
    case Some(value: String) => update(key, value)
    case Some(value: Int) => update(key, value)
    case Some(value: Double) => update(key, value)
    case Some(value: Boolean) => update(key, value)
    case Some(value: Long) => update(key, value)
    case Some(value: JsValue) => update(key, value)

    case Some(value: Array[String]) => update(key, value)
    case Some(value: Array[Int]) => update(key, value)
    case Some(value: Array[Double]) => update(key, value)
    case Some(value: Array[Boolean]) => update(key, value)
    case Some(value: Array[Long]) => update(key, value)
    case Some(value: Array[JsValue]) => update(key, value)
/*
    case Some(value: Seq[String]) => update(key, value)
    case Some(value: Seq[Int]) => update(key, value)
    case Some(value: Seq[Double]) => update(key, value)
    case Some(value: Seq[Boolean]) => update(key, value)
    case Some(value: Seq[Long]) => update(key, value)
    case Some(value: Seq[JsValue]) => update(key, value)
*/
    case _ => throw new IllegalArgumentException("Unsupport JSON value type")
  }
  */
}

object JsObject {
  def apply(field: (String, JsValue)) = new JsObject(collection.mutable.Map(field))
  def apply(fields: (String, JsValue)*) = new JsObject(collection.mutable.Map(fields: _*))
  def apply(map: Map[String, JsValue]) = new JsObject(collection.mutable.Map() ++ map)
}

case class JsArray(elements: collection.mutable.ArrayBuffer[JsValue]) extends JsValue {
  override def apply(index: Int): JsValue = {
    elements(index)
  }

  override def remove(index: Int): JsValue = {
    elements.remove(index)
  }

  override def update(index: Int, value: JsValue): JsValue = {
    elements(index) = value
    value
  }

  override def updateDynamic(index: Int)(value: JsValue): JsValue = value match {
    case null => update(index, JsNull)
    case JsUndefined | None => remove(index)

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
    /*
        case value: Seq[String] => update(key, value)
        case value: Seq[Int] => update(key, value)
        case value: Seq[Double] => update(key, value)
        case value: Seq[Boolean] => update(key, value)
        case value: Seq[Long] => update(key, value)
        case value: Seq[JsValue] => update(key, value)
    */
    case Some(value: String) => update(index, value)
    case Some(value: Int) => update(index, value)
    case Some(value: Double) => update(index, value)
    case Some(value: Boolean) => update(index, value)
    case Some(value: Long) => update(index, value)
    case Some(value: JsValue) => update(index, value)

    case Some(value: Array[String]) => update(index, value)
    case Some(value: Array[Int]) => update(index, value)
    case Some(value: Array[Double]) => update(index, value)
    case Some(value: Array[Boolean]) => update(index, value)
    case Some(value: Array[Long]) => update(index, value)
    case Some(value: Array[JsValue]) => update(index, value)
    /*
        case Some(value: Seq[String]) => update(key, value)
        case Some(value: Seq[Int]) => update(key, value)
        case Some(value: Seq[Double]) => update(key, value)
        case Some(value: Seq[Boolean]) => update(key, value)
        case Some(value: Seq[Long]) => update(key, value)
        case Some(value: Seq[JsValue]) => update(key, value)
    */
    case _ => throw new IllegalArgumentException("Unsupport JSON value type")
  }

  /**
   * Appends a single element to this array and returns
   * the identity of the array. It takes constant amortized time.
   *
   * @param value  the element to append.
   * @return      the updated array.
   */
  override def +=(value: JsValue): JsArray = {
    elements += value
    this
  }
}

object JsArray {
  def apply(elements: JsValue*) = new JsArray(collection.mutable.ArrayBuffer(elements: _*))
}
