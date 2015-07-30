/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.json

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date
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

  def applyDynamic(key: String): JsValue = {
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
/*
  def +=(value: JsValue): JsArray = {
    throw new UnsupportedOperationException
  }
  */
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

case class JsDate(value: Date) extends JsValue {
  override def toString = JsDate.format.format(value)
}

object JsDate {
  def apply(date: Long) = new JsDate(new Date(date))
  val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
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

  override def applyDynamic(key: String): JsValue = {
    apply(key)
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

  override def updateDynamic(key: String)(value: JsValue): JsValue = update(key, value)
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

  override def updateDynamic(index: Int)(value: JsValue): JsValue = update(index, value)
  /**
   * Appends a single element to this array and returns
   * the identity of the array. It takes constant amortized time.
   *
   * @param value  the element to append.
   * @return      the updated array.
   */
  /*
  override def +=(value: JsValue): JsArray = {
    elements += value
    this
  }
  */
}

object JsArray {
  def apply(elements: JsValue*) = new JsArray(collection.mutable.ArrayBuffer(elements: _*))
}
