/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.json

import java.text.SimpleDateFormat
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

  def apply(index: Int): JsValue = {
    throw new UnsupportedOperationException
  }

  def applyDynamic(key: String): JsValue = {
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
  def apply(b: Byte) = if (b != 0) JsTrue else JsFalse
  def apply(b: Int)  = if (b != 0) JsTrue else JsFalse
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
  val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val formatLength = "yyyy-MM-ddTHH:mm:ss.SSSZ".length
  def apply(date: Long) = new JsDate(new Date(date))
  def apply(date: String) = new JsDate(format.parse(date))
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

  override def applyDynamic(key: String): JsValue = apply(key)

  override def selectDynamic(key: String): JsValue = apply(key)

  override def remove(key: String): Option[JsValue] = fields.remove(key)

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

case class JsArray(elements: collection.mutable.ArrayBuffer[JsValue]) extends JsValue with TraversableOnce[JsValue] {
  override def copyToArray[B >: JsValue](xs: Array[B], start: Int, len: Int): Unit = elements.copyToArray(xs, start, len)
  override def find(p: (JsValue) => Boolean): Option[JsValue] = elements.find(p)
  override def exists(p: (JsValue) => Boolean): Boolean = elements.exists(p)
  override def forall(p: (JsValue) => Boolean): Boolean = elements.forall(p)
  override def foreach[U](p: (JsValue) => U): Unit = elements.foreach(p)
  override def hasDefiniteSize: Boolean = elements.hasDefiniteSize
  override def isEmpty: Boolean = elements.isEmpty
  override def isTraversableAgain: Boolean = elements.isTraversableAgain
  override def seq: Traversable[JsValue] = elements.seq
  override def toIterator: Iterator[JsValue] = elements.toIterator
  override def toStream: Stream[JsValue] = elements.toStream
  override def toTraversable: Traversable[JsValue] = elements.toTraversable

  override def apply(index: Int): JsValue = elements(index)

  override def remove(index: Int): JsValue = elements.remove(index)

  override def update(index: Int, value: JsValue): JsValue = {
    elements(index) = value
    value
  }

  override def updateDynamic(index: Int)(value: JsValue): JsValue = update(index, value)

  /** Appends a single element to this array and returns
   * the identity of the array. It takes constant amortized time.
   *
   * @param elem  the element to append.
   * @return      the updated array.
   */
  def +=(elem: JsValue): JsArray = {
    elements += elem
    this
  }

  /** Appends a number of elements provided by a traversable object.
   *  The identity of the array is returned.
   *
   *  @param xs    the traversable object.
   *  @return      the updated buffer.
   */
  def ++=(xs: TraversableOnce[JsValue]): JsValue = {
    elements ++= xs
    this
  }

  /** Prepends a single element to this buffer and returns
   *  the identity of the array. It takes time linear in
   *  the buffer size.
   *
   *  @param elem  the element to prepend.
   *  @return      the updated array.
   */
  def +=:(elem: JsValue): JsValue = {
    elem +=: elements
    this
  }

  /** Prepends a number of elements provided by a traversable object.
   *  The identity of the array is returned.
   *
   *  @param xs    the traversable object.
   *  @return      the updated array.
   */
  def ++=:(xs: TraversableOnce[JsValue]): JsValue = {
    xs ++=: elements
    this
  }

  /** Inserts new elements at the index `n`. Opposed to method
   *  `update`, this method will not replace an element with a new
   *  one. Instead, it will insert a new element at index `n`.
   *
   *  @param n     the index where a new element will be inserted.
   *  @param seq   the traversable object providing all elements to insert.
   *  @throws IndexOutOfBoundsException if `n` is out of bounds.
   */
  def insertAll(n: Int, seq: Traversable[JsValue]) {
    elements.insertAll(n, seq)
  }

  /** Removes the element on a given index position. It takes time linear in
   *  the buffer size.
   *
   *  @param n       the index which refers to the first element to delete.
   *  @param count   the number of elements to delete
   *  @throws IndexOutOfBoundsException if `n` is out of bounds.
   */
  def remove(n: Int, count: Int) {
    elements.remove(n, count)
  }
}

object JsArray {
  def apply(elements: JsValue*) = new JsArray(collection.mutable.ArrayBuffer(elements: _*))
}
