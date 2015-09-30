/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn

import java.util.{Date, UUID}

/**
 * @author Haifeng Li
 */
package object json {
  type JsTopLevel = Either[JsObject, JsArray]

  val JsTrue = new JsBoolean(true)
  val JsFalse = new JsBoolean(false)

  implicit def jsObjectTopLevel(x: JsObject) = Left(x)
  implicit def jsArrayTopLevel(x: JsArray) = Right(x)
  implicit def pimpString(string: String) = new PimpedString(string)

  implicit def boolean2JsValue(x: Boolean) = JsBoolean(x)
  implicit def int2JsValue(x: Int) = JsInt(x)
  implicit def long2JsValue(x: Long) = JsLong(x)
  implicit def double2JsValue(x: Double) = JsDouble(x)
  implicit def string2JsValue(x: String) = JsString(x)
  implicit def date2JsValue(x: Date) = JsDate(x)
  implicit def uuid2JsValue(x: UUID) = JsUUID(x)
  implicit def byteArray2JsValue(x: Array[Byte]) = JsBinary(x)

  implicit def array2JsValue(x: Array[JsValue]) = JsArray(x: _*)
  implicit def seq2JsValue(x: Seq[JsValue]) = JsArray(x: _*)
  implicit def map2JsValue(x: Seq[(String, JsValue)]) = JsObject(x: _*)
  implicit def map2JsValue(x: collection.mutable.Map[String, JsValue]) = JsObject(x)
  implicit def map2JsValue(x: collection.immutable.Map[String, JsValue]) = JsObject(x)

  implicit def booleanArray2JsValue(x: Array[Boolean]): JsArray = JsArray(x.map {e => JsBoolean(e)}: _*)
  implicit def intArray2JsValue(x: Array[Int]): JsArray = JsArray(x.map {e => JsInt(e)}: _*)
  implicit def longArray2JsValue(x: Array[Long]): JsArray = JsArray(x.map {e => JsLong(e)}: _*)
  implicit def doubleArray2JsValue(x: Array[Double]): JsArray = JsArray(x.map {e => JsDouble(e)}: _*)
  implicit def stringArray2JsValue(x: Array[String]): JsArray = JsArray(x.map {e => JsString(e)}: _*)
  implicit def dateArray2JsValue(x: Array[Date]): JsArray = JsArray(x.map {e => JsDate(e)}: _*)
  implicit def uuidArray2JsValue(x: Array[UUID]): JsArray = JsArray(x.map {e => JsUUID(e)}: _*)
  implicit def binaryArray2JsValue(x: Array[Array[Byte]]): JsArray = JsArray(x.map {e => JsBinary(e)}: _*)

  // Implicit conversion applies only once. Since Array has an implicit conversion to Seq, we duplicate
  // the definition for Seq again.
  implicit def booleanSeq2JsValue(x: Seq[Boolean]): JsArray = JsArray(x.map {e => JsBoolean(e)}: _*)
  implicit def intSeq2JsValue(x: Seq[Int]): JsArray = JsArray(x.map {e => JsInt(e)}: _*)
  implicit def longSeq2JsValue(x: Seq[Long]): JsArray = JsArray(x.map {e => JsLong(e)}: _*)
  implicit def doubleSeq2JsValue(x: Seq[Double]): JsArray = JsArray(x.map {e => JsDouble(e)}: _*)
  implicit def stringSeq2JsValue(x: Seq[String]): JsArray = JsArray(x.map {e => JsString(e)}: _*)
  implicit def dateSeq2JsValue(x: Seq[Date]): JsArray = JsArray(x.map {e => JsDate(e)}: _*)
  implicit def uuidSeq2JsValue(x: Seq[UUID]): JsArray = JsArray(x.map {e => JsUUID(e)}: _*)
  implicit def binarySeq2JsValue(x: Seq[Array[Byte]]): JsArray = JsArray(x.map {e => JsBinary(e)}: _*)

  implicit def json2Boolean(x: JsBoolean) = x.value
  implicit def json2Int(x: JsInt) = x.value
  implicit def json2Long(x: JsLong) = x.value
  implicit def json2Double(x: JsDouble) = x.value
  implicit def json2String(x: JsString) = x.value
  implicit def json2Date(x: JsDate) = x.value
  implicit def json2UUID(x: JsUUID) = x.value
  implicit def json2Binary(x: JsBinary) = x.value

  implicit def json2Boolean(json: JsValue): Boolean = json match {
    case JsBoolean(x) => x
    case JsInt(x) => x != 0
    case JsLong(x) => x != 0
    case JsDouble(x) => x != 0
    case JsString(x) => !x.isEmpty
    case JsNull => false
    case JsUndefined => false
    case _ => throw new UnsupportedOperationException("convert JsValue to boolean")
  }

  implicit def json2Int(json: JsValue): Int = json match {
    case JsBoolean(x) => if (x) 1 else 0
    case JsInt(x) => x
    case JsLong(x) => x.toInt
    case JsDouble(x) => x.toInt
    case JsString(x) => x.toInt
    case JsNull => 0
    case JsUndefined => 0
    case _ => throw new UnsupportedOperationException("convert JsValue to int")
  }

  implicit def json2Long(json: JsValue): Long = json match {
    case JsBoolean(x) => if (x) 1L else 0L
    case JsInt(x) => x
    case JsLong(x) => x
    case JsDate(x) => x.getTime
    case JsDouble(x) => x.toLong
    case JsString(x) => x.toLong
    case JsNull => 0L
    case JsUndefined => 0L
    case _ => throw new UnsupportedOperationException("convert JsValue to long")
  }

  implicit def json2Double(json: JsValue): Double = json match {
    case JsBoolean(x) => if (x) 1.0 else 0.0
    case JsInt(x) => x
    case JsLong(x) => x
    case JsDouble(x) => x
    case JsString(x) => x.toDouble
    case JsNull => 0.0
    case JsUndefined => 0.0
    case _ => throw new UnsupportedOperationException("convert JsValue to double")
  }

  implicit def json2String(json: JsValue): String = json.toString

  implicit def json2ByteArray(json: JsValue): Array[Byte] = json match {
    case JsBinary(x) => x
    case _ => throw new UnsupportedOperationException("convert JsValue to Array[Byte]")
  }
}

package json {
  private[json] class PimpedString(string: String) {
    def parseJson: JsValue = JsonParser(string)
  }
}
