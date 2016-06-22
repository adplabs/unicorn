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
import unicorn.oid.BsonObjectId

/**
 * @author Haifeng Li
 */
package object json {
  type JsTopLevel = Either[JsObject, JsArray]

  val JsTrue = new JsBoolean(true)
  val JsFalse = new JsBoolean(false)

  implicit class JsonHelper(private val sc: StringContext) extends AnyVal {
    def json(args: Any*): JsObject = {
      JsonParser(sc.s(args: _*).stripMargin).asInstanceOf[JsObject]
    }
  }

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
  implicit def objectId2JsValue(x: BsonObjectId) = JsObjectId(x)
  implicit def byteArray2JsValue(x: Array[Byte]) = JsBinary(x)

  implicit def array2JsValue(x: Array[JsValue]) = JsArray(x: _*)
  implicit def seq2JsValue(x: Seq[JsValue]) = JsArray(x: _*)
  implicit def map2JsValue(x: Seq[(String, JsValue)]) = JsObject(x: _*)
  implicit def map2JsValue(x: collection.mutable.Map[String, JsValue]) = JsObject(x)
  implicit def map2JsValue(x: collection.immutable.Map[String, JsValue]) = JsObject(x)

  implicit def pimpBooleanSeq(x: Seq[Boolean]) = new PimpedBooleanSeq(x)
  implicit def pimpIntSeq(x: Seq[Int]) = new PimpedIntSeq(x)
  implicit def pimpLongSeq(x: Seq[Long]) = new PimpedLongSeq(x)
  implicit def pimpDoubleSeq(x: Seq[Double]) = new PimpedDoubleSeq(x)
  implicit def pimpStringSeq(x: Seq[String]) = new PimpedStringSeq(x)
  implicit def pimpDateSeq(x: Seq[Date]) = new PimpedDateSeq(x)

  implicit def pimpBooleanArray(x: Array[Boolean]) = new PimpedBooleanSeq(x)
  implicit def pimpIntArray(x: Array[Int]) = new PimpedIntSeq(x)
  implicit def pimpLongArray(x: Array[Long]) = new PimpedLongSeq(x)
  implicit def pimpDoubleArray(x: Array[Double]) = new PimpedDoubleSeq(x)
  implicit def pimpStringArray(x: Array[String]) = new PimpedStringSeq(x)
  implicit def pimpDateArray(x: Array[Date]) = new PimpedDateSeq(x)

  implicit def pimpBooleanMap(x: Map[String, Boolean]) = new PimpedBooleanMap(x)
  implicit def pimpIntMap(x: Map[String, Int]) = new PimpedIntMap(x)
  implicit def pimpLongMap(x: Map[String, Long]) = new PimpedLongMap(x)
  implicit def pimpDoubleMap(x: Map[String, Double]) = new PimpedDoubleMap(x)
  implicit def pimpStringMap(x: Map[String, String]) = new PimpedStringMap(x)
  implicit def pimpDateMap(x: Map[String, Date]) = new PimpedDateMap(x)

  implicit def pimpBooleanMutableMap(x: collection.mutable.Map[String, Boolean]) = new PimpedBooleanMutableMap(x)
  implicit def pimpIntMutableMap(x: collection.mutable.Map[String, Int]) = new PimpedIntMutableMap(x)
  implicit def pimpLongMutableMap(x: collection.mutable.Map[String, Long]) = new PimpedLongMutableMap(x)
  implicit def pimpDoubleMutableMap(x: collection.mutable.Map[String, Double]) = new PimpedDoubleMutableMap(x)
  implicit def pimpStringMutableMap(x: collection.mutable.Map[String, String]) = new PimpedStringMutableMap(x)
  implicit def pimpDateMutableMap(x: collection.mutable.Map[String, Date]) = new PimpedDateMutableMap(x)

  implicit def json2Boolean(x: JsBoolean) = x.value
  implicit def json2Int(x: JsInt) = x.value
  implicit def json2Long(x: JsLong) = x.value
  implicit def json2Double(x: JsDouble) = x.value
  implicit def json2String(x: JsString) = x.value
  implicit def json2Date(x: JsDate) = x.value
  implicit def json2UUID(x: JsUUID) = x.value
  implicit def json2Binary(x: JsBinary) = x.value

  implicit def json2Boolean(json: JsValue): Boolean = json.asBoolean
  implicit def json2Int(json: JsValue): Int = json.asInt
  implicit def json2Long(json: JsValue): Long = json.asLong
  implicit def json2Double(json: JsValue): Double = json.asDouble
  implicit def json2Date(json: JsValue): Date = json.asDate
  implicit def json2String(json: JsValue): String = json.toString
  implicit def json2ByteArray(json: JsValue): Array[Byte] = json match {
    case JsBinary(x) => x
    case _ => throw new UnsupportedOperationException("convert JsValue to Array[Byte]")
  }
}

package json {

  private[json] class PimpedString(string: String) {
    def parseJson: JsValue = JsonParser(string)
    def parseJsObject: JsObject = parseJson.asInstanceOf[JsObject]
  }

  private[json] class PimpedBooleanSeq(seq: Seq[Boolean]) {
    def toJsArray: JsArray = JsArray(seq.map {e => JsBoolean(e)}: _*)
  }

  private[json] class PimpedIntSeq(seq: Seq[Int]) {
    def toJsArray: JsArray = JsArray(seq.map {e => JsInt(e)}: _*)
  }

  private[json] class PimpedLongSeq(seq: Seq[Long]) {
    def toJsArray: JsArray = JsArray(seq.map {e => JsLong(e)}: _*)
  }

  private[json] class PimpedDoubleSeq(seq: Seq[Double]) {
    def toJsArray: JsArray = JsArray(seq.map {e => JsDouble(e)}: _*)
  }

  private[json] class PimpedStringSeq(seq: Seq[String]) {
    def toJsArray: JsArray = JsArray(seq.map {e => JsString(e)}: _*)
  }

  private[json] class PimpedDateSeq(seq: Seq[Date]) {
    def toJsArray: JsArray = JsArray(seq.map {e => JsDate(e)}: _*)
  }

  private[json] class PimpedBooleanMap(map: Map[String, Boolean]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) => (k, JsBoolean(v)) })
  }

  private[json] class PimpedIntMap(map: Map[String, Int]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) => (k, JsInt(v)) })
  }

  private[json] class PimpedLongMap(map: Map[String, Long]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) => (k, JsLong(v)) })
  }

  private[json] class PimpedDoubleMap(map: Map[String, Double]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) => (k, JsDouble(v)) })
  }

  private[json] class PimpedStringMap(map: Map[String, String]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) => (k, JsString(v)) })
  }

  private[json] class PimpedDateMap(map: Map[String, Date]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) => (k, JsDate(v)) })
  }

  private[json] class PimpedBooleanMutableMap(map: collection.mutable.Map[String, Boolean]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) =>
      val js: JsValue = JsBoolean(v)
      (k, js)
    })
  }

  private[json] class PimpedIntMutableMap(map: collection.mutable.Map[String, Int]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) =>
      val js: JsValue = JsInt(v)
      (k, js)
    })
  }

  private[json] class PimpedLongMutableMap(map: collection.mutable.Map[String, Long]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) =>
      val js: JsValue = JsLong(v)
      (k, js)
    })
  }

  private[json] class PimpedDoubleMutableMap(map: collection.mutable.Map[String, Double]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) =>
      val js: JsValue = JsDouble(v)
      (k, js)
    })
  }

  private[json] class PimpedStringMutableMap(map: collection.mutable.Map[String, String]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) =>
      val js: JsValue = JsString(v)
      (k, js)
    })
  }

  private[json] class PimpedDateMutableMap(map: collection.mutable.Map[String, Date]) {
    def toJsObject: JsObject = JsObject(map.map { case (k, v) =>
      val js: JsValue = JsDate(v)
      (k, js)
    })
  }
}
