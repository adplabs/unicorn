package unicorn.json

import org.specs2.mutable._

/**
 * Created by lihb on 8/4/15.
 */
class JsonSerializerSpec extends Specification {

  "The BsonSerializer" should {
    "serialize JsNull" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsNull)) === JsNull
    }
    "serialize JsTrue" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsTrue)) === JsTrue
    }
    "serialize JsFalse" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsFalse)) === JsFalse
    }
    "serialize 0" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsInt.zero)) === JsInt.zero
    }
    "serialize  '1.23'" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsDouble(1.23))) === JsDouble(1.23)
    }
    "serialize \"xyz\"" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsString("xyz"))) === JsString("xyz")
    }
    "serialize escapes in a JsString" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsString("\"\\/\b\f\n\r\t"))) === JsString("\"\\/\b\f\n\r\t")
      serializer.deserialize(serializer.serialize(JsString("Länder"))) === JsString("Länder")
    }
    "serialize '1302806349000'" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsLong(1302806349000L))) === JsLong(1302806349000L)
    }
    "serialize '2015-08-10T10:00:00.123Z'" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsDate("2015-08-10T10:00:00.123Z"))) === JsDate("2015-08-10T10:00:00.123Z")
    }
    "serialize 'CA761232-ED42-11CE-BACD-00AA0057B223'" in {
      val serializer = new BsonSerializer
      serializer.deserialize(serializer.serialize(JsUUID("CA761232-ED42-11CE-BACD-00AA0057B223"))) === JsUUID("CA761232-ED42-11CE-BACD-00AA0057B223")
    }
    "serialize test.json" in {
      val serializer = new BsonSerializer
      val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/test.json")).mkString
      val json = JsonParser(jsonSource)
      val bson = serializer.serialize(json)
      bson.size === 1
      bson.isDefinedAt("$") ==== true
      serializer.deserialize(bson, "$") === json
    }
  }

  "The ColumnarJsonSerializer" should {
    "serialize JsNull" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsNull)) === JsNull
    }
    "serialize JsTrue" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsTrue)) === JsTrue
    }
    "serialize JsFalse" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsFalse)) === JsFalse
    }
    "serialize 0" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsInt.zero)) === JsInt.zero
    }
    "serialize  '1.23'" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsDouble(1.23))) === JsDouble(1.23)
    }
    "serialize \"xyz\"" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsString("xyz"))) === JsString("xyz")
    }
    "serialize escapes in a JsString" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsString("\"\\/\b\f\n\r\t"))) === JsString("\"\\/\b\f\n\r\t")
      serializer.deserialize(serializer.serialize(JsString("Länder"))) === JsString("Länder")
    }
    "serialize '1302806349000'" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsLong(1302806349000L))) === JsLong(1302806349000L)
    }
    "serialize '2015-08-10T10:00:00.123Z'" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsDate("2015-08-10T10:00:00.123Z"))) === JsDate("2015-08-10T10:00:00.123Z")
    }
    "serialize 'CA761232-ED42-11CE-BACD-00AA0057B223'" in {
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(JsUUID("CA761232-ED42-11CE-BACD-00AA0057B223"))) === JsUUID("CA761232-ED42-11CE-BACD-00AA0057B223")
    }
    "serialize test.json" in {
      val serializer = new ColumnarJsonSerializer
      val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/test.json")).mkString
      val json = JsonParser(jsonSource)
      serializer.deserialize(serializer.serialize(json), "$") === json
    }
  }
}
