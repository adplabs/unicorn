package unicorn.json

import org.specs2.mutable._

/**
 * Created by lihb on 8/4/15.
 */
class JsonSerializerSpec extends Specification {

  "The BsonSerializer" should {
    "serialize test.json" in {
      val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/test.json")).mkString
      val json = JsonParser(jsonSource)
      val serializer = new BsonSerializer
      val bson = serializer.serialize(json)
      bson.size === 1
      bson.isDefinedAt("$") ==== true
      serializer.deserialize(bson, "$") === json
    }
  }

  "The ColumnarJsonSerializer" should {
    "serialize test.json" in {
      val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/test.json")).mkString
      val json = JsonParser(jsonSource)
      val serializer = new ColumnarJsonSerializer
      serializer.deserialize(serializer.serialize(json), "$") === json
    }
  }
}
