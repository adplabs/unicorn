package unicorn.json

import org.specs2.mutable._

/**
 * Created by lihb on 8/4/15.
 */
class BsonSerializerSpec extends Specification {

  "The BsonSerializer" should {
    "serialize test.json" in {
      val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/test.json")).mkString
      val json = JsonParser(jsonSource)
      val serializer = new BsonSerializer
      val bson = serializer.serialize(json)
      //println(json("questions")(3))
      //println(JsBinary(bson(0)._2))
      //println(JsString(new String(bson(0)._2)))
      bson.size === 1
      bson(0)._1 === "$"
      val deserialized = serializer.deserialize(Map(bson: _*), "$")
      deserialized === json
    }
  }
}
