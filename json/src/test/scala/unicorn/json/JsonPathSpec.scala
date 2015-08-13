package unicorn.json

import org.specs2.mutable._

class JsonPathSpec extends Specification {
  "JsonPath" should {

    "field" in {
      JsonPath.query("$.id", json) === JsInt(1)
      JsonPath.query("$['id']", json) === JsInt(1)
    }

    "recursive field" in {
      JsonPath.query("$..id", json) === JsArray(2, 3, 4, 1)
    }

    "multi fields" in {
      JsonPath.query("$['id', 'name']", json) === JsArray("Joe", 1)
    }

    "any field" in {
      JsonPath.query("$.*", json) === JsArray(json.fields.map(_._2).toArray: _*)
      JsonPath.query("$.tags.*", json) === JsArray(tags.map(JsString(_)): _*)
      JsonPath.query("$['tags'].*", json) === JsArray(tags.map(JsString(_)): _*)
    }

    "recursive any" in {
      JsonPath.query("$..*", json) === json
    }

    "array slices" in {
      tags.indices.foreach{ i =>
        JsonPath.query("$.tags[" + i + ":]", json) === JsArray(tags.drop(i).map(JsString(_)): _*)
      }
      JsonPath.query("$.tags[2]", json) === JsString("father")
      JsonPath.query("$.tags[0:3:2]", json) === JsArray(JsString(tags(0)), JsString(tags(2)))
      JsonPath.query("$.tags[-2:]", json) === JsArray(tags.takeRight(2).map(JsString(_)): _*)
      JsonPath.query("$.tags[:-2]", json) === JsArray(tags.dropRight(2).map(JsString(_)): _*)
    }

    "array random" in {
      JsonPath.query("$.tags[0,2]", json) === JsArray(JsString(tags(0)), JsString(tags(2)))
      JsonPath.query("$.tags[-1]", json) === JsString(tags.last)
    }

    "array recursive" in {
      JsonPath.query("$.address[*].city", json).asInstanceOf[JsArray].size === 3
    }

    "has filter" in {
      println(JsonPath.query("$.address[?(@.work)]", json))
      JsonPath.query("$.address[?(@.work)]", json).asInstanceOf[JsArray].size === 1
    }

    "comparison filter" in {
      JsonPath.query("$.address[?(@.id < 3)]", json).asInstanceOf[JsArray].size === 1
      JsonPath.query("$.address[?(@.id <= 3)]", json).asInstanceOf[JsArray].size === 2

      JsonPath.query("$.address[?(@.id > 2)]", json).asInstanceOf[JsArray].size === 2
      JsonPath.query("$.address[?(@.id >= 2)]", json).asInstanceOf[JsArray].size === 3

      JsonPath.query("$.address[?(@.state == 'PA')]", json).asInstanceOf[JsArray].size === 2
      JsonPath.query("$.address[?(@.city == 'Springfield')]", json).asInstanceOf[JsArray].size === 1
      JsonPath.query("$.address[?(@.city != 'Devon')]", json).asInstanceOf[JsArray].size === 2
    }

    "boolean filter" in {
      JsonPath.query("$.address[?(@.id > 1 && @.state != 'PA')]", json).asInstanceOf[JsArray].size === 1
      JsonPath.query("$.address[?(@.id < 4 && @.state == 'PA')]", json).asInstanceOf[JsArray].size === 2
      JsonPath.query("$.address[?(@.id == 4 || @.state == 'PA')]", json).asInstanceOf[JsArray].size === 3
      JsonPath.query("$.address[?(@.id == 4 || @.state == 'NJ')]", json).asInstanceOf[JsArray].size === 1
    }
  }

  lazy val ids = Seq(1,2,3,4)
  lazy val tags = Seq("programmer", "husband", "father", "golfer")
  lazy val json = JsonParser(testJsonStr).asInstanceOf[JsObject]
  val testJsonStr =
    """
      |{
      | "id": 1,
      | "name": "Joe",
      | "tags": ["programmer", "husband", "father", "golfer"],
      | "address": [
      | {
      |   "id": 2,
      |   "street": "123 Main St.",
      |   "city": "Springfield",
      |   "state": "PA"
      | },
      | {
      |   "id": 3,
      |   "street": "456 Main St.",
      |   "city": "Devon",
      |   "state": "PA",
      |   "work": true
      | },
      | {
      |   "id": 4,
      |   "street": "789 Main St.",
      |   "city": "Sea Isle City",
      |   "state": "NJ"
      | }
      | ]
      |}
    """.stripMargin
}
