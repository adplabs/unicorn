package unicorn.json

import org.specs2.mutable._

class JsonPathSpec extends Specification {
  "JsonPath" should {

    "field" in {
      val jp = JsonPath(json)
      jp("$.id") === JsInt(1)
      jp("$['id']") === JsInt(1)
    }

    "recursive field" in {
      JsonPath(json)("$..id") === JsArray(2, 3, 4, 1)
    }

    "multi fields" in {
      JsonPath(json)("$['id', 'name']") === JsArray("Joe", 1)
    }

    "any field" in {
      val jp = JsonPath(json)
      jp("$.*") === JsArray(json.fields.map(_._2).toArray: _*)
      jp("$.tags.*") === JsArray(tags.map(JsString(_)): _*)
      jp("$['tags'].*") === JsArray(tags.map(JsString(_)): _*)
    }

    "recursive any" in {
      JsonPath(json)("$..*") === json
    }

    "array slices" in {
      val jp = JsonPath(json)
      tags.indices.foreach{ i =>
        jp("$.tags[" + i + ":]") === JsArray(tags.drop(i).map(JsString(_)): _*)
      }
      jp("$.tags[2]") === JsString("father")
      jp("$.tags[0:3:2]") === JsArray(JsString(tags(0)), JsString(tags(2)))
      jp("$.tags[-2:]") === JsArray(tags.takeRight(2).map(JsString(_)): _*)
      jp("$.tags[:-2]") === JsArray(tags.dropRight(2).map(JsString(_)): _*)
    }

    "array random" in {
      val jp = JsonPath(json)
      jp("$.tags[0,2]") === JsArray(JsString(tags(0)), JsString(tags(2)))
      jp("$.tags[-1]") === JsString(tags.last)
    }

    "array recursive" in {
      JsonPath(json)("$.address[*].city").asInstanceOf[JsArray].size === 3
    }

    "has filter" in {
      JsonPath(json)("$.address[?(@.work)]").asInstanceOf[JsArray].size === 1
    }

    "comparison filter" in {
      val jp = JsonPath(json)
      jp("$.address[?(@.id < 3)]").asInstanceOf[JsArray].size === 1
      jp("$.address[?(@.id <= 3)]").asInstanceOf[JsArray].size === 2

      jp("$.address[?(@.id > 2)]").asInstanceOf[JsArray].size === 2
      jp("$.address[?(@.id >= 2)]").asInstanceOf[JsArray].size === 3

      jp("$.address[?(@.state == 'PA')]").asInstanceOf[JsArray].size === 2
      jp("$.address[?(@.city == 'Springfield')]").asInstanceOf[JsArray].size === 1
      jp("$.address[?(@.city != 'Devon')]").asInstanceOf[JsArray].size === 2
    }

    "boolean filter" in {
      val jp = JsonPath(json)
      jp("$.address[?(@.id > 1 && @.state != 'PA')]").asInstanceOf[JsArray].size === 1
      jp("$.address[?(@.id < 4 && @.state == 'PA')]").asInstanceOf[JsArray].size === 2
      jp("$.address[?(@.id == 4 || @.state == 'PA')]").asInstanceOf[JsArray].size === 3
      jp("$.address[?(@.id == 4 || @.state == 'NJ')]").asInstanceOf[JsArray].size === 1
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
