package unicorn.json

import scala.collection.immutable.ListMap
import org.specs2.mutable._

class PrettyPrinterSpec extends Specification {

  "The PrettyPrinter" should {
    "print a more complicated JsObject nicely aligned" in {
      val json = JsonParser {
        """{
          |  "Boolean no": false,
          |  "Boolean yes":true,
          |  "Unic\u00f8de" :  "Long string with newline\nescape",
          |  "key with \"quotes\"" : "string",
          |  "key with spaces": null,
          |  "number": -1.2323424E-5,
          |  "simpleKey" : "some value",
          |  "sub object" : {
          |    "sub key": 26.5,
          |    "a": "b",
          |    "array": [1, 2, { "yes":1, "no":0 }, ["a", "b", null], false]
          |  },
          |  "zero": 0
          |}""".stripMargin
      }

      PrettyPrinter(json) mustEqual {
        """{
          |  "simpleKey": "some value",
          |  "Boolean yes": true,
          |  "key with \"quotes\"": "string",
          |  "sub object": {
          |    "sub key": 26.5,
          |    "a": "b",
          |    "array": [1, 2, {
          |      "no": 0,
          |      "yes": 1
          |    }, ["a", "b", null], false]
          |  },
          |  "Boolean no": false,
          |  "key with spaces": null,
          |  "number": -1.2323424E-5,
          |  "Unic\u00f8de": "Long string with newline\nescape",
          |  "zero": 0
          |}""".stripMargin
      }
    }
  }
}