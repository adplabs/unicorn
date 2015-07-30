package unicorn.json

import org.specs2.mutable._

class JsValueSpec extends Specification {
  val json = JsObject(
    "store" -> JsObject(
      "book" -> JsArray(
        JsObject(
          "category" -> "reference",
          "author" -> "Nigel Rees",
          "title" -> "Sayings of the Century",
          "price" -> 8.95
        ),
        JsObject(
          "category" -> "fiction",
          "author" -> "Evelyn Waugh",
          "title" -> "Sword of Honour",
          "price" -> 12.99
        ),
        JsObject(
          "category" -> "fiction",
          "author" -> "Herman Melville",
          "title" -> "Moby Dick",
          "isbn" -> "0-553-21311-3",
          "price" -> 8.99
        ),
        JsObject(
          "category" -> "fiction",
          "author" -> "J. R. R. Tolkien",
          "title" -> "The Lord of the Rings",
          "isbn" -> "0-395-19395-8",
          "price" -> 22.99
        )
      ),
      "bicycle" -> JsObject(
        "color" -> "red",
        "price" -> 19.95
      )
    )
  )

  "The JsValue" should {
    "JsObject apply" in {
      json("store")("bicycle")("color") === JsString("red")
    }
    "JsObject selectDynamic" in {
      json.store.bicycle.color === JsString("red")
    }
    "JsArray apply" in {
      json("store")("book")(0)("author") === JsString("Nigel Rees")
    }
    "JsArray selectDynamic" in {
      json.store.book(0).author === JsString("Nigel Rees")
    }
  }
}