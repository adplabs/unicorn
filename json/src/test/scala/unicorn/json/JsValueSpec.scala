package unicorn.json

import org.specs2.mutable._

class JsValueSpec extends Specification {
  val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/store.json")).mkString

  "The JsValue" should {
    "JsObject apply" in {
      val json = JsonParser(jsonSource)
      json("store")("bicycle")("color") === JsString("red")
    }
    "JsObject selectDynamic" in {
      val json = JsonParser(jsonSource)
      json.store.bicycle.color === JsString("red")
    }
    "JsArray apply" in {
      val json = JsonParser(jsonSource)
      json("store")("book")(0)("author") === JsString("Nigel Rees")
    }
    "JsArray selectDynamic" in {
      val json = JsonParser(jsonSource)
      json.store.book(0).author === JsString("Nigel Rees")
    }
    "JsObject.field returns JsUndefined" in {
      val json = JsonParser(jsonSource)
      json.store.book(0).isbn === JsUndefined
    }
    "JsArray(10) selectDynamic" in {
      val json = JsonParser(jsonSource)
      json.store.book(10).author must throwA[IndexOutOfBoundsException]
    }

    "JsObject update" in {
      val json = JsonParser(jsonSource)
      json("store")("bicycle")("color") = "blue"
      json("store")("bicycle")("color") === JsString("blue")
    }
    "JsObject updateDynamic" in {
      val json = JsonParser(jsonSource)
      json.store.bicycle.color = "green"
      json.store.bicycle.color === JsString("green")
    }
    "JsArray update" in {
      val json = JsonParser(jsonSource)
      json("store")("book")(0)("author") = "Dude"
      json("store")("book")(0)("author") === JsString("Dude")
    }
    "JsArray updateDynamic" in {
      val json = JsonParser(jsonSource)
      json.store.book(0).author = "Confucius"
      json.store.book(0).author === JsString("Confucius")
    }
    "JsArray(10) updateDynamic" in {
      val json = JsonParser(jsonSource)
      (json.store.book(10).author = "Confucius") must throwA[IndexOutOfBoundsException]
    }

    "JsObject remove" in {
      val json = JsonParser(jsonSource)
      json("store")("bicycle").remove("color") === Some(JsString("red"))
      json("store")("bicycle")("color") === JsUndefined
    }
    "JsArray remove" in {
      val json = JsonParser(jsonSource)
      json("store")("book") remove 0
      json("store")("book")(0) === JsObject(
        "category" -> "fiction",
        "author" -> "Evelyn Waugh",
        "title" -> "Sword of Honour",
        "price" -> 12.99
      )
    }

    "JsArray +=" in {
      val a: JsArray = Array(1, 2, 3, 4)
      a += 5
      a === JsArray(1, 2, 3, 4, 5)
    }
    "JsArray ++=" in {
      val a: JsArray = Array(1, 2, 3, 4)
      a ++= JsArray(5, 6)
      a === JsArray(1, 2, 3, 4, 5, 6)
    }
    "JsArray +=:" in {
      val a: JsArray = Array(1, 2, 3, 4)
      5 +=: a
      a === JsArray(5, 1, 2, 3, 4)
    }
    "JsArray ++=:" in {
      val a: JsArray = Array(1, 2, 3, 4)
      JsArray(5, 6) ++=: a
      a === JsArray(5, 6, 1, 2, 3, 4)
    }
  }
}