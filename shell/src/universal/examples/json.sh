#!/bin/bash
exec unicorn -nc "$0" "$@"
!#

import java.util._
import unicorn.json._

// Basic JSON
val json =
  """
  {
    "store": {
      "book": [
        {
          "category": "reference",
          "author": "Nigel Rees",
          "title": "Sayings of the Century",
          "price": 8.95
        },
        {
          "category": "fiction",
          "author": "Evelyn Waugh",
          "title": "Sword of Honour",
          "price": 12.99
        },
        {
          "category": "fiction",
          "author": "Herman Melville",
          "title": "Moby Dick",
          "isbn": "0-553-21311-3",
          "price": 8.99
        },
        {
          "category": "fiction",
          "author": "J. R. R. Tolkien",
          "title": "The Lord of the Rings",
          "isbn": "0-395-19395-8",
          "price": 22.99
        }
      ],
      "bicycle": {
        "color": "red",
        "price": 19.95
      }
    }
  }
  """.parseJsObject

println(json("store")("bicycle")("color"))
println(json.store.bicycle.color)
println(json.store.book(0).author)

json.store.bicycle.color = "green"
println(json.store.bicycle.color)

json("store")("book") remove 0
println(json.store.book)

val a = JsArray(1, 2, 3, 4)
a += 5
println(a)

val b: JsArray = Array(1, 2, 3, 4)
b ++= JsArray(5, 6)
println(b)

val obj = JsObject(
  "key1" -> JsObject(
    "key11" -> JsObject("tags" -> JsArray("alpha1", "beta1", "gamma1"))
  ),
  "key2" -> JsObject(
    "key21" -> JsObject("tags" -> JsArray("alpha2", "beta2", "gamma2"))
  ),
  "key3" -> "blabla"
)

// retrieve 1-level recursive path
println(obj \\ "tags")
// retrieve 2-level recursive path
println(obj \ "key1" \\ "tags")


// JsonPath
val jsonPath = JsonPath(
  """
    {
     "id": 1,
     "name": "Joe",
     "tags": ["programmer", "husband", "father", "golfer"],
     "address": [
       {
         "id": 2,
         "street": "123 Main St.",
         "city": "Springfield",
         "state": "PA"
       },
       {
         "id": 3,
         "street": "456 Main St.",
         "city": "Devon",
         "state": "PA",
         "work": true
       },
       {
         "id": 4,
         "street": "789 Main St.",
         "city": "Sea Isle City",
         "state": "NJ"
       }
     ]
  }
  """.parseJson)//.asInstanceOf[JsObject]

// field
println(jsonPath("$.id"))
println(jsonPath("$['id']"))

// recursive field
println(jsonPath("$..id"))

// multi fields
println(jsonPath("$['id', 'name']"))

// any field
println(jsonPath("$.*"))
println(jsonPath("$.tags.*"))
println(jsonPath("$['tags'].*"))

// recursive any
println(jsonPath("$..*"))

// array slices
println(jsonPath("$.tags[2]"))
println(jsonPath("$.tags[0:3:2]"))
println(jsonPath("$.tags[-2:]"))
println(jsonPath("$.tags[:-2]"))

// array random
println(jsonPath("$.tags[0,2]"))
println(jsonPath("$.tags[-1]"))

// array recursive
println(jsonPath("$.address[*].city"))


// has filter
println(jsonPath("$.address[?(@.work)]"))


// comparison filter
println(jsonPath("$.address[?(@.id < 3)]"))
println(jsonPath("$.address[?(@.id <= 3)]"))

println(jsonPath("$.address[?(@.id > 2)]"))
println(jsonPath("$.address[?(@.id >= 2)]"))

println(jsonPath("$.address[?(@.state == 'PA')]"))
println(jsonPath("$.address[?(@.city == 'Springfield')]"))
println(jsonPath("$.address[?(@.city != 'Devon')]"))


// boolean filter
println(jsonPath("$.address[?(@.id > 1 && @.state != 'PA')]"))
println(jsonPath("$.address[?(@.id < 4 && @.state == 'PA')]"))
println(jsonPath("$.address[?(@.id == 4 || @.state == 'PA')]"))
println(jsonPath("$.address[?(@.id == 4 || @.state == 'NJ')]"))

// update field of nonexistent object
jsonPath("$.person.id") = 10
println(jsonPath("$.person"))
println(jsonPath("$.person.id"))


// update multi fields of nonexistent object
jsonPath("$['person']['id', 'name']") = 30
println(jsonPath("$.person"))
println(jsonPath("$['person']['id', 'name']"))


// update array slices of nonexistent object
jsonPath("$.person.tags[1:3]") = "father"
println(jsonPath("$.person.tags"))


// update array random of nonexistent object
jsonPath("$.person.tags[2]") = "father"
println(jsonPath("$.person.tags"))
