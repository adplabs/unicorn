package unicorn.json

import io.gatling.jsonpath._
import io.gatling.jsonpath.AST._
import scala.util.Try

/**
 * An implementation of JSONPath that follows the semantics described by Stefan Goessner
 * on his <a href="http://goessner.net/articles/JsonPath/">blog</a>.
 *
 * Adopt from https://github.com/josephpconley/play-jsonpath
 *
 * This parser supports all queries except for queries that rely on expressions of the underlying language
 * like $..book[(@.length-1)]. However, there’s usually a ready workaround as you can execute the same query
 * using $..book[-1:].
 *
 * One deviation from JSONPath is to always flatten the results of a recursive query. Using the bookstore example,
 * typically a query of $..book will return an array with one element, the array of books. If there was another book
 * array somewhere in the document, then $..book will return an array with two elements, both arrays of books.
 * However, if you were to query $..book[2] for our example, you would get the second book in the first array,
 * which assumes that the $..book result has been flattened. In order to make recursion easier and the code simpler,
 * we always flatten the result of recursive queries regardless of the context.
 */
case class JsonPath(json: JsValue) {
  val parser = new Parser

  private def error(msg: Option[String] = None) = throw new Exception("Bad JsonPath query" + msg.map(" :" + _).getOrElse(""))

  def apply(q: String): JsValue = {
    val tokens = parser.compile(q).getOrElse(error(Some(q)))
    parse(tokens, json)
  }

  private def parse(tokens: List[PathToken], js: JsValue): JsValue = tokens.foldLeft[JsValue](js)( (js, token) => token match {
    case Field(name) => js match {
      case JsObject(fields) => js(name)
      case JsArray(arr) => JsArray(arr.map(_(name)): _*)
      case _ => error()
    }
    case RecursiveField(name) => js match {
      case JsObject(fields) => {
        var value = js \\ name
        if(value.size > 0 && value.elements.head.isInstanceOf[JsArray]){
          value = value.elements.flatMap(_.asInstanceOf[JsArray].elements)
        }
        value
      }
      case JsArray(arr) => {
        var value = arr.flatMap(_ \\ name)
        if(value.head.isInstanceOf[JsArray]){
          value = value.flatMap(_.asInstanceOf[JsArray].elements)
        }
        JsArray(value: _*)
      }
      case _ => error()
    }
    case MultiField(names) => js match {
      case JsObject(fields) => JsArray(fields.filter(f => names.contains(f._1)).map(_._2).toArray: _*)
      case _ => error()
    }
    case AnyField => js match {
      case JsObject(fields) => JsArray(fields.map(_._2).toArray: _*)
      case JsArray(arr) => js
      case _ => error()
    }
    case RecursiveAnyField => js
    case ArraySlice(start, stop, step) =>
      val arr = js.asInstanceOf[JsArray]
      var sliced = if (start.getOrElse(0) >= 0) arr.elements.drop(start.getOrElse(0)) else arr.elements.takeRight(Math.abs(start.get))
      sliced = if(stop.getOrElse(arr.elements.size) >= 0) sliced.slice(0, stop.getOrElse(arr.elements.size)) else sliced.dropRight(Math.abs(stop.get))

      if(step < 0) sliced = sliced.reverse

      JsArray(sliced.zipWithIndex.filter(_._2 % Math.abs(step) == 0).map(_._1): _*)
    case ArrayRandomAccess(indices) => {
      val arr = js.asInstanceOf[JsArray].elements
      val selectedIndices = indices.map(i => if(i >= 0) i else arr.size + i).toSet.toSeq

      if(selectedIndices.size == 1) arr(selectedIndices.head) else JsArray(selectedIndices.map(arr(_)): _*)
    }
    case ft: FilterToken => JsArray(parseFilterToken(ft, js): _*)
    case _ => js
  })

  private def parseFilterToken(ft: FilterToken, js: JsValue): Seq[JsValue] = ft match {
    case HasFilter(SubQuery(tokens)) =>
      (for {
        arr <- Try(js.asInstanceOf[JsArray]).toOption
        children <- Try(arr.elements.map(_.asInstanceOf[JsObject])).toOption
      } yield {
          tokens.last match {
            case Field(name) => children.filter(_.fields.keySet.contains(name))
            case MultiField(names) => children.filter(_.fields.keySet.intersect(names.toSet) == names.toSet)
            case _ => error()
          }
        }).getOrElse(error())
    case ComparisonFilter(op, lhv, rhv) => {
      val arr = js.asInstanceOf[JsArray]
      arr.elements.filter { obj =>
        val left = parseFilterValue(lhv, obj)
        val right = parseFilterValue(rhv, obj)
        op.apply(left, right)
      }
    }
    case BooleanFilter(binOp, lht, rht) => {
      val leftJs = parseFilterToken(lht, js)
      val rightJs = parseFilterToken(rht, js)

      binOp match {
        case OrOperator => leftJs.union(rightJs).toSet.toSeq
        case AndOperator => leftJs.intersect(rightJs)
      }
    }
  }

  private def parseFilterValue(fv: FilterValue, js: JsValue): Any = fv match {
    case SubQuery(tokens) => Try{
      primitive(parse(tokens, js)) match {
        case n:Number => n.doubleValue()
        case a @ _ => a
      }
    }.getOrElse(error())
    case dv: FilterDirectValue => dv.value
  }

  private def primitive(js: JsValue): Any = js match {
    case JsBoolean(b) => b
    case JsInt(n) => n
    case JsLong(n) => n
    case JsDouble(n) => n
    case JsString(s) => s
    case JsDate(t) => t
    case JsUUID(uuid) => uuid
    case JsBinary(b) => b
    case _ => throw new Exception("Not a primitive: " + js)
  }
}
