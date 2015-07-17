package com.adp.unicorn

import java.lang.StringBuilder

/**
 * A JsonPrinter that produces compact JSON source without any superfluous whitespace.
 * Adopt from spray-json.
 */
trait CompactPrinter extends JsonPrinter {

  def print(x: JsonValue, sb: StringBuilder) {
    x match {
      case JsonObject(x) => printObject(x, sb)
      case JsonArray(x)  => printArray(x, sb)
      case _ => printLeaf(x, sb)
    }
  }

  protected def printObject(members: Iterable[(String, JsonValue)], sb: StringBuilder) {
    sb.append('{')
    printSeq(members, sb.append(',')) { m =>
      printString(m._1, sb)
      sb.append(':')
      print(m._2, sb)
    }
    sb.append('}')
  }

  protected def printArray(elements: Seq[JsonValue], sb: StringBuilder) {
    sb.append('[')
    printSeq(elements, sb.append(','))(print(_, sb))
    sb.append(']')
  }
}

object CompactPrinter extends CompactPrinter