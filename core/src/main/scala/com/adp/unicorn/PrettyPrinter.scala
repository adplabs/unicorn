package com.adp.unicorn

import java.lang.StringBuilder
import annotation.tailrec

/**
 * A JsonPrinter that produces a nicely readable JSON source.
 * Adopt from spray-json.
 */
trait PrettyPrinter extends JsonPrinter {
  val Indent = 2

  def print(x: JsonValue, sb: StringBuilder) {
    print(x, sb, 0)
  }

  protected def print(x: JsonValue, sb: StringBuilder, indent: Int) {
    x match {
      case JsonObject(x) => printObject(x, sb, indent)
      case JsonArray(x)  => printArray(x, sb, indent)
      case _ => printLeaf(x, sb)
    }
  }

  protected def printObject(members: Iterable[(String, JsonValue)], sb: StringBuilder, indent: Int) {
    sb.append("{\n")
    printSeq(members, sb.append(",\n")) { m =>
      printIndent(sb, indent + Indent)
      printString(m._1, sb)
      sb.append(": ")
      print(m._2, sb, indent + Indent)
    }
    sb.append('\n')
    printIndent(sb, indent)
    sb.append("}")
  }

  protected def printArray(elements: Seq[JsonValue], sb: StringBuilder, indent: Int) {
    sb.append('[')
    printSeq(elements, sb.append(", "))(print(_, sb, indent))
    sb.append(']')
  }

  protected def printIndent(sb: StringBuilder, indent: Int) {
    @tailrec def rec(indent: Int): Unit =
      if (indent > 0) {
        sb.append(' ')
        rec(indent - 1)
      }
    rec(indent)
  }
}

object PrettyPrinter extends PrettyPrinter