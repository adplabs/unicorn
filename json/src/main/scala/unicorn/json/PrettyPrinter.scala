package unicorn.json

import java.lang.StringBuilder
import scala.annotation.tailrec

/**
 * A JsonPrinter that produces a nicely readable JSON source.
 * Adopt from spray-json.
 */
trait PrettyPrinter extends JsonPrinter {
  val Indent = 2

  def print(x: JsValue, sb: StringBuilder) {
    print(x, sb, 0)
  }

  protected def print(x: JsValue, sb: StringBuilder, indent: Int) {
    x match {
      case JsObject(x) => printObject(x, sb, indent)
      case JsArray(x)  => printArray(x, sb, indent)
      case _ => printLeaf(x, sb)
    }
  }

  protected def printObject(members: Iterable[(String, JsValue)], sb: StringBuilder, indent: Int) {
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

  protected def printArray(elements: Seq[JsValue], sb: StringBuilder, indent: Int) {
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
