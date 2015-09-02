/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package com.adp.unicorn

import java.lang.StringBuilder

/**
 * A JsonPrinter that produces compact JSON source without any superfluous whitespace.
 * Adopt from spray-json.
 *
 * @author Haifeng Li
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