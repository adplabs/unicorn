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

package unicorn.bigtable

import java.util.Date
import java.time.Instant
import scala.util.parsing.combinator.JavaTokenParsers
import unicorn.util.Logging

sealed trait Literal
case class StringLiteral(x: String) extends Literal
case class IntLiteral(x: Int) extends Literal
case class DoubleLiteral(x: Double) extends Literal
case class DateLiteral(x: Date) extends Literal

sealed trait FilterExpression
case class And(left: FilterExpression, right: FilterExpression) extends FilterExpression
case class Or (left: FilterExpression, right: FilterExpression) extends FilterExpression
case class Eq(left: String, right: Literal) extends FilterExpression
case class Ne(left: String, right: Literal) extends FilterExpression
case class Gt(left: String, right: Literal) extends FilterExpression
case class Ge(left: String, right: Literal) extends FilterExpression
case class Lt(left: String, right: Literal) extends FilterExpression
case class Le(left: String, right: Literal) extends FilterExpression

/**
 * The where clause used for scan and index. Currently we support = (equal), <> or != (not equal),
 * > (greater than), >= (greater than or equal), < (less than), and <= (less than or equal).
 * The AND & OR operators can be used to combine multiple conditions. Datetime should be
 * in ISO 8601 format (yyyy-MM-dd'T'HH:mm:ss'Z' or yyyy-MM-dd'T'HH:mm:ss.SSS'Z').
 *
 * @author Haifeng Li
 */
class FilterExpressionParser extends JavaTokenParsers with Logging {
  def filterLiteral: Parser[Literal] =
    stringLiteral ^^ { x => StringLiteral(x.substring(1, x.length-1)) } | // dequoted
    """\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(.\d{3})?Z)?""".r ^^ { x => DateLiteral(Date.from(Instant.parse(x))) } |
    // floatingPointNumber recognizes integers too. We force a dot after the integer part here
    """-?((\d+\.\d*|\d*\.\d+)|(\d+(\.\d*)?|\d*\.\d+)[eE][+-]?\d+)""".r ^^ { x => DoubleLiteral(x.toDouble) } |
    // wholeNumber should after the floating number. Otherwise, it will recognize the integer part first.
    wholeNumber ^^ { x => IntLiteral(x.toInt) }

  def jsFieldPath: Parser[String] =
    """[$_a-zA-Z][$_a-zA-Z0-9]*(\[\d+\])?(\.[$_a-zA-Z][$_a-zA-Z0-9]*(\[\d+\])?)*""".r

  def eqExpression: Parser[FilterExpression] =
    jsFieldPath ~ "="  ~ filterLiteral ^^ { case left ~ _  ~ right => Eq(left, right) } |
    filterLiteral ~ "="  ~ jsFieldPath ^^ { case left ~ _  ~ right => Eq(right, left) }

  def neExpression: Parser[FilterExpression] =
    jsFieldPath ~ ("<>" | "!=") ~ filterLiteral ^^ { case left ~ _ ~ right => Ne(left, right) } |
    filterLiteral ~ ("<>" | "!=") ~ jsFieldPath ^^ { case left ~ _ ~ right => Ne(right, left) }

  def gtExpression: Parser[FilterExpression] =
    jsFieldPath ~ ">"  ~ filterLiteral ^^ { case left ~ _ ~ right => Gt(left, right) } |
    filterLiteral ~ "<"  ~ jsFieldPath ^^ { case left ~ _ ~ right => Gt(right, left) }

  def geExpression: Parser[FilterExpression] =
    jsFieldPath ~ ">=" ~ filterLiteral ^^ { case left ~ _ ~ right => Ge(left, right) } |
    filterLiteral ~ "<=" ~ jsFieldPath ^^ { case left ~ _ ~ right => Ge(right, left) }

  def ltExpression: Parser[FilterExpression] =
    jsFieldPath ~ "<"  ~ filterLiteral ^^ { case left ~ _ ~ right => Lt(left, right) } |
    filterLiteral ~ ">"  ~ jsFieldPath ^^ { case left ~ _ ~ right => Lt(right, left) }

  def leExpression: Parser[FilterExpression] =
    jsFieldPath ~ "<=" ~ filterLiteral ^^ { case left ~ _ ~ right => Le(left, right) } |
    filterLiteral ~ ">=" ~ jsFieldPath ^^ { case left ~ _ ~ right => Le(right, left) }

  def andOp: Parser[String] = """(?i)(and)|(&&)""".r

  def orOp: Parser[String] = """(?i)(or)|(\|\|)""".r

  def andExpression: Parser[FilterExpression] =
    simpleExpression * (andOp ^^^ { (left: FilterExpression, right: FilterExpression) => And(left, right) })

  def orExpression:  Parser[FilterExpression] =
    andExpression * (orOp ^^^ { (left: FilterExpression, right: FilterExpression) => Or(left, right) })

  def simpleExpression: Parser[FilterExpression] =
    eqExpression | neExpression | gtExpression | geExpression | ltExpression | leExpression | "(" ~> expression <~ ")"

  def expression: Parser[FilterExpression] =
    orExpression | andExpression | simpleExpression

  def parse(input: String): FilterExpression = parseAll(expression, input) match {
    case Success(result, _) => result
    case failure: NoSuccess => log.error(failure.toString); scala.sys.error(failure.msg)
  }
}

object FilterExpression {
  def apply(input: String): FilterExpression = {
    (new FilterExpressionParser).parse(input)
  }
}
