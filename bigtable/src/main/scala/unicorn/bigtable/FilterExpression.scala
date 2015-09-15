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

sealed trait Literal
case class StringLiteral(x: String) extends Literal
case class IntLiteral(x: Int) extends Literal
case class DoubleLiteral(x: Double) extends Literal
case class DateLiteral(x: Date) extends Literal

sealed trait FilterExpression
case class AndExpression(exps: Seq[FilterExpression]) extends FilterExpression
case class OrExpression(exps: Seq[FilterExpression]) extends FilterExpression
case class EqExpression(left: String, right: Literal) extends FilterExpression
case class NeExpression(left: String, right: Literal) extends FilterExpression
case class GtExpression(left: String, right: Literal) extends FilterExpression
case class GeExpression(left: String, right: Literal) extends FilterExpression
case class LtExpression(left: String, right: Literal) extends FilterExpression
case class LeExpression(left: String, right: Literal) extends FilterExpression

/**
 * The where clause used for scan and index. Currently we support = (equal), <> or != (not equal),
 * > (greater than), >= (greater than or equal), < (less than), and <= (less than or equal).
 * The AND & OR operators can be used to combine multiple conditions. Datetime should be
 * in ISO 8601 format (yyyy-MM-dd'T'HH:mm:ss'Z' or yyyy-MM-dd'T'HH:mm:ss.SSS'Z').
 *
 * @author Haifeng Li
 */
class FilterExpressionParser extends JavaTokenParsers {
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
    jsFieldPath ~ "="  ~ filterLiteral ^^ { case left ~ _  ~ right => EqExpression(left, right) } |
    filterLiteral ~ "="  ~ jsFieldPath ^^ { case left ~ _  ~ right => EqExpression(right, left) }

  def neExpression: Parser[FilterExpression] =
    jsFieldPath ~ ("<>" | "!=") ~ filterLiteral ^^ { case left ~ _ ~ right => NeExpression(left, right) } |
    filterLiteral ~ ("<>" | "!=") ~ jsFieldPath ^^ { case left ~ _ ~ right => NeExpression(right, left) }

  def gtExpression: Parser[FilterExpression] =
    jsFieldPath ~ ">"  ~ filterLiteral ^^ { case left ~ _ ~ right => GtExpression(left, right) } |
    filterLiteral ~ "<"  ~ jsFieldPath ^^ { case left ~ _ ~ right => GtExpression(right, left) }

  def geExpression: Parser[FilterExpression] =
    jsFieldPath ~ ">=" ~ filterLiteral ^^ { case left ~ _ ~ right => GeExpression(left, right) } |
    filterLiteral ~ "<=" ~ jsFieldPath ^^ { case left ~ _ ~ right => GeExpression(right, left) }

  def ltExpression: Parser[FilterExpression] =
    jsFieldPath ~ "<"  ~ filterLiteral ^^ { case left ~ _ ~ right => LtExpression(left, right) } |
    filterLiteral ~ ">"  ~ jsFieldPath ^^ { case left ~ _ ~ right => LtExpression(right, left) }

  def leExpression: Parser[FilterExpression] =
    jsFieldPath ~ "<=" ~ filterLiteral ^^ { case left ~ _ ~ right => LeExpression(left, right) } |
    filterLiteral ~ ">=" ~ jsFieldPath ^^ { case left ~ _ ~ right => LeExpression(right, left) }

  def andOp: Parser[String] = """(?i)(and)|(&&)""".r

  def orOp: Parser[String] = """(?i)(or)|(\|\|)""".r

  def andExpression: Parser[FilterExpression] =
    expression ~ rep1(andOp ~> expression) ^^ { case f1 ~ fs => AndExpression(f1 :: fs) }

  def orExpression:  Parser[FilterExpression] =
    expression ~ rep1(orOp ~> expression) ^^ { case f1 ~ fs => OrExpression(f1 :: fs) }

  def expression: Parser[FilterExpression] =
    eqExpression | neExpression | gtExpression | geExpression | ltExpression | leExpression |
    andExpression | orExpression | "(" ~> expression <~ ")"

  def parse(input: String): FilterExpression = parseAll(expression, input) match {
    case Success(result, _) => result
    case failure : NoSuccess => scala.sys.error(failure.msg)
  }
}

object FilterExpression {
  def apply(input: String): FilterExpression = {
    (new FilterExpressionParser).parse(input)
  }
}
