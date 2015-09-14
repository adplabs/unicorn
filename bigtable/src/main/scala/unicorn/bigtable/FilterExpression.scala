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
 *
 *  @author Haifeng Li
 */
class FilterExpressionParser extends JavaTokenParsers {
  private lazy val filterLiteral: Parser[Literal] =
    stringLiteral ^^ { x => StringLiteral(x) } |
    wholeNumber ^^ { x => IntLiteral(x.toInt) } |
    (decimalNumber | floatingPointNumber) ^^ { x => DoubleLiteral(x.toDouble) } |
    """\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(.\d{3})?)?""".r ^^ { x => DateLiteral(Date.from(Instant.parse(x))) }

  private lazy val eqExpression: Parser[FilterExpression] = ident ~ "="  ~ filterLiteral ^^ { case left ~ "="  ~ right => EqExpression(left, right) }
  private lazy val neExpression: Parser[FilterExpression] = ident ~ "<>" ~ filterLiteral ^^ { case left ~ "<>" ~ right => NeExpression(left, right) }
  private lazy val gtExpression: Parser[FilterExpression] = ident ~ ">"  ~ filterLiteral ^^ { case left ~ ">"  ~ right => GtExpression(left, right) }
  private lazy val geExpression: Parser[FilterExpression] = ident ~ ">=" ~ filterLiteral ^^ { case left ~ ">=" ~ right => GeExpression(left, right) }
  private lazy val ltExpression: Parser[FilterExpression] = ident ~ "<"  ~ filterLiteral ^^ { case left ~ "<"  ~ right => LtExpression(left, right) }
  private lazy val leExpression: Parser[FilterExpression] = ident ~ "<=" ~ filterLiteral ^^ { case left ~ "<=" ~ right => LeExpression(left, right) }

  private lazy val andExpression: Parser[FilterExpression] = expression ~ rep("and" ~ expression) ^^ { case f1 ~ fs => AndExpression(f1 :: fs.map(_._2)) }
  private lazy val orExpression:  Parser[FilterExpression] = expression ~ rep("or" ~ expression) ^^ { case f1 ~ fs => OrExpression(f1 :: fs.map(_._2)) }
  private lazy val expression: Parser[FilterExpression] = eqExpression | neExpression | gtExpression | geExpression | ltExpression | leExpression |
    andExpression | orExpression | ("(" ~ expression ~ ")" ^^ { case "(" ~ exp ~ ")" => exp })

  def parse(input: String): FilterExpression = parseAll(expression, input) match {
    case Success(result, _) => result
    case failure : NoSuccess => scala.sys.error(failure.msg)
  }
}

object FilterExpression {
  def parse(input: String) {
    (new FilterExpressionParser).parse(input)
  }
}
