package unicorn.bigtable

import scala.util.parsing.combinator.JavaTokenParsers

/**
 *  <b-expression>::= <b-term> [<orop> <b-term>]*
 *  <b-term>      ::= <not-factor> [AND <not-factor>]*
 *  <not-factor>  ::= [NOT] <b-factor>
 *  <b-factor>    ::= <b-literal> | <b-variable> | (<b-expression>)
 *
 *  @author Haifeng Li
 */

case class FilterExpression(variableMap: Map[String, Boolean]) extends JavaTokenParsers {
  private lazy val b_expression: Parser[Boolean] = b_term ~ rep("or" ~ b_term) ^^ { case f1 ~ fs ⇒ (f1 /: fs)(_ || _._2) }
  private lazy val b_term: Parser[Boolean] = (b_not_factor ~ rep("and" ~ b_not_factor)) ^^ { case f1 ~ fs ⇒ (f1 /: fs)(_ && _._2) }
  private lazy val b_not_factor: Parser[Boolean] = opt("not") ~ b_factor ^^ (x ⇒ x match { case Some(v) ~ f ⇒ !f; case None ~ f ⇒ f })
  private lazy val b_factor: Parser[Boolean] = b_literal | b_variable | ("(" ~ b_expression ~ ")" ^^ { case "(" ~ exp ~ ")" ⇒ exp })
  private lazy val b_literal: Parser[Boolean] = "true" ^^ (x ⇒ true) | "false" ^^ (x ⇒ false)
  // This will construct the list of variables for this parser
  private lazy val b_variable: Parser[Boolean] = variableMap.keysIterator.map(Parser(_)).reduceLeft(_ | _) ^^ (x ⇒ variableMap(x))

  def parse(expression: String) = this.parseAll(b_expression, expression)
}

object FilterExpression {
  def parse(variables: Map[String, Boolean])(value: String) {
    println(FilterExpression(variables).parse(value))
  }
}
