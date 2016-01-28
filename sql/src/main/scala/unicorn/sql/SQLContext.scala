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

package unicorn.sql

import unicorn.json._
import unicorn.unibase.HUnibase

/** SQL Executor.
  *
  * @author Haifeng Li
  */
class SQLContext(db: HUnibase) {

  def sql(query: String): Iterator[JsObject] = {
    val sql = SQLParser.parse(query)
    if (sql.isEmpty)
      throw new IllegalArgumentException(s"Invalid SQL statement: $query")

    val select = sql.get

    if (select.groupBy.isDefined)
      throw new UnsupportedOperationException("Group By is not supported yet")

    if (select.orderBy.isDefined)
      throw new UnsupportedOperationException("Order By is not supported yet")

    if (select.limit.isDefined)
      throw new UnsupportedOperationException("LIMIT is not supported yet")

    if (select.relations.size > 1)
      throw new UnsupportedOperationException("Join is not supported yet")

    val table = select.relations(0) match {
      case Table(name, None) => db(name)
      case Table(name, Some(_)) => throw new UnsupportedOperationException("Table Alias is not supported yet")
      case Subquery(_, _) => throw new UnsupportedOperationException("Subquery is not supported yet")
    }

    table.find(projections2Json(select.projections), where2Json(select.where))
  }

  private def projections2Json(projections: Projections): JsObject = {
    projections match {
      case AllColumns() => JsObject()
      case ExpressionProjections(lst) =>
        val js = JsObject()
        lst.foreach {
          case (FieldIdent(_, field, _), _) => js(field) = 1
          case _ => throw new UnsupportedOperationException("Only plain field projection is supported")
        }
        js
    }
  }

  private def where2Json(where: Option[Expression]): JsObject = {
    where match {
      case None => JsObject()
      case Some(expr) =>
        val js = JsObject()
        predict(js, expr)
        js
    }
  }

  private def predict(where: JsObject, expr: Expression): Unit = {
    expr match {
      case And(left, right) =>
        predict(where, left)
        predict(where, right)

      case Or(left, right) =>
        val leftObj = JsObject()
        predict(leftObj, left)
        val rightObj = JsObject()
        predict(rightObj, right)
        where("$or") = JsArray(leftObj, rightObj)

      case Equals(FieldIdent(_, field, _), IntLiteral(value)) => where(field) = JsInt(value)
      case Equals(FieldIdent(_, field, _), FloatLiteral(value)) => where(field) = JsDouble(value)
      case Equals(FieldIdent(_, field, _), StringLiteral(value)) => where(field) = JsString(value)
      case Equals(FieldIdent(_, field, _), DateLiteral(value)) => where(field) = JsDate(value)
      case Equals(IntLiteral(value), FieldIdent(_, field, _)) => where(field) = JsInt(value)
      case Equals(FloatLiteral(value), FieldIdent(_, field, _)) => where(field) = JsDouble(value)
      case Equals(StringLiteral(value), FieldIdent(_, field, _)) => where(field) = JsString(value)
      case Equals(DateLiteral(value), FieldIdent(_, field, _)) => where(field) = JsDate(value)

      case NotEquals(FieldIdent(_, field, _), IntLiteral(value)) => where(field) = JsObject("$ne" -> JsInt(value))
      case NotEquals(FieldIdent(_, field, _), FloatLiteral(value)) => where(field) = JsObject("$ne" -> JsDouble(value))
      case NotEquals(FieldIdent(_, field, _), StringLiteral(value)) => where(field) = JsObject("$ne" -> JsString(value))
      case NotEquals(FieldIdent(_, field, _), DateLiteral(value)) => where(field) = JsObject("$ne" -> JsDate(value))
      case NotEquals(IntLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ne" -> JsInt(value))
      case NotEquals(FloatLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ne" -> JsDouble(value))
      case NotEquals(StringLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ne" -> JsString(value))
      case NotEquals(DateLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ne" -> JsDate(value))

      case GreaterThan(FieldIdent(_, field, _), IntLiteral(value)) => where(field) = JsObject("$gt" -> JsInt(value))
      case GreaterThan(FieldIdent(_, field, _), FloatLiteral(value)) => where(field) = JsObject("$gt" -> JsDouble(value))
      case GreaterThan(FieldIdent(_, field, _), StringLiteral(value)) => where(field) = JsObject("$gt" -> JsString(value))
      case GreaterThan(FieldIdent(_, field, _), DateLiteral(value)) => where(field) = JsObject("$gt" -> JsDate(value))
      case GreaterThan(IntLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$lt" -> JsInt(value))
      case GreaterThan(FloatLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$lt" -> JsDouble(value))
      case GreaterThan(StringLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$lt" -> JsString(value))
      case GreaterThan(DateLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$lt" -> JsDate(value))

      case GreaterOrEqual(FieldIdent(_, field, _), IntLiteral(value)) => where(field) = JsObject("$ge" -> JsInt(value))
      case GreaterOrEqual(FieldIdent(_, field, _), FloatLiteral(value)) => where(field) = JsObject("$ge" -> JsDouble(value))
      case GreaterOrEqual(FieldIdent(_, field, _), StringLiteral(value)) => where(field) = JsObject("$ge" -> JsString(value))
      case GreaterOrEqual(FieldIdent(_, field, _), DateLiteral(value)) => where(field) = JsObject("$ge" -> JsDate(value))
      case GreaterOrEqual(IntLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$le" -> JsInt(value))
      case GreaterOrEqual(FloatLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$le" -> JsDouble(value))
      case GreaterOrEqual(StringLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$le" -> JsString(value))
      case GreaterOrEqual(DateLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$le" -> JsDate(value))

      case LessThan(FieldIdent(_, field, _), IntLiteral(value)) => where(field) = JsObject("$lt" -> JsInt(value))
      case LessThan(FieldIdent(_, field, _), FloatLiteral(value)) => where(field) = JsObject("$lt" -> JsDouble(value))
      case LessThan(FieldIdent(_, field, _), StringLiteral(value)) => where(field) = JsObject("$lt" -> JsString(value))
      case LessThan(FieldIdent(_, field, _), DateLiteral(value)) => where(field) = JsObject("$lt" -> JsDate(value))
      case LessThan(IntLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$gt" -> JsInt(value))
      case LessThan(FloatLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$gt" -> JsDouble(value))
      case LessThan(StringLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$gt" -> JsString(value))
      case LessThan(DateLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$gt" -> JsDate(value))

      case LessOrEqual(FieldIdent(_, field, _), IntLiteral(value)) => where(field) = JsObject("$le" -> JsInt(value))
      case LessOrEqual(FieldIdent(_, field, _), FloatLiteral(value)) => where(field) = JsObject("$le" -> JsDouble(value))
      case LessOrEqual(FieldIdent(_, field, _), StringLiteral(value)) => where(field) = JsObject("$le" -> JsString(value))
      case LessOrEqual(FieldIdent(_, field, _), DateLiteral(value)) => where(field) = JsObject("$le" -> JsDate(value))
      case LessOrEqual(IntLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ge" -> JsInt(value))
      case LessOrEqual(FloatLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ge" -> JsDouble(value))
      case LessOrEqual(StringLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ge" -> JsString(value))
      case LessOrEqual(DateLiteral(value), FieldIdent(_, field, _)) => where(field) = JsObject("$ge" -> JsDate(value))
    }
  }
}
