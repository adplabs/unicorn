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
          case (StringLiteral(field), _) => js(field) = 1
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
        js
    }
  }
}
