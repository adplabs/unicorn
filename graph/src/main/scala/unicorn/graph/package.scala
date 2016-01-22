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

package unicorn

import unicorn.json._
import unicorn.unibase.Unibase.{$id, $graph}

/**
 * @author Haifeng Li
 */
package object graph {
  /** The field of data associated with a relationship. */
  val $data = "data"
  def graph(js: JsObject) = new PimpedJsObject(js)
}

package graph {

private[graph] class PimpedJsObject(js: JsObject) {
  /** Returns an object of neighbors for given relationship.
    *
    * @param relationship the relationship of interest.
    * @return an object of which each field is a neighbor.
    */
  def apply(relationship: String): JsObject = {
    js($graph)(relationship).asInstanceOf[JsObject]
  }

  /** Returns any associated data with a relationship.
    *
    * @param relationship the relationship of interest.
    * @param id the string of directed edge target object id.
    * @return any data associated with the relationship.
    */
  def apply(relationship: String, id: String): JsValue = {
    js($graph)(relationship)(id)
  }

  /** Sets or removes a relationship.
    *
    * @param relationship the relationship of interest.
    * @param id the directed edge target object id.
    * @param data any data associated with the relationship.
    *             Removes the relationship if data is JsUndefined.
    *             If data is JsNull, the relationship will be
    *             created but without associated data.
    */
  def update(relationship: String, id: JsValue, data: JsValue): Unit = {
    val link = JsObject($id -> id, $data -> data)
    if (data != JsUndefined && data != JsNull) link($data) = data

    if (js($graph) == JsUndefined) {
      js($graph) = JsObject(relationship -> JsObject())
    }

    if (js($graph)(relationship) == JsUndefined) {
      js($graph)(relationship) = JsObject()
    }

    if (data == JsUndefined) {
      js($graph)(relationship).remove(id.toString)
    } else {
      val link = JsObject($id -> id)
      if (data != JsNull) link($data) = data
      js($graph)(relationship)(id.toString) = link
    }
  }
}

}