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

package unicorn.unibase.graph

import scala.language.dynamics
import unicorn.json._

/** Graph vertex.
  *
  * @author Haifeng Li
  */
case class Vertex(val id: Long, val properties: JsObject, in: Map[String, Seq[Edge]], out: Map[String, Seq[Edge]]) extends Dynamic {
  def apply(key: String): JsValue = properties.apply(key)

  def applyDynamic(key: String): JsValue = apply(key)

  def selectDynamic(key: String): JsValue = apply(key)
}