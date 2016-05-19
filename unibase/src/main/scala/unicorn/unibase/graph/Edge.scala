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
import unicorn.json.JsValue

/** Graph edge.
  *
  * @author Haifeng Li
  */
case class Edge(val source: Long, val label: String, val target: Long, val properties: JsValue) extends Dynamic {

  def apply(property: String): JsValue = properties.apply(property)

  def applyDynamic(property: String): JsValue = apply(property)

  def selectDynamic(property: String): JsValue = apply(property)
}