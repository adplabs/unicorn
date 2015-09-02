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

package unicorn.json

/**
 * @author Haifeng Li
 */
trait JsonSerializer {
  val root = "$"
  /** Serializes a JSON value to a list of key/value pairs, where key is the JSONPath of element. */
  def serialize(value: JsValue, jsonPath: String = root): Map[String, Array[Byte]]
  /** Deserialize a JSON value from the given root JSONPath. */
  def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String = root): JsValue
}
