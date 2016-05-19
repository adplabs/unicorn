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

import java.nio.charset.Charset

/**
 * @author Haifeng Li
 */
trait JsonSerializer {
  /** string encoder/decoder */
  val charset: Charset

  /** The path to the root of a JsValue. */
  val root: String

  /** The delimiter in the path to an embedded object or in an array.
    * In general, we follow the dot notation as in MongoDB (even for array elements).
    */
  val pathDelimiter: String

  /** Byte array of undefined. */
  val undefined: Array[Byte]

  /** Byte array of null. */
  val `null`: Array[Byte]

  /** Serialize a string to bytes. */
  def str2Bytes(s: String) = s.getBytes(charset)

  /** Returns the json path of a dot notation path as in MongoDB. */
  def str2Path(path: String) = s"${root}${pathDelimiter}$path"

  /** Returns the byte array of json path */
  def str2PathBytes(path: String) = str2Bytes(str2Path(path))


  /** Serializes a JSON value to a list of key/value pairs, where key is the JSONPath of element. */
  def serialize(value: JsValue, rootJsonPath: String = root): Map[String, Array[Byte]]

  /** Deserialize a JSON value from the given root JSONPath. */
  def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String = root): JsValue
}
