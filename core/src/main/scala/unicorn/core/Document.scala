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

package unicorn.core

import java.nio._
import java.util.UUID
import unicorn.json._

/**
 * A document is a JSON value with a unique key.
 * 
 * @author Haifeng Li
 */
case class Document(val key: Array[Byte], value: JsValue)

object Document {
  def apply(json: JsValue): Document = apply(UUID.randomUUID, json)

  def apply(key: String, json: JsValue): Document = new Document(key.getBytes("UTF-8"), json)

  def apply(key: Int, json: JsValue): Document = {
    val buffer = new Array[Byte](4)
    ByteBuffer.wrap(buffer).putInt(key)
    new Document(buffer, json)
  }

  def apply(key: Long, json: JsValue): Document = {
    val buffer = new Array[Byte](8)
    ByteBuffer.wrap(buffer).putLong(key)
    new Document(buffer, json)
  }

  def apply(key: UUID, json: JsValue): Document = {
    val buffer = new Array[Byte](16)
    ByteBuffer.wrap(buffer).putLong(key.getMostSignificantBits)
    ByteBuffer.wrap(buffer).putLong(key.getLeastSignificantBits)
    new Document(buffer, json)
  }
}
