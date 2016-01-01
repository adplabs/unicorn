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

package unicorn.unibase

import java.util.{Date, UUID}
import unicorn.json._
import unicorn.oid.BsonObjectId
import unicorn.util.{ByteArray, utf8}

/**
 * A document is a JSON value with a unique key.
 * 
 * @author Haifeng Li
 */
case class Document(val key: JsValue, value: JsObject)

object Document {
  def apply(json: JsObject): Document = apply(UUID.randomUUID, json)

  def apply(key: String, json: JsObject): Document = new Document(JsString(key), json)

  def apply(key: Int, json: JsObject): Document = new Document(JsInt(key), json)

  def apply(key: Long, json: JsObject): Document = new Document(JsLong(key), json)

  def apply(key: Date, json: JsObject): Document = new Document(JsDate(key), json)

  def apply(key: UUID, json: JsObject): Document = new Document(JsUUID(key), json)

  def apply(key: BsonObjectId, json: JsObject): Document = new Document(JsObjectId(key), json)

  def apply(key: Array[Byte], json: JsObject): Document = new Document(JsBinary(key), json)
}
