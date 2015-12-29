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

import unicorn.json._
import unicorn.bigtable._
import unicorn.util.utf8

/**
 * @author Haifeng Li
 */
class DocumentCollection(table: BigTable, family: String) {
  val serializer = new ColumnarJsonSerializer
  val columnFamily = family.getBytes("UTF-8")

  def apply(id: String): Document = {
    apply(id.getBytes("UTF-8"))
  }

  def apply(id: Array[Byte]): Document = {
    val map = table.get(id, columnFamily).map { case Column(qualifier, value, _) => (new String(qualifier, utf8), value) }.toMap
    new Document(id, serializer.deserialize(map))
  }

  def insert(doc: Document): Unit = {
    val columns = serializer.serialize(doc.value).map { case (path, value) => Column(path.getBytes(utf8), value) }.toSeq
    table.put(doc.key, columnFamily, columns: _*)
  }

  def insert(json: JsValue): Document = {
    val doc = Document(json)
    insert(doc)
    doc
  }

  def remove(key: String): Unit = {
    remove(key.getBytes(utf8))
  }

  def remove(key: Array[Byte]): Unit = {
    table.delete(key, columnFamily)
  }

  def update(doc: Document): Unit = {

  }
}

trait AppendOnly {
  def remove(key: Array[Byte]): Unit = {
    throw new UnsupportedOperationException
  }

  def update(doc: Document): Unit = {
    throw new UnsupportedOperationException
  }
}
