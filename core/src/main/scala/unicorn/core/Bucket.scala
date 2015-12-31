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
import unicorn.util.{ByteArray, utf8}

/**
 * Buckets are the data containers of documents.
 *
 * @author Haifeng Li
 */
class Bucket(table: BigTable, meta: JsObject) {
  val serializer = new ColumnarJsonSerializer

  /**
   * Column families storing document fields. There may be other column families in the table
   * for meta data or index.
   */
  val families: Seq[(String, Seq[ByteArray])] = meta.families.asInstanceOf[JsArray].elements.map { e =>
    (e.toString, Seq.empty)
  }

  /**
   * A map of document fields to column families for storing of sets of fields in column families
   * separately to allow clients to scan over fields that are frequently used together efficient
   * and to avoid scanning over column families that are not requested.
   */
  val locality: Map[String, String] = {
    val default = meta.apply(DefaultLocalityField).toString
    val map = meta.locality.asInstanceOf[JsObject].fields.mapValues(_.toString).toMap
    map.withDefaultValue(default)
    map
  }

  def apply(id: String): Document = {
    apply(id.getBytes(utf8))
  }

  def apply(id: Array[Byte]): Document = {
    val objects = table.get(id, families).map { case ColumnFamily(family, columns) =>
      val map = columns.map { case Column(qualifier, value, _) =>
        (new String(qualifier, utf8), value.bytes)
      }.toMap
      val json = serializer.deserialize(map)
      json.asInstanceOf[JsObject]
    }

    val doc = objects.foldLeft(JsObject()) { (doc, family) =>
      doc.fields ++= family.fields
      doc
    }

    new Document(id, doc)
  }

  def upsert(doc: Document): Unit = {
    val groups = doc.value.fields.toSeq.groupBy { case (field, value) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = serializer.serialize(json).map { case (path, value) => Column(path.getBytes(utf8), value) }.toSeq
      ColumnFamily(family, columns)
    }

    table.put(doc.key, families: _*)
  }

  def upsert(json: JsObject): Document = {
    val doc = Document(json)
    upsert(doc)
    doc
  }

  def remove(key: String): Unit = {
    remove(key.getBytes(utf8))
  }

  def remove(key: Array[Byte]): Unit = {
    table.delete(key, families)
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
