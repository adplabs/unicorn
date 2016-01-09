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
import unicorn.bigtable._, hbase.HBaseTable
import unicorn.oid.BsonObjectId
import unicorn.util.{ByteArray, utf8}

/**
 * Buckets are the data containers of documents.
 *
 * @author Haifeng Li
 */
class Bucket(table: BigTable, meta: JsObject) {
  val keySerializer = new BsonSerializer
  val valueSerializer = new ColumnarJsonSerializer

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
  }

  /** True if the bucket is append only. */
  val appendOnly: Boolean = meta.appendOnly

  /** Get a document. */
  def apply(id: Int): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Get a document. */
  def apply(id: Long): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Get a document. */
  def apply(id: String): Option[JsObject] = {
    apply(JsString(id))
  }

  /** Get a document. */
  def apply(id: Date): Option[JsObject] = {
    apply(JsDate(id))
  }

  /** Get a document. */
  def apply(id: UUID): Option[JsObject] = {
    apply(JsUUID(id))
  }

  /** Get a document. */
  def apply(id: BsonObjectId): Option[JsObject] = {
    apply(JsObjectId(id))
  }

  /** Get a document. */
  def apply(id: JsValue): Option[JsObject] = {
    val data = table.get(key2Bytes(id), families)

    val objects = data.map { case ColumnFamily(family, columns) =>
      val map = columns.map { case Column(qualifier, value, _) =>
        (new String(qualifier, utf8), value.bytes)
      }.toMap
      val json = valueSerializer.deserialize(map)
      json.asInstanceOf[JsObject]
    }

    if (objects.size == 0)
      None
    else if (objects.size == 1)
      Some(objects(0))
    else {
      val fold = objects.foldLeft(JsObject()) { (doc, family) =>
        doc.fields ++= family.fields
        doc
      }
      Some(fold)
    }
  }

  /** Serialize key. */
  def key2Bytes(key: JsValue): Array[Byte] = {
    keySerializer.serialize(key)("$")
  }

  /** A query may include a projection that specifies the fields from the matching documents to return.
    *  The projection limits the amount of data that Unibase returns to the client over the network.
    *
    * @param doc
    * @return
    */
  def get(doc: JsObject): JsObject = {
    doc
  }

  /**
   * Upsert a document. If a document with same key exists, it will overwritten.
   * The _id field of document will be used as the primary key in bucket.
   * If the document doesn't have _id field, a random UUID will be generated as _id.
   */
  def upsert(doc: JsObject): JsObject = {
    val id = doc("_id")
    val key = if (id == JsUndefined || id == JsNull) {
      doc("_id") = UUID.randomUUID
    } else id

    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) => Column(path.getBytes(utf8), value) }.toSeq
      ColumnFamily(family, columns)
    }

    table.put(key2Bytes(key), families: _*)
    doc
  }

  /** Remove a document. */
  def delete(key: JsValue): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException
    else
      table.delete(key2Bytes(key))
  }

  /** Update a document. */
  def update(doc: JsObject): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException
    else {
      val key = doc("_id")
      if (key == JsNull || key == JsUndefined)
        throw new IllegalArgumentException("missing _id")

      set(key, doc("$set").asInstanceOf[JsObject])
      unset(key, doc("$unset").asInstanceOf[JsObject])
    }
  }

  /** The $set operator replaces the value of a field with the specified value.
    *
    * The document key _id should not be set.
    *
    * If the field does not exist, $set will add a new field with the specified
    * value, provided that the new field does not violate a type constraint.
    *
    * For a dotted path of a non-existent field, $set will create
    * the embedded documents as needed to fulfill the dotted path to the field.
    *
    * To set an element of an array by the zero-based index position,
    * concatenate the array name with the dot (.) and zero-based index position.
    */
  def set(key: JsValue, fields: JsObject): Unit = {

  }

  /** The $unset operator deletes a particular field.
    *
    * The document key _id should not be set.
    *
    * If the field does not exist, then $unset does nothing (i.e. no operation).
    *
    * When used with $ to match an array element, $unset replaces the matching element
    * with undefined rather than removing the matching element from the array.
    * This behavior keeps consistent the array size and element positions.
    */
  def unset(key: JsValue, doc: JsObject): Unit = {
    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(".") match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val path = fields.map { case (field, _) => ByteArray(field.getBytes(utf8)) }
      (family, path)
    }

    table.delete(key2Bytes(key), families)
  }
}

class HBaseBucket(table: HBaseTable, meta: JsObject) extends Bucket(table, meta) {
  override def update(doc: JsObject): Unit = {
    super.update(doc)
    inc(doc("_id"), doc("$doc").asInstanceOf[JsObject])
  }

  /** The $inc operator accepts positive and negative values.
    *
    * If the field does not exist, $inc creates the field and sets the field to the specified value. */
  def inc(key: JsValue, doc: JsObject): Unit = {
    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(".") match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, JsLong(value)) => (ByteArray(field.getBytes(utf8)), value)
        case (field, JsInt(value)) => (ByteArray(field.getBytes(utf8)), value.toLong)
        case (_, value) => throw new IllegalArgumentException(s"Invalid value: $value")
      }
      (family, columns)
    }

    table.increaseCounter(key2Bytes(key), families)
  }
}
