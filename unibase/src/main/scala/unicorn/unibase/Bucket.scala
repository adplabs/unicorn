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

  /** Gets a document. */
  def apply(id: Int): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(id: Long): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(id: String): Option[JsObject] = {
    apply(JsString(id))
  }

  /** Gets a document. */
  def apply(id: Date): Option[JsObject] = {
    apply(JsDate(id))
  }

  /** Gets a document. */
  def apply(id: UUID): Option[JsObject] = {
    apply(JsUUID(id))
  }

  /** Gets a document. */
  def apply(id: BsonObjectId): Option[JsObject] = {
    apply(JsObjectId(id))
  }

  /** Gets a document. */
  def apply(id: JsValue): Option[JsObject] = {
    val data = table.get(key2Bytes(id), families)
    assemble(data)
  }

  /** Assembles the document from multi-column family data. */
  private[unibase] def assemble(data: Seq[ColumnFamily]): Option[JsObject] = {
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

  /** A query may include a projection that specifies the fields of the document to return.
    * The projection limits the disk access and the network data transmission.
    * Note that the semantics is different from MongoDB due to the design of BigTable. For example, if a specified
    * field is a nested object, there is no easy way to read only the specified object in BigTable.
    * Intra-row scan may help but not all BigTable implementations support it. And if there are multiple
    * nested objects in request, we have to send multiple Get requests, which is not efficient. Instead,
    * we return the whole object of a column family if some of its fields are in request. This is usually
    * good enough for hot-cold data scenario. For instance of a bucket of events, each event has a
    * header in a column family and event body in another column family. In many reads, we only need to
    * access the header (the hot data). When only user is interested in the event details, we go to read
    * the event body (the cold data). Such a design is simple and efficient. Another difference from MongoDB is
    * that we don't support the excluded fields.
    *
    * @param projection an object that specifies the fields to return. The _id field should be included to
    *                   indicate which document to retrieve.
    * @return the projected document. The _id field will be always included.
    */
  def get(projection: JsObject): Option[JsObject] = {
    val key = projection("_id")
    if (key == JsNull || key == JsUndefined)
      throw new IllegalArgumentException("missing _id")

    val groups = projection.fields.toSeq.filter(_._1 != "_id").groupBy { case (field, _) =>
      val head = field.indexOf(".") match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    // We need to get the whole column family
    val families = groups.toSeq.map { case (family, _) => (family, Seq.empty) }

    val data = table.get(key2Bytes(key), families)
    val doc = assemble(data)
    if (doc.isDefined) doc.get("_id") = key // always set _id
    doc
  }

  /** A query may include a projection that specifies the fields of the document to return.
    * The projection limits the disk access and the network data transmission.
    * Note that the semantics is different from MongoDB due to the design of BigTable. For example, if a specified
    * field is a nested object, there is no easy way to read only the specified object in BigTable.
    * Intra-row scan may help but not all BigTable implementations support it. And if there are multiple
    * nested objects in request, we have to send multiple Get requests, which is not efficient. Instead,
    * we return the whole object of a column family if some of its fields are in request. This is usually
    * good enough for hot-cold data scenario. For instance of a bucket of events, each event has a
    * header in a column family and event body in another column family. In many reads, we only need to
    * access the header (the hot data). When only user is interested in the event details, we go to read
    * the event body (the cold data). Such a design is simple and efficient. Another difference from MongoDB is
    * that we don't support the excluded fields.
    *
    * @param fields top level fields to retrieve.
    * @return the projected document.
    */
  def get(id: JsValue, fields: String*): Option[JsObject] = {
    val projection = JsObject(fields.map(_ -> JsInt(1)): _*)
    projection("_id") = id
    get(projection)
  }

  /**
   * Upserts a document. If a document with same key exists, it will overwritten.
   * The _id field of document will be used as the primary key in bucket.
   * If the document doesn't have _id field, a random UUID will be generated as _id.
   *
   * @param doc the doucment.
   * @return the document id.
   */
  def upsert(doc: JsObject): JsValue = {
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
    key
  }

  /** Removes a document.
   *
   * @param id the document id.
   */
  def delete(id: JsValue): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException
    else
      table.delete(key2Bytes(id))
  }

  /** Updates a document. The supported update operators include
    *
    *  - $inc: Increments the value of the field by the specified amount.
    *  - $set: Sets the value of a field in a document.
    *  - $unset: Removes the specified field from a document.
    *
    * @param doc the document update operators.
    */
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

  /** The $set operator replaces the values of fields.
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

  /** The $unset operator deletes particular fields.
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

  /** Serialize key. */
  private[unibase] def key2Bytes(key: JsValue): Array[Byte] = {
    keySerializer.serialize(key)("$")
  }
}

class HBaseBucket(table: HBaseTable, meta: JsObject) extends Bucket(table, meta) {
  override def update(doc: JsObject): Unit = {
    super.update(doc)
    inc(doc("_id"), doc("$doc").asInstanceOf[JsObject])
  }

  /** The $inc operator accepts positive and negative values.
    *
    * If the field does not exist, $inc creates the field and sets the field to the specified value.
    */
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
