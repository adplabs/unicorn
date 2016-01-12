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
import unicorn.util.ByteArray

/** Buckets are the data containers of documents. A document is simply a JSON
  * object with a unique id (the _id field), which is similar to the primary key
  * in relational database. The key can be arbitrary JSON
  * value including Object and Array. However, they are some limitations in practice:
  *
  *  - The size limitation. The id of document serves as the row key in the underlying
  *    BigTable implementation. Most BigTable implementations have the upper limit of
  *    row key (often as 64KB).
  *  - For compound primary key (i.e. multiple fields), it is better use Array instead
  *    of Object as the container because it maintains the order of fields, which is
  *    important in the scan operations. When serializing an Object, the order of fields
  *    may be undefined (because of hashing) or simply ascending in field name.
  *  - The fields of compound primary key should be fixed size. In database with schema
  *    (e.g. relational database). The variable length field is padded to the maximum size.
  *    However, Unibase is schemaless database and thus we are lack of this type information.
  *    It is the developer's responsibility to make sure the proper use of primary key.
  *    Otherwise, the index and scan operations won't work correctly.
  *
  * @author Haifeng Li
  */
class Bucket(table: BigTable, meta: JsObject) {
  /** Document id field. */
  val _id = "_id"

  /** Returns the json path of a dot notation path as in MongoDB. */
  def jsonPath(path: String) = s"${JsonSerializer.root}${JsonSerializer.pathDelimiter}$path"

  /** Json path of id, i.e. the column qualifier in BigTable. */
  val idPath = jsonPath(_id)

  /** Document key serializer. */
  val keySerializer = new BsonSerializer
  /** Document value serializer. */
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
        (new String(qualifier, JsonSerializer.charset), value.bytes)
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
    val id = projection(_id)
    if (id == JsNull || id == JsUndefined)
      throw new IllegalArgumentException(s"missing ${_id}")

    val groups = projection.fields.toSeq.filter(_._1 != "_id").groupBy { case (field, _) =>
      val head = field.indexOf(JsonSerializer.pathDelimiter) match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    // We need to get the whole column family
    val families = groups.toSeq.map { case (family, _) => (family, Seq.empty) }

    val data = table.get(key2Bytes(id), families)
    val doc = assemble(data)
    doc.map(_(_id) = id)
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
    projection(_id) = id
    get(projection)
  }

  /**
    * Upserts a document. If a document with same key exists, it will overwritten.
    * The _id field of document will be used as the primary key in bucket.
    * If the document doesn't have _id field, a random UUID will be generated as _id.
    *
    * @param doc the document.
    * @return the document id.
    */
  def upsert(doc: JsObject): JsValue = {
    val id =  doc(_id) match {
      case JsUndefined | JsNull => doc(_id) = UUID.randomUUID
      case id => id
    }

    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) => Column(path.getBytes(JsonSerializer.charset), value) }.toSeq
      ColumnFamily(family, columns)
    }

    table.put(key2Bytes(id), families: _*)
    id
  }

  /**
    * Inserts a document. Different from upsert, this operation checks if the document already
    * exists first.
    *
    * @param doc the document.
    * @return true if the document is inserted, false if the document already existed.
    */
  def insert(doc: JsObject): Boolean = {
    val id = doc(_id)
    if (id == JsNull || id == JsUndefined)
      throw new IllegalArgumentException(s"missing ${_id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val checkFamily = locality(_id)
    val checkColumn = idPath.getBytes(JsonSerializer.charset)
    val key = key2Bytes(id)
    if (table.apply(key, checkFamily, checkColumn).isDefined) return false

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) =>
        Column(path.getBytes(JsonSerializer.charset), value)
      }.toSeq
      ColumnFamily(family, columns)
    }

    table.put(key, families: _*)
    true
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
      val id = doc(_id)
      if (id == JsNull || id == JsUndefined)
        throw new IllegalArgumentException(s"missing ${_id}")

      val $set = doc("$set")
      if ($set.isInstanceOf[JsObject]) set(id, $set.asInstanceOf[JsObject])

      val $unset = doc("$unset")
      if ($unset.isInstanceOf[JsObject]) unset(id, $unset.asInstanceOf[JsObject])
    }
  }

  /** The $set operator replaces the values of fields.
    *
    * The document key _id should not be set.
    *
    * If the field does not exist, $set will add a new field with the specified
    * value, provided that the new field does not violate a type constraint.
    *
    * In MongoDB, $set will create the embedded documents as needed to fulfill
    * the dotted path to the field. For example, for a $set {"a.b.c" : "abc"}, MongoDB
    * will create the embedded object "a.b" if it doesn't exist.
    * However, we don't support this behavior because of the performance considerations.
    * We suggest the the alternative syntax {"a.b" : {"c" : "abc"}}, which has the
    * equivalent effect.
    *
    * To set an element of an array by the zero-based index position,
    * concatenate the array name with the dot (.) and zero-based index position.
    *
    * @param id the id of document.
    * @param doc the fields to update.
    */
  def set(id: JsValue, doc: JsObject): Unit = {
    if (doc.fields.exists(_._1 == _id))
      throw new IllegalArgumentException(s"Invalid operation: set ${_id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(JsonSerializer.pathDelimiter) match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val (parents, children) = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map { case (field, _) =>
        field.lastIndexOf(JsonSerializer.pathDelimiter) match {
          case -1 => JsonSerializer.root
          case end => s"${JsonSerializer.root}${JsonSerializer.pathDelimiter}${field.substring(0, end)}"
        }
      }.distinct.map { parent =>
        ByteArray(valueSerializer.nullstring(parent))
      }

      ((family, columns), fields.map(_._1).filter(_ != id))
    }.unzip

    val key = key2Bytes(id)
    val parentValues = table.get(key, parents)

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.foldLeft(Seq[Column]()) { case (seq, (field, value)) =>
        seq ++ valueSerializer.serialize(value, jsonPath(field)).map { case (path, value) => Column(path.getBytes(JsonSerializer.charset), value) }.toSeq
      }
      ColumnFamily(family, columns)
    }

    table.put(key, families: _*)
  }

  /** The $unset operator deletes particular fields.
    *
    * The document key _id should not be unset.
    *
    * If the field does not exist, then $unset does nothing (i.e. no operation).
    *
    * When used with $ to match an array element, $unset replaces the matching element
    * with undefined rather than removing the matching element from the array.
    * This behavior keeps consistent the array size and element positions.
    *
    * @param id the id of document.
    * @param doc the fields to delete.
    */
  def unset(id: JsValue, doc: JsObject): Unit = {
    if (doc.fields.exists(_._1 == _id))
      throw new IllegalArgumentException(s"Invalid operation: unset ${_id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(JsonSerializer.pathDelimiter) match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val path = fields.map { case (field, _) => ByteArray(jsonPath(field).getBytes(JsonSerializer.charset)) }
      (family, path)
    }
    println(id)
    println(families)

    table.delete(key2Bytes(id), families)
  }

  /** Serialize document id. */
  private[unibase] def key2Bytes(id: JsValue): Array[Byte] = {
    keySerializer.toBytes(id)
  }
}

class HBaseBucket(table: HBaseTable, meta: JsObject) extends Bucket(table, meta) {
    override def update(doc: JsObject): Unit = {
      super.update(doc)

      val $inc = doc("$inc")
      if ($inc.isInstanceOf[JsObject]) inc(doc(_id), $inc.asInstanceOf[JsObject])
    }

  /** The $inc operator accepts positive and negative values.
    *
    * If the field does not exist, $inc creates the field and sets the field to the specified value.
    *
    * @param id the id of document.
    * @param doc the fields to increase/decrease.
    */
  def inc(id: JsValue, doc: JsObject): Unit = {
    if (doc.fields.exists(_._1 == _id))
      throw new IllegalArgumentException(s"Invalid operation: inc ${_id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(JsonSerializer.pathDelimiter) match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, JsLong(value)) => (ByteArray(jsonPath(field).getBytes(JsonSerializer.charset)), value)
        case (field, JsInt(value)) => (ByteArray(jsonPath(field).getBytes(JsonSerializer.charset)), value.toLong)
        case (_, value) => throw new IllegalArgumentException(s"Invalid value: $value")
      }
      (family, columns)
    }

    table.increaseCounter(key2Bytes(id), families)
  }

  /** Use checkAndPut for insert. */
  override def insert(doc: JsObject): Boolean = {
    val id = doc(_id)
    if (id == JsNull || id == JsUndefined)
      throw new IllegalArgumentException(s"missing ${_id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) =>
        Column(path.getBytes(JsonSerializer.charset), value)
      }.toSeq
      ColumnFamily(family, columns)
    }

    val checkFamily = locality(_id)
    val checkColumn = idPath.getBytes(JsonSerializer.charset)
    table.checkAndPut(key2Bytes(id), checkFamily, checkColumn, families: _*)
  }
}
