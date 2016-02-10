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
import scala.collection.mutable.ArrayBuffer
import unicorn.json._
import unicorn.bigtable._
import unicorn.oid.BsonObjectId
import unicorn.util.ByteArray

/** Tables are the data containers of documents. A document is simply a JSON
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
class Table(table: BigTable, meta: JsObject) {
  /** Document id field. */
  val $id = Unibase.$id

  /** Returns the json path of a dot notation path as in MongoDB. */
  private[unibase] def jsonPath(path: String) = s"${JsonSerializer.root}${JsonSerializer.pathDelimiter}$path"

  /** Json path of id, i.e. the column qualifier in BigTable. */
  val idPath = jsonPath($id)

  /** Document key serializer. */
  val keySerializer = new BsonSerializer
  /** Document value serializer. */
  val valueSerializer = new ColumnarJsonSerializer

  /** Column families storing document fields. There may be other column families in the table
    * for meta data or index.
    */
  val families: Seq[(String, Seq[ByteArray])] = meta.families.asInstanceOf[JsArray].elements.map { e =>
    (e.toString, Seq.empty)
  }

  /** The table name. */
  val name = table.name

  /** A map of document fields to column families for storing of sets of fields in column families
    * separately to allow clients to scan over fields that are frequently used together efficient
    * and to avoid scanning over column families that are not requested.
    */
  private[unibase] val locality: Map[String, String] = {
    val default = meta.apply(DefaultLocalityField).toString
    val map = meta.locality.asInstanceOf[JsObject].fields.mapValues(_.toString).toMap
    map.withDefaultValue(default)
  }

  private[unibase] def getBytes(s: String) = s.getBytes(JsonSerializer.charset)

  /** True if the table is append only. */
  val appendOnly: Boolean = meta.appendOnly

  /** Optional tenant id in case of multi-tenancy. */
  var tenant: Option[JsValue] = None

  /** Gets a document. */
  def apply(id: Int, fields: String*): Option[JsObject] = {
    apply(JsInt(id))
  }

  /** Gets a document. */
  def apply(id: Long, fields: String*): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(id: String, fields: String*): Option[JsObject] = {
    apply(JsString(id))
  }

  /** Gets a document. */
  def apply(id: Date, fields: String*): Option[JsObject] = {
    apply(JsDate(id))
  }

  /** Gets a document. */
  def apply(id: UUID, fields: String*): Option[JsObject] = {
    apply(JsUUID(id))
  }

  /** Gets a document. */
  def apply(id: BsonObjectId, fields: String*): Option[JsObject] = {
    apply(JsObjectId(id))
  }

  /** Gets a document.
    *
    * @param id document id.
    * @param fields top level fields to retrieve.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(id: JsValue, fields: String*): Option[JsObject] = {
    if (fields.isEmpty) {
      val data = table.get(getKey(id), families)
      assemble(data)
    } else {
      val projection = JsObject(fields.map(_ -> JsInt(1)): _*)
      projection($id) = id
      get(projection)
    }
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

  /** Returns the _id of a document. Throws exception if missing. */
  private[unibase] def getId(doc: JsObject): JsValue = {
    val id = doc($id)

    require(id != JsNull && id != JsUndefined, s"missing ${$id}")
    require(!id.isInstanceOf[JsCounter], "${$id} cannot be JsCounter")

    id
  }

  /** A query may include a projection that specifies the fields of the document to return.
    * The projection limits the disk access and the network data transmission.
    * Note that the semantics is different from MongoDB due to the design of BigTable. For example, if a specified
    * field is a nested object, there is no easy way to read only the specified object in BigTable.
    * Intra-row scan may help but not all BigTable implementations support it. And if there are multiple
    * nested objects in request, we have to send multiple Get requests, which is not efficient. Instead,
    * we return the whole object of a column family if some of its fields are in request. This is usually
    * good enough for hot-cold data scenario. For instance of a table of events, each event has a
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
    val id = getId(projection)
    val families = project(projection)

    val data = table.get(getKey(id), families)
    val doc = assemble(data)
    doc
  }

  /** Maps a  projection to the seq of column families to fetch. */
  private[unibase] def project(projection: JsObject): Seq[(String, Seq[ByteArray])] = {

    val groups = projection.fields.map(_._1).groupBy { field =>
      getFamily(field)
    }

    // We need to get the whole column family.
    // If the path is to a nested object, we will miss the children if not read the
    // whole column family. If BigTable implementations support reading columns
    // by prefix, we can do it more efficiently.
    groups.toSeq.map { case (family, _) => (family, Seq.empty) }
  }

  /** Upserts a document. If a document with same key exists, it will overwritten.
    * The _id field of document will be used as the primary key in the table.
    * If the document doesn't have _id field, a random UUID will be generated as _id.
    *
    * @param doc the document.
    * @return the document id.
    */
  def upsert(doc: JsObject): JsValue = {
    val id = doc($id) match {
      case JsUndefined | JsNull => doc($id) = UUID.randomUUID
      case id => id
    }

    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) => Column(getBytes(path), value) }.toSeq
      ColumnFamily(family, columns)
    }

    table.put(getKey(id), families)
    id
  }

  /** Inserts a document. Different from upsert, this operation checks if the document already
    * exists first.
    *
    * @param doc the document.
    * @return true if the document is inserted, false if the document already existed.
    */
  def insert(doc: JsObject): Unit = {
    val id = getId(doc)
    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val checkFamily = locality($id)
    val checkColumn = getBytes(idPath)
    val key = getKey(id)
    require(table.apply(key, checkFamily, checkColumn).isEmpty, s"Document $id already exists")

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) =>
        Column(getBytes(path), value)
      }.toSeq
      ColumnFamily(family, columns)
    }

    table.put(key, families)
  }

  /** Removes a document.
    *
    * @param id the document id.
    */
  def delete(id: JsValue): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException
    else
      table.delete(getKey(id))
  }

  /** Updates a document. The supported update operators include
    *
    *  - \$set: Sets the value of a field in a document.
    *  - \$unset: Removes the specified field from a document.
    *
    * @param doc the document update operators.
    */
  def update(doc: JsObject): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException
    else {
      val id = getId(doc)

      val $set = doc("$set")
      if ($set.isInstanceOf[JsObject]) set(id, $set.asInstanceOf[JsObject])

      val $unset = doc("$unset")
      if ($unset.isInstanceOf[JsObject]) unset(id, $unset.asInstanceOf[JsObject])
    }
  }

  /** Returns the column family of a field. */
  private[unibase] def getFamily(field: String): String = {
    val head = field.indexOf(JsonSerializer.pathDelimiter) match {
      case -1 => field
      case end => field.substring(0, end)
    }
    locality(head)
  }

  /** The \$set operator replaces the values of fields.
    *
    * The document key _id should not be set.
    *
    * If the field does not exist, \$set will add a new field with the specified
    * value, provided that the new field does not violate a type constraint.
    *
    * In MongoDB, \$set will create the embedded documents as needed to fulfill
    * the dotted path to the field. For example, for a \$set {"a.b.c" : "abc"}, MongoDB
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
    require(!doc.fields.exists(_._1 == $id), s"Invalid operation: set ${$id}")

    // Group field by locality
    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      getFamily(field)
    }

    // Map from parent to the fields to update
    val children = scala.collection.mutable.Map[(String, ByteArray), Seq[String]]().withDefaultValue(Seq.empty)
    val parents = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map { case (field, _) =>
        val (parent, name) = field.lastIndexOf(JsonSerializer.pathDelimiter) match {
          case -1 => (JsonSerializer.root, field)
          case end => (jsonPath(field.substring(0, end)), field.substring(end+1))
        }

        children((family, parent)) =  children((family, parent)) :+ name
        parent
      }.distinct.map { parent =>
        ByteArray(getBytes(parent))
      }

      (family, columns)
    }

    val key = getKey(id)

    // Get the update to parents which get new child fields.
    val pathUpdates = table.get(key, parents).map { case ColumnFamily(family, parents) =>
      val updates = parents.map { parent =>
        val value = new ArrayBuffer[Byte]()
        value ++= parent.value.bytes
        val update = children((family, parent.qualifier)).foldLeft[Option[ArrayBuffer[Byte]]](None) { (parent, child) =>
          // appending the null terminal
          val child0 = valueSerializer.serialize(child)
          if (isChild(value, child0)) parent
          else {
            value ++= child0
            Some(value)
          }
        }

        update.map { value => Column(parent.qualifier, value.toArray) }
      }.filter(_.isDefined).map(_.get)
      ColumnFamily(family, updates)
    }.filter(!_.columns.isEmpty)

    // Columns to update
    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.foldLeft(Seq[Column]()) { case (seq, (field, value)) =>
        seq ++ valueSerializer.serialize(value, jsonPath(field)).map {
          case (path, value) => Column(getBytes(path), value)
        }.toSeq
      }
      ColumnFamily(family, columns)
    }

    table.put(key, pathUpdates ++ families)
  }

  /** Given the column value of an object (containing its children field names), return
    * true if node is its child.
    */
  private[unibase] def isChild(parent: ArrayBuffer[Byte], node: Array[Byte]): Boolean = {
    def isChild(start: Int): Boolean = {
      for (i <- 0 until node.size) {
        if (parent(start + i) != node(i)) return false
      }
      true
    }

    var start = 1
    while (start < parent.size) {
      if (isChild(start)) return true
      while (start < parent.size && parent(start) != 0) start = start + 1
      start = start + 1
    }
    false
  }

  /** The \$unset operator deletes particular fields.
    *
    * The document key _id should not be unset.
    *
    * If the field does not exist, then \$unset does nothing (i.e. no operation).
    *
    * When deleting an array element, \$unset replaces the matching element
    * with undefined rather than removing the matching element from the array.
    * This behavior keeps consistent the array size and element positions.
    *
    * Note that we don't really delete the field but set it as JsUndefined
    * so that we keep the history and be able to time travel back. Otherwise,
    * we will lose the history after a major compaction.
    *
    * @param id the id of document.
    * @param doc the fields to delete.
    */
  def unset(id: JsValue, doc: JsObject): Unit = {
    require(!doc.fields.exists(_._1 == $id), s"Invalid operation: unset ${$id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      getFamily(field)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, _) => Column(getBytes(jsonPath(field)), valueSerializer.undefined)
      }
      ColumnFamily(family, columns)
    }

    table.put(getKey(id), families)
  }

  /** Serialize document id. */
  private[unibase] def getKey(id: JsValue): Array[Byte] = {
    val key = tenant match {
      case None => id
      case Some(tenant) => JsArray(tenant, id)
    }
    
    keySerializer.toBytes(key)
  }
}
