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

import java.util.{UUID, Date}
import unicorn.json._
import unicorn.bigtable._, hbase.HBaseTable
import unicorn.oid.BsonObjectId
import unicorn.util._

class HBaseBucket(table: HBaseTable, meta: JsObject) extends Bucket(table, meta) {
  /** Visibility expression which can be associated with a cell.
    * When it is set with a Mutation, all the cells in that mutation will get associated with this expression.
    */
  def setCellVisibility(expression: String): Unit = table.setCellVisibility(expression)

  /** Returns the current visibility expression setting. */
  def getCellVisibility: String = table.getCellVisibility

  /** Visibility labels associated with a Scan/Get deciding which all labeled data current scan/get can access. */
  def setAuthorizations(labels: String*): Unit = table.setAuthorizations(labels: _*)

  /** Returns the current authorization labels. */
  def getAuthorizations: Seq[String] = table.getAuthorizations

  /** Gets a document. */
  def apply(asOfDate: Date, id: Int, fields: String*): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: Long, fields: String*): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: String, fields: String*): Option[JsObject] = {
    apply(JsString(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: Date, fields: String*): Option[JsObject] = {
    apply(JsDate(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: UUID, fields: String*): Option[JsObject] = {
    apply(JsUUID(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: BsonObjectId, fields: String*): Option[JsObject] = {
    apply(JsObjectId(id))
  }

  /** Gets a document.
    *
    * @param id document id.
    * @param fields top level fields to retrieve.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(asOfDate: Date, id: JsValue, fields: String*): Option[JsObject] = {
    if (fields.isEmpty) {
      val data = table.getAsOf(asOfDate, getKey(id), families)
      assemble(data)
    } else {
      val projection = JsObject(fields.map(_ -> JsInt(1)): _*)
      projection($id) = id
      get(asOfDate, projection)
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
    * @param asOfDate snapshot of document is at this point of time
    * @param projection an object that specifies the fields to return. The _id field should be included to
    *                   indicate which document to retrieve.
    * @return the projected document. The _id field will be always included.
    */
  def get(asOfDate: Date, projection: JsObject): Option[JsObject] = {
    val id = getId(projection)
    val families = project(projection)
    val data = table.getAsOf(asOfDate, getKey(id), families)
    val doc = assemble(data)
    doc.map(_($id) = id)
    doc
  }

  /** Updates a document. The supported update operators include
    *
    *  - \$set: Sets the value of a field in a document.
    *  - \$unset: Removes the specified field from a document.
    *  - \$inc: Increments the value of the field by the specified amount.
    *  - \$rollback: Rolls back to previous version.
    *
    * @param doc the document update operators.
    */
  override def update(doc: JsObject): Unit = {
    super.update(doc)

    val id = getId(doc)

    val $inc = doc("$inc")
    if ($inc.isInstanceOf[JsObject]) inc(id, $inc.asInstanceOf[JsObject])

    val $rollback= doc("$rollback")
    if ($rollback.isInstanceOf[JsObject]) rollback(id, $rollback.asInstanceOf[JsObject])
  }

  /** The \$inc operator accepts positive and negative values.
    *
    * The field must exist. Increase a nonexist counter will create
    * the column in BigTable. However, it won't show up in the parent
    * object. We could check and add it to the parent. However it is
    * expensive and we lose many benefits of built-in counters (efficiency
    * and atomic). It is the user's responsibility to create the counter
    * first.
    *
    * @param id the id of document.
    * @param doc the fields to increase/decrease.
    */
  def inc(id: JsValue, doc: JsObject): Unit = {
    if (doc.fields.exists(_._1 == $id))
      throw new IllegalArgumentException(s"Invalid operation: inc ${$id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(JsonSerializer.pathDelimiter) match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, JsLong(value)) => (ByteArray(getBytes(jsonPath(field))), value)
        case (field, JsInt(value)) => (ByteArray(getBytes(jsonPath(field))), value.toLong)
        case (_, value) => throw new IllegalArgumentException(s"Invalid value: $value")
      }
      (family, columns)
    }

    table.addCounter(getKey(id), families)
  }

  /** The \$rollover operator roll particular fields back to previous version.
    *
    * The document key _id should not be rollover.
    *
    * Note that if rollover, the latest version will be lost after a major compaction.
    *
    * @param id the id of document.
    * @param doc the fields to delete.
    */
  def rollback(id: JsValue, doc: JsObject): Unit = {
    if (doc.fields.exists(_._1 == $id))
      throw new IllegalArgumentException(s"Invalid operation: rollover ${$id}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) =>
      val head = field.indexOf(JsonSerializer.pathDelimiter) match {
        case -1 => field
        case end => field.substring(0, end)
      }
      locality(head)
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, _) => ByteArray(getBytes(jsonPath(field)))
      }
      (family, columns)
    }

    table.rollback(getKey(id), families)
  }

  /** Use checkAndPut for insert. */
  override def insert(doc: JsObject): Unit = {
    val id = getId(doc)
    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = valueSerializer.serialize(json).map { case (path, value) =>
        Column(getBytes(path), value)
      }.toSeq
      ColumnFamily(family, columns)
    }

    val checkFamily = locality($id)
    val checkColumn = getBytes(idPath)
    if (!table.checkAndPut(getKey(id), checkFamily, checkColumn, families))
      throw new IllegalArgumentException(s"Document $id already exists")
  }

  /*
  def find(projection: JsObject, query: JsObject): Iterator[JsObject] = {
    val families = project(projection)

    val it = tenant match {
      case None => table.scanAll(families)
      case Some(tenant) => table.scanPrefix(getBytes(tenant), families)
    }

    it.map { data =>
      assemble(data.families).get
    }
  }

  private def queryFilter(query: JsObject): ScanFilter.Expression = {

  }
  */
}
