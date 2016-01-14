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

import java.util.Date
import unicorn.json._
import unicorn.bigtable._, hbase.HBaseTable

class HBaseBucket(table: HBaseTable, meta: JsObject) extends Bucket(table, meta) {
  /* Hbase counter must be 64 bits.
   * TODO: How to encoding counters including data type? It is not plain Long!
   */
  /*
    override def update(doc: JsObject): Unit = {
      super.update(doc)

      val $inc = doc("$inc")
      if ($inc.isInstanceOf[JsObject]) inc(doc(_id), $inc.asInstanceOf[JsObject])
    }
    */

  /** The $inc operator accepts positive and negative values.
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
  /*
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

    table.addCounter(key2Bytes(id), families)
  }
  */

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

  /** Gets a document. */
  def apply(id: JsValue, asOfDate: Date): Option[JsObject] = {
    val data = table.getAsOf(asOfDate, key2Bytes(id), families)
    assemble(data)
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
  def get(projection: JsObject, asOfDate: Date): Option[JsObject] = {
    val (id, families) = project(projection)
    val data = table.getAsOf(asOfDate, key2Bytes(id), families)
    val doc = assemble(data)
    doc.map(_(_id) = id)
    doc
  }
}
