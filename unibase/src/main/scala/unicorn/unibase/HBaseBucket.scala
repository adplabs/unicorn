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
}
