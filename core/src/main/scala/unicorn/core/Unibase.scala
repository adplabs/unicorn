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

import unicorn.bigtable.{Column, BigTable}
import unicorn.json._
import unicorn.util._

/**
 * A Unibase is a database of documents. A collection of documents are called bucket.
 *
 * @author Haifeng Li
 */
class Unibase[+T <: BigTable](db: unicorn.bigtable.Database[T]) {
  val DefaultDocumentColumnFamily = "doc"

  /**
   * Returns a document collection.
   * @param name the name of collection/table.
   */
  def apply(name: String): Bucket = {

    // Bucket meta data table
    val metaTable = db(BucketMetaTableName)
    val serializer = new ColumnarJsonSerializer
    val map = metaTable.get(name, BucketMetaTableColumnFamily).map { case Column(qualifier, value, _) => (new String(qualifier, utf8), value.bytes) }.toMap
    val meta = serializer.deserialize(map).asInstanceOf[JsObject]

    new Bucket(db(name), meta)
  }

  /**
   * Returns a document collection.
   * @param name the name of document bucket.
   * @param meta the meta data of document bucket.
   */
  def apply(name: String, meta: JsObject): Bucket = {
    new Bucket(db(name), meta)
  }

  /**
   * Creates a document bucket.
   * @param name the name of bucket.
   * @param families the column family that documents resident.
   * @param locality a map of document fields to column families for storing of sets of fields in column families
   *                 separately to allow clients to scan over fields that are frequently used together efficient
   *                 and to avoid scanning over column families that are not requested.
   */
  def createBucket(name: String, families: Seq[String] = Seq[String](DefaultDocumentColumnFamily), locality: Map[String, String] = Map().withDefaultValue(DefaultDocumentColumnFamily)): Bucket = {
    db.createTable(name, families: _*)

    // If the meta data table doesn't exist, create it.
    if (!db.tableExists(BucketMetaTableName))
      db.createTable(BucketMetaTableName, BucketMetaTableColumnFamily)

    // Bucket meta data table
    val metaTable = db(BucketMetaTableName)
    val serializer = new ColumnarJsonSerializer
    val meta = BucketMeta(families, locality)
    val columns = serializer.serialize(meta).map { case (path, value) => Column(path.getBytes(utf8), value) }.toSeq
    metaTable.put(name, BucketMetaTableColumnFamily, columns: _*)

    apply(name, meta)
  }

  /**
   * Drops a document bucket. All column families in the table will be dropped.
   */
  def dropBucket(name: String): Unit = {
    db.dropTable(name)
  }
}

private object BucketMeta {
  def apply(families: Seq[String], locality: Map[String, String]): JsObject = {
    JsObject(
      "families" -> JsArray(families),
      "locality" -> JsObject(locality.mapValues(JsString(_))),
      DefaultLocalityField -> locality("") // hacking the default value of a map
    )
  }
}