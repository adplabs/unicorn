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

import unicorn.bigtable.BigTable

/**
 * @author Haifeng Li
 */
class Unibase[+T <: BigTable](db: unicorn.bigtable.Database[T]) {
  val DefaultDocumentColumnFamily = "doc"

  /**
   * Returns a document collection.
   * @param name the name of collection/table.
   * @param family the column family that documents resident.
   */
  def apply(name: String, family: String = DefaultDocumentColumnFamily): DocumentCollection = {
    new DocumentCollection(db(name), family)
  }

  /**
   * Creates a document collection.
   * @param name the name of collection.
   * @param families the column family that documents resident.
   */
  def createCollection(name: String, families: Seq[String] = Seq[String](DefaultDocumentColumnFamily)): Unit = {
    db.createTable(name, families: _*)
  }

  /**
   * Drops a document collection. All column families in the table will be dropped.
   */
  def dropCollection(name: String): Unit = {
    db.dropTable(name)
  }
}
