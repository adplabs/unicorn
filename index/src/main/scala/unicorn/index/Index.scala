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

package unicorn.index

import org.apache.hadoop.io.Text
import unicorn.bigtable._

/**
 * Secondary index.
 * 
 * @author Haifeng Li
 */
trait Index {
  case class Column(family: Array[Byte], qualifier: Array[Byte])
  case class IndexMeta(indexTable: BigTable, columns: Column*)

  /** Base table */
  val baseTable: BigTable with RowScan
  /** Index tables. A column may appears in multiple indices (compound index) */
  val indexTables = collection.mutable.Map[(Text, Text), IndexMeta]()

  /** Close the base table and the index table */
  def close: Unit = {
    baseTable.close
    indexTables.foreach { case (_, table) => table.close }
  }

  def createIndex(family: Array[Byte], column: Array[Byte]): Unit = {

  }

  def dropIndex(family: Array[Byte], column: Array[Byte]): Unit = {

  }
}

class IndexMeta(db: Database) {
  /** Index meta data table */
  val metaTable = db("unicorn.meta.index")

}