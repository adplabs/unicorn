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

package unicorn

import unicorn.bigtable._
import unicorn.util._

/**
 * @author Haifeng Li
 */
package object index {
  type IndexableTable = BigTable with RowScan with FilterScan with Rollback with Counter
  type ColumnMap = collection.mutable.Map[String, collection.mutable.Map[ByteArray, Column]]

  private[index] val IndexTableNamePrefix = "unicorn_index_"
  private[index] val IndexColumnFamily = "index"
  private[index] val IndexMetaColumnFamily = "meta"
  private[index] val IndexColumnFamilies = Seq(IndexColumnFamily, IndexMetaColumnFamily)

  private[index] val IndexTableMetaRow: ByteArray = ".unicorn_index_meta."
  private[index] val IndexTableNewIndexId: ByteArray = ".new_index_id."
  private[index] val IndexTableStatColumnCount: ByteArray = ".count."

  private[index] val IndexDummyValue = ByteArray(Array[Byte](1))
  private[index] val UniqueIndexColumnQualifier = ByteArray(Array[Byte](1))

  private[index] object ColumnMap {
    def apply(families: Seq[ColumnFamily]): ColumnMap = {
      collection.mutable.Map(families.map { case ColumnFamily(family, columns) =>
        (family, collection.mutable.Map(columns.map { column => (ByteArray(column.qualifier), column) }: _*))
      }: _*)
    }

    def apply(family: String, columns: Seq[Column]): ColumnMap = {
      collection.mutable.Map(family -> collection.mutable.Map(columns.map { column => (ByteArray(column.qualifier), column) }: _*))
    }

    def apply(row: Row): ColumnMap = {
      apply(row.families)
    }
  }
}
