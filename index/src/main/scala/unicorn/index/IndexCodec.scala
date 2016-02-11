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

import java.nio.ByteBuffer

import unicorn.bigtable.{Cell, Column}
import unicorn.util.ByteArray

/** Calculate the cell(s) in the index table for a given column set in the base table.
  * In case of text index, we have multiple index entries (for each word).
  *
  * @author Haifeng Li
  */
trait IndexCodec {
  /** Index definition. */
  val index: Index

  /** Workspace to encode index row keys. */
  val buffer = ByteBuffer.allocate(16 * 1024)

  /** Given a row, calculate the index entries.
    * @param row the row key.
    * @param columns a map of family to map of qualifier to cell.
    * @return a seq of index entries.
    */
  def apply(row: ByteArray, columns: ColumnMap): Seq[Cell]

  /** A helper function useful for testing. */
  def apply(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Seq[Cell] = {
    apply(row, ColumnMap(family, Seq(Column(column, value))))
  }

  /** A helper function useful for testing. */
  def apply(row: ByteArray, family: String, columns: Column*): Seq[Cell] = {
    apply(row, ColumnMap(family, columns))
  }

  /** Optional tenant id. */
  val tenant: Option[ByteArray] = None

  /** Clear buffer. */
  def clear: Unit = {
    buffer.clear
    buffer.putShort(index.id.toShort)
    if (tenant.isDefined) buffer.put(tenant.get.length.toByte).put(tenant.get)
    else buffer.put(0.toByte)
  }
}

object IndexCodec {
  def apply(index: Index): IndexCodec = {
    index.indexType match {
      case IndexType.Hashed => new HashIndexCodec(index)
      case IndexType.Text => new TextIndexCodec(index)
      case _ => if (index.columns.size == 1) new SingleColumnIndexCodec(index) else new CompositeIndexCodec(index)
    }
  }
}