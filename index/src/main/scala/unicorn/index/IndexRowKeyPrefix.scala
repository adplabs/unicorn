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

import unicorn.bigtable.Column

/**
 * Optionally, row key in the index table may have specific prefix,
 * e.g. tenant id, index name (all index in one table), etc.
 *
 * @author Haifeng Li
 */
trait IndexRowKeyPrefix {
  def apply(row: Array[Byte], family: Array[Byte], columns: Seq[Column]): Array[Byte]
}

/**
 * By default index has no row key prefix.
 */
object NoRowKeyPrefix extends IndexRowKeyPrefix {
  val empty = Array[Byte]()
  override def apply(row: Array[Byte], family: Array[Byte], columns: Seq[Column]): Array[Byte] = {
    empty
  }
}