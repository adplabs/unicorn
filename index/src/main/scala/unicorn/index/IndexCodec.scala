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

import unicorn.bigtable.{Cell, Column}
import unicorn.util.ByteArray

/**
 * Calculate the cell(s) in the index table for a given column set in the base table.
 * In case of text index, we have multiple index entries (for each word).
 *
 * @author Haifeng Li
 */
trait IndexCodec {
  /**
   * Given a row, calculate the index entries.
   * @param row the row key.
   * @param columns a map of family to map of qualifier to cell.
   * @return a seq of index entries.
   */
  def apply(row: ByteArray, columns: RowMap): Seq[Cell]
}
