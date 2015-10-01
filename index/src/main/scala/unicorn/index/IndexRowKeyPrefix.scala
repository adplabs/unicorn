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

/**
 * Optionally, row key in the index table may have specific prefix,
 * e.g. tenant id, index name (all index in one table), etc.
 *
 * @author Haifeng Li
 */
trait IndexRowKeyPrefix {
  /**
   * Returns a (optionally) prefix for index table row key.
   * @param index index meta data.
   * @param baseTableRowKey the row key of base table.
   * @return optional index table row key prefix.
   */
  def apply(index: Index, baseTableRowKey: Array[Byte]): Array[Byte]
}

/**
 * Suppose the base table row key with tenant id as the prefix. Corresponding, the index
 * table may have the tenant id as the prefix too to ensure proper sharding. The tenant id
 * should be of fixed size
 *
 * @param tenantIdSize the tenant id size.
 *
 * @author Haifeng Li
 */
class TenantIdPrefix(tenantIdSize: Int) extends IndexRowKeyPrefix {

  override def apply(index: Index, baseTableRowKey: Array[Byte]): Array[Byte] = {
    val prefix = new Array[Byte](tenantIdSize)
    Array.copy(baseTableRowKey, 0, prefix, 0, tenantIdSize)
    prefix
  }

  override def toString = s"tenant($tenantIdSize)"
}

/**
 * Multiple indices may be stored in one BigTable. Note that BigTable was designed
 * for a few very large tables rather than many small table. With multiple indices in
 * one table, it helps to reduce the number of tables.
 *
 * @param id the index id.
 *
 * @author Haifeng Li
 */
class IndexIdPrefix(id: Int) extends IndexRowKeyPrefix {
  val prefix = ByteBuffer.allocate(4).putInt(id).array

  override def apply(index: Index, baseTableRowKey: Array[Byte]): Array[Byte] = {
    prefix
  }

  override def toString = s"index($id)"
}
