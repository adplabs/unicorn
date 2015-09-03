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

package unicorn.oid

import java.util.{Arrays, Date, UUID}
import java.nio.ByteBuffer
import unicorn.util._

/**
 * Abstract Object Id type.
 */
class ObjectId(val id: Array[Byte]) {
  override lazy val toString = bytes2Hex(id)

  override def equals(that: Any): Boolean = {
    that.isInstanceOf[ObjectId] && Arrays.equals(id, that.asInstanceOf[ObjectId].id)
  }

  override lazy val hashCode: Int = Arrays.hashCode(id)
}

object ObjectId {
  def apply(id: Array[Byte]) = new ObjectId(id)
  def apply(id: String) = new ObjectId(id.getBytes("UTF-8"))

  def apply(id: Int) = {
    val array = Array[Byte](4)
    val buffer = ByteBuffer.wrap(array)
    buffer.putInt(id)
    new ObjectId(array)
  }

  def apply(id: Long) = {
    val array = Array[Byte](8)
    val buffer = ByteBuffer.wrap(array)
    buffer.putLong(id)
    new ObjectId(array)
  }

  def apply(id: Date) = {
    val array = Array[Byte](8)
    val buffer = ByteBuffer.wrap(array)
    buffer.putLong(id.getTime)
    new ObjectId(array)
  }

  def apply(id: UUID) = {
    val array = Array[Byte](16)
    val buffer = ByteBuffer.wrap(array)
    buffer.putLong(id.getMostSignificantBits)
    buffer.putLong(id.getLeastSignificantBits)
    new ObjectId(array)
  }
}