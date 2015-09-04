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
class ObjectId(val id: Array[Byte]) extends Comparable[ObjectId] {
  override def equals(that: Any): Boolean = {
    that.isInstanceOf[ObjectId] && Arrays.equals(id, that.asInstanceOf[ObjectId].id)
  }

  override def compareTo(o: ObjectId): Int = {
    BytesOrdering.compare(id, o.id)
  }

  /** Hexadecimal string representation */
  override def toString = bytes2Hex(id)

  /** Suppose the byte array is the UTF-8 encoding of a printable string. */
  def string: String = {
    new String(id, utf8)
  }

  def toInt: Int = {
    if (id.length != 4) throw new IllegalStateException("ObjectId is not an Int")
    val buffer = ByteBuffer.wrap(id)
    buffer.getInt
  }

  def toLong: Long = {
    if (id.length != 8) throw new IllegalStateException("ObjectId is not a Long")
    val buffer = ByteBuffer.wrap(id)
    buffer.getLong
  }

  def toDate: Date = {
    if (id.length != 8) throw new IllegalStateException("ObjectId is not a Date")
    val buffer = ByteBuffer.wrap(id)
    new Date(buffer.getLong)
  }

  def toUUID: UUID = {
    if (id.length != 16) throw new IllegalStateException("ObjectId is not a UUID")
    val buffer = ByteBuffer.wrap(id)
    new UUID(buffer.getLong, buffer.getLong)
  }

  def toBsonObjectId: BsonObjectId = {
    if (id.length != 12) throw new IllegalStateException("ObjectId is not a BSON ObjectId")
    new BsonObjectId(id)
  }
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