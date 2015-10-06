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

package unicorn.util

/**
 * Pimped byte array.
 *
 * @author Haifeng Li
 */
case class ByteArray(bytes: Array[Byte]) extends Comparable[ByteArray] {
  /** Flip each bit of a byte string */
  def unary_~ = bytes.map(~_)

  /** Hexadecimal string representation */
  override def toString = bytes2Hex(bytes)

  override def compareTo(o: ByteArray): Int = compareByteArray(bytes, o.bytes)

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[ByteArray]) return false
    
    val that = o.asInstanceOf[ByteArray]
    if (this.bytes.size != that.bytes.size) return false
    
    compareTo(that) == 0
  }

  override def hashCode: Int = {
    var hash = 7
    bytes.foreach { i => hash = 31 * hash + i }
    hash
  }
}
