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

 import java.nio.charset.Charset
 import java.time.format.DateTimeFormatter

/**
 * @author Haifeng Li
 */
package object util {

  val iso8601DateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val iso8601DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]['Z']")

  val utf8 = Charset.forName("UTF-8")

  implicit def pimpedBitArray(bits: Array[Byte]) = new PimpedBitArray(bits)

  /** Byte array to hexadecimal string. */
  def bytes2Hex(bytes: Array[Byte]): String = {
    bytes.map("%02X" format _).mkString
  }

  /** Hexadecimal string to byte array. */
  def hex2Bytes(s: String): Array[Byte] = {
    if (s.length % 2 != 0)
      throw new IllegalArgumentException("Hexadecimal string must contain an even number of characters")

    val bytes = new Array[Byte](s.length / 2)
    for (i <- 0 until s.length by 2) {
      bytes(i/2) = java.lang.Integer.parseInt(s.substring(i, i+2), 16).toByte
    }
    bytes
  }

  /** MD5 hash function */
  def md5(bytes: Array[Byte]) = java.security.MessageDigest.getInstance("MD5").digest(bytes)

  /** Byte array ordering */
  object BytesOrdering extends Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      val n = Math.min(x.length, y.length)
      for (i <- 0 until n) {
        val a: Int = x(i) & 0xFF
        val b: Int = y(i) & 0xFF
        if (a != b) return a - b
      }
      x.length - y.length
    }
  }
}

package util {
  private[util] class PimpedBitArray(bits: Array[Byte]) {
    def unary_~ = bits.map(~_)
  }
}
