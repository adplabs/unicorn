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

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.format.DateTimeFormatter

/**
 * Utility functions.
 *
 * @author Haifeng Li
 */
package object util {

  val iso8601DateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val iso8601DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]['Z']")

  val utf8 = Charset.forName("UTF-8")

  implicit def boxByteArray(x: Array[Byte]) = new ByteArray(x)
  implicit def unboxByteArray(x: ByteArray) = x.bytes
  implicit def string2Bytes(x: String) = x.getBytes(utf8)
  implicit def string2ByteArray(x: String) = new ByteArray(x.getBytes(utf8))
  implicit def bytesSeq2ByteArray(x: Seq[Array[Byte]]) = x.map { bytes => new ByteArray(bytes) }
  implicit def stringSeq2ByteArray(x: Seq[String]) = x.map { s => new ByteArray(s.getBytes(utf8)) }

  /** Measure running time of a function/block. */
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: " + (System.nanoTime - s)/1e6 + " ms")
    ret
  }

  /** Helper function convert ByteBuffer to Array[Byte]. */
  implicit def byteBuffer2ArrayByte(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes
  }

  /** Helper function convert ByteBuffer to ByteArray. */
  implicit def byteBuffer2ByteArray(buffer: ByteBuffer): ByteArray = ByteArray(byteBuffer2ArrayByte(buffer))

  /** Byte array to hexadecimal string. */
  def bytes2Hex(bytes: Array[Byte]): String = {
    bytes.map("%02X" format _).mkString
  }

  /** Hexadecimal string to byte array. */
  def hex2Bytes(s: String): Array[Byte] = {
    require(s.length % 2 == 0, "Hexadecimal string must contain an even number of characters")

    val bytes = new Array[Byte](s.length / 2)
    for (i <- 0 until s.length by 2) {
      bytes(i/2) = java.lang.Integer.parseInt(s.substring(i, i+2), 16).toByte
    }
    bytes
  }

  val md5Encoder = java.security.MessageDigest.getInstance("MD5")

  /** MD5 hash function */
  def md5(bytes: Array[Byte]) = md5Encoder.digest(bytes)

  /** Byte array ordering */
  def compareByteArray(x: Array[Byte], y: Array[Byte]): Int = {
    val n = Math.min(x.length, y.length)
    for (i <- 0 until n) {
      val a: Int = x(i) & 0xFF
      val b: Int = y(i) & 0xFF
      if (a != b) return a - b
    }
    x.length - y.length
  }

  /** Left pad a String with a specified character.
    *
    * @param str  the String to pad out, may be null
    * @param size  the size to pad to
    * @param padChar  the character to pad with
    * @return left padded String or original String if no padding is necessary,
    *         null if null String input
    */
  def leftPad(str: String, size: Int, padChar: Char = ' '): String = {
    if (str == null)
      return null

    val pads = size - str.length
    if (pads <= 0)
      return str // returns original String when possible

    return (String.valueOf(padChar) * pads).concat(str)
  }

  /** Right pad a String with a specified character.
    *
    * @param str  the String to pad out, may be null
    * @param size  the size to pad to
    * @param padChar  the character to pad with
    * @return left padded String or original String if no padding is necessary,
    *         null if null String input
    */
  def rightPad(str: String, size: Int, padChar: Char = ' '): String = {
    if (str == null)
      return null

    val pads = size - str.length
    if (pads <= 0)
      return str // returns original String when possible

    return str.concat(String.valueOf(padChar) * pads)
  }
}
