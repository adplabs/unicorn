/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.doc

import java.nio._
import java.util.UUID
import unicorn.json._

/**
 * A document is a JSON value with a unique key.
 * 
 * @author Haifeng Li
 */
case class Document(val id: Array[Byte], value: JsValue)

object Document {
  def apply(json: JsValue): Document = apply(UUID.randomUUID, json)

  def apply(id: String, json: JsValue): Document = new Document(id.getBytes, json)

  def apply(id: Int, json: JsValue): Document = {
    val buffer = new Array[Byte](4)
    ByteBuffer.wrap(buffer).putInt(id)
    new Document(buffer, json)
  }

  def apply(id: Long, json: JsValue): Document = {
    val buffer = new Array[Byte](8)
    ByteBuffer.wrap(buffer).putLong(id)
    new Document(buffer, json)
  }

  def apply(id: UUID, json: JsValue): Document = {
    val buffer = new Array[Byte](16)
    ByteBuffer.wrap(buffer).putLong(id.getMostSignificantBits)
    ByteBuffer.wrap(buffer).putLong(id.getLeastSignificantBits)
    new Document(buffer, json)
  }
}
