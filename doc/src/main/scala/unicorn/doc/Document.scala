/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.doc

import java.util.UUID
import unicorn.json._

/**
 * A document is a JSON value with a unique key.
 * 
 * @author Haifeng Li
 */
case class Document(val id: JsValue, value: JsValue)

object Document {
  def apply(json: JsValue): Document = apply(UUID.randomUUID, json)
  def apply(id: String, json: JsValue): Document = new Document(JsString(id), json)
  def apply(id: Int, json: JsValue): Document = new Document(JsInt(id), json)
  def apply(id: Long, json: JsValue): Document = new Document(JsLong(id), json)
  def apply(id: UUID, json: JsValue): Document = new Document(JsUUID(id), json)
}
