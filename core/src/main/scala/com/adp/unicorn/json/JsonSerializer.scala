package com.adp.unicorn.json

/**
 * @author Haifeng Li
 */
trait JsonSerializer {
  def toBytes(value: JsValue): Array[Byte]
  def fromBytes(bytes: Array[Byte]): JsValue
}
