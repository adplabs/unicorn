package com.adp.unicorn.json

/**
 * @author Haifeng Li
 */
trait JsonSerializer {
  def serialize(value: JsTopLevel): Array[Byte]
  def deserialize(bytes: Array[Byte]): JsTopLevel
}
