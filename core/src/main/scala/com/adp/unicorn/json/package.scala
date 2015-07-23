package com.adp.unicorn

/**
 * Created by lihb on 7/23/15.
 */
package object json {
  val JsTrue = JsBool(true)
  val JsFalse = JsBool(false)
  implicit def pimpString(string: String) = new PimpedString(string)
}

package json {
  private[json] class PimpedString(string: String) {
    def parseJson: JsValue = JsonParser(string)
  }
}
