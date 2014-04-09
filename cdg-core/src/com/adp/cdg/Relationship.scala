package com.adp.cdg

class Relationship(val source: Document, val target: Document, val label: String, val value: JsonValue) {

}

object Relationship {
  def apply(source: Document, target: Document, label: String, value: JsonValue) =
    new Relationship(source, target, label, value)
}