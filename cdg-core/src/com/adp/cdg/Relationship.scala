package com.adp.cdg

class Relationship(label: String, value: JsonValue) {

}

object Relationship {
  def apply(label: String, value: JsonValue): Relationship = new Relationship(label, value)
}