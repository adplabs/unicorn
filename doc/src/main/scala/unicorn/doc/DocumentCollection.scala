package unicorn.doc

import unicorn.json._

/**
 * @author Haifeng Li
 */
class DocumentCollection(table: unicorn.bigtable.Table, family: String) {
  val bsonSerializer = new BsonSerializer
  val columnarSerializer = new ColumnarJsonSerializer
  val columnFamily = family.getBytes

  def apply(id: JsValue): Document = {
    val row = bsonSerializer.serialize(id)
    val map = table.get(row, columnFamily)
    new Document(id, columnarSerializer.deserialize(map))
  }

  def insert(doc: Document): Unit = {
    val row = bsonSerializer.serialize(doc.id)
    val value = columnarSerializer.serialize(doc.value)
    table.put(row, columnFamily, value.toSeq: _*)
  }

  def insert(json: JsValue): Document = {
    val doc = Document(json)
    insert(doc)
    doc
  }

  def update(doc: Document): Unit

  def delete(id: JsValue): Unit = {
    val row = bsonSerializer.serialize(id)
    table.delete(row, columnFamily)
  }
}
