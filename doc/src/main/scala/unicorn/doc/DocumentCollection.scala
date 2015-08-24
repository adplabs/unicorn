package unicorn.doc

import unicorn.json._

/**
 * @author Haifeng Li
 */
class DocumentCollection(table: unicorn.bigtable.Table, family: String) {
  val serializer = new ColumnarJsonSerializer
  val columnFamily = family.getBytes("UTF-8")

  def apply(id: String): Document = {
    apply(id.getBytes("UTF-8"))
  }

  def apply(id: Array[Byte]): Document = {
    val map = table.get(id, columnFamily).map { case (key, value) =>
      (new String(key._3), value._1)
    }
    new Document(id, serializer.deserialize(map))
  }

  def insert(doc: Document): Unit = {
    val columns = serializer.serialize(doc.value).map { case (path, value) =>
      (path.getBytes("UTF-8"), value)
    }
    table.put(doc.id, columnFamily, columns.toSeq: _*)
  }

  def insert(json: JsValue): Document = {
    val doc = Document(json)
    insert(doc)
    doc
  }

  def remove(id: String): Unit = {
    table.delete(id.getBytes("UTF-8"))
  }

  def remove(id: Array[Byte]): Unit = {
    table.delete(id, columnFamily)
  }

  def update(doc: Document): Unit = {

  }
}
