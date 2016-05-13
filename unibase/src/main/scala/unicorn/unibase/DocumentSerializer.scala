package unicorn.unibase

import java.nio.ByteBuffer

import unicorn.bigtable.{Column, ColumnFamily}
import unicorn.json._

/** Document serializer. Key size is up to 64KB, column size is up to 10MB.
 */
case class DocumentSerializer(
  val keySerializer: BsonSerializer = new BsonSerializer(ByteBuffer.allocate(65536)),
  val valueSerializer: ColumnarJsonSerializer = new ColumnarJsonSerializer(ByteBuffer.allocate(10485760))) {

  def toBytes(s: String) = s.getBytes(JsonSerializer.charset)

  /** Returns the json path of a dot notation path as in MongoDB. */
  def jsonPath(path: String) = s"${JsonSerializer.root}${JsonSerializer.pathDelimiter}$path"

  /** Returns the byte array of json path */
  def jsonPathBytes(path: String) = toBytes(jsonPath(path))

  /** Assembles the document from multi-column family data. */
  def deserialize(data: Seq[ColumnFamily]): Option[JsObject] = {
    val objects = data.map { case ColumnFamily(family, columns) =>
      val map = columns.map { case Column(qualifier, value, _) =>
        (new String(qualifier, JsonSerializer.charset), value.bytes)
      }.toMap
      val json = valueSerializer.deserialize(map)
      json.asInstanceOf[JsObject]
    }

    if (objects.size == 0)
      None
    else if (objects.size == 1)
      Some(objects(0))
    else {
      val fold = objects.foldLeft(JsObject()) { (doc, family) =>
        doc.fields ++= family.fields
        doc
      }
      Some(fold)
    }
  }

  def serialize(json: JsObject): Seq[Column] = {
    valueSerializer.serialize(json).map { case (path, value) =>
      Column(toBytes(path), value)
    }.toSeq
  }


  /** Serialize document id. */
  def serialize(tenant: JsValue, id: JsValue): Array[Byte] = {
    keySerializer.clear
    keySerializer.put(tenant)
    keySerializer.put(id)
    keySerializer.toBytes
  }

  def deserialize(key: Array[Byte]): (JsValue, JsValue) = {
    val buffer = ByteBuffer.wrap(key)
    val tenant = keySerializer.deserialize(buffer)
    val id = keySerializer.deserialize(buffer)
    (tenant, id)
  }

  def prefix(tenant: JsValue): Array[Byte] = {
    keySerializer.clear
    keySerializer.put(tenant)
    keySerializer.toBytes
  }
}
