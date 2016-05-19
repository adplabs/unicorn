package unicorn.unibase

import scala.collection.mutable.ArrayBuffer
import unicorn.bigtable.{BigTable, Column, ColumnFamily}
import unicorn.json.{JsObject, JsValue, ColumnarJsonSerializer}
import unicorn.util.ByteArray

/** Document update operators for columnar JSON serialization.
  *
  * @author Haifeng Li
  */
trait UpdateOps {
  /** Underlying BigTable. */
  val table: BigTable

  /** Document value serializer. */
  val valueSerializer: ColumnarJsonSerializer

  /** Returns the column family of a field. */
  def familyOf(field: String): String

  /** Returns the row key of a document. */
  def key(id: JsValue): Array[Byte]

  /** The \$set operator replaces the values of fields.
    *
    * The document key _id and _tenant should not be set.
    *
    * If the field does not exist, \$set will add a new field with the specified
    * value, provided that the new field does not violate a type constraint.
    *
    * In MongoDB, \$set will create the embedded documents as needed to fulfill
    * the dotted path to the field. For example, for a \$set {"a.b.c" : "abc"}, MongoDB
    * will create the embedded object "a.b" if it doesn't exist.
    * However, we don't support this behavior because of the performance considerations.
    * We suggest the the alternative syntax {"a.b" : {"c" : "abc"}}, which has the
    * equivalent effect.
    *
    * To set an element of an array by the zero-based index position,
    * concatenate the array name with the dot (.) and zero-based index position.
    *
    * @param id the id of document.
    * @param doc the fields to update.
    */
  def set(id: JsValue, doc: JsObject): Unit = {
    require(!doc.fields.exists(_._1 == $id), s"Invalid operation: set ${$id}")
    require(!doc.fields.exists(_._1 == $tenant), s"Invalid operation: set ${$tenant}")

    // Group field by locality
    val groups = doc.fields.toSeq.groupBy { case (field, _) => familyOf(field) }

    // Map from parent to the fields to update
    import scala.collection.mutable.{Map, Set}
    val children = Map[(String, String), Set[String]]().withDefaultValue(Set.empty)
    val parents = groups.map { case (family, fields) =>
      val columns = fields.map { case (field, _) =>
        val (parent, name) = field.lastIndexOf(valueSerializer.pathDelimiter) match {
          case -1 => (valueSerializer.root, field)
          case end => (valueSerializer.str2Path(field.substring(0, end)), field.substring(end+1))
        }

        children((family, parent)).add(name)
        parent
      }.distinct.map { parent =>
        ByteArray(valueSerializer.str2Bytes(parent))
      }

      (family, columns)
    }.toSeq

    val $key = key(id)

    // Get the update to parents which get new child fields.
    val pathUpdates = table.get($key, parents).map { case ColumnFamily(family, parents) =>
      val updates = parents.map { parent =>
        val value = new ArrayBuffer[Byte]()
        value ++= parent.value.bytes
        val update = children((family, String.valueOf(parent))).foldLeft[Option[ArrayBuffer[Byte]]](None) { (parent, child) =>
          // appending the null terminal
          val bytes = valueSerializer.serialize(child)
          if (value.contains(bytes)) parent
          else {
            value ++= bytes
            Some(value)
          }
        }

        update.map { value => Column(parent.qualifier, value.toArray) }
      }.filter(_.isDefined).map(_.get)
      ColumnFamily(family, updates)
    }.filter(!_.columns.isEmpty)

    // Columns to update
    val families = groups.map { case (family, fields) =>
      val columns = fields.foldLeft(Seq[Column]()) { case (seq, (field, value)) =>
        seq ++ valueSerializer.serialize(value, valueSerializer.str2Path(field)).map {
          case (path, value) => Column(valueSerializer.str2Bytes(path), value)
        }.toSeq
      }
      ColumnFamily(family, columns)
    }

    table.put($key, pathUpdates ++ families)
  }

  /** The \$unset operator deletes particular fields.
    *
    * The document key _id & _tenant should not be unset.
    *
    * If the field does not exist, then \$unset does nothing (i.e. no operation).
    *
    * When deleting an array element, \$unset replaces the matching element
    * with undefined rather than removing the matching element from the array.
    * This behavior keeps consistent the array size and element positions.
    *
    * Note that we don't really delete the field but set it as JsUndefined
    * so that we keep the history and be able to time travel back. Otherwise,
    * we will lose the history after a major compaction.
    *
    * @param id the id of document.
    * @param doc the fields to delete.
    */
  def unset(id: JsValue, doc: JsObject): Unit = {
    require(!doc.fields.exists(_._1 == $id), s"Invalid operation: unset ${$id}")
    require(!doc.fields.exists(_._1 == $tenant), s"Invalid operation: unset ${$tenant}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) => familyOf(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, _) => Column(valueSerializer.str2PathBytes(field), valueSerializer.undefined)
      }
      ColumnFamily(family, columns)
    }

    table.put(key(id), families)
  }
}
