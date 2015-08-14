/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn

import scala.language.dynamics
import scala.language.implicitConversions
import unicorn.store.Dataset
import unicorn.json._

/**
 * A document can be regarded as a JSON value with a unique key.
 * 
 * @author Haifeng Li
 */
class Document(val id: String, value: JsValue) {
  
  /**
   * The database that this document binds to.
   */
  private var dataset: Option[Dataset] = None
  
  /**
   * Document attributes.
   */
  lazy val attributes = collection.mutable.Map[String, JsValue]()
  /**
   * The relationships to other documents.
   * The key is the (relationship, doc). The value is any JSON object
   * associated with the relationship.
   */
  lazy val links = collection.mutable.Map[(String, String), JsValue]()
  /**
   * Updates to commit into database.
   */
  private lazy val updates = collection.mutable.Map[(String, String), Option[JsValue]]()

  /**
   * Document equality is purely based on ID rather than values.
   */
  override def equals(other: Any) = other match { 
    case that: Document => this.id == that.id 
    case _ => false 
  }

  /**
   * The hash code of document is based on ID only.
   */
  override def hashCode = id.hashCode
  
  override def toString = {
    val s = id + " = " + JsObject(attributes).prettyPrint
    if (links.isEmpty) s
    else s + " with relationships " +
      JsObject(links.map { case (key, value) => (relationshipColumnQualifier(key._1, key._2), value) }).prettyPrint
  }
 
  /**
   * Returns a copy of this document with a new ID.
   */
  def copy(id: String): Document = {
    val doc = Document(id)
    foreach { case (attr, value) => doc(attr) = value }
    foreachRelationship { case ((label, target), value) => doc(label, target) = value }
    doc
  }
  
  /**
   * For each loop over attributes.
   */
  def foreach[U](f: ((String, JsValue)) => U): Unit = attributes.foreach(f)
  
  /**
   * For each loop over relationships.
   */
  def foreachRelationship[U](f: (((String, String), JsValue)) => U): Unit = links.foreach(f)
  
  /**
   * Returns all attributes as a JSON object.
   */
  def json = JsObject(attributes)

  /**
   * Returns the value of a field if it exists or JsUndefinedValue.
   */
  def apply(key: String): JsValue = {
    if (attributes.contains(key))
      attributes(key)
    else
      JsUndefined
  }
  
  def selectDynamic(key: String): JsValue = {
    apply(key)
  }
  
  /**
   * Removes all attributes and relationships. If commit immediately after clear,
   * all attributes and relationships will be deleted from data set.
   */
  def clear: Document = {
    clearAttributes
    clearRelationships
    this
  }
  
  /**
   * Removes all attributes. If commit immediately after clear,
   * all attributes will be deleted from data set.
   */
  def clearAttributes: Document = {
    attributes.keySet.foreach { key => remove(key) }
    this
  }
  
  /**
   * Removes all attributes. If commit immediately after clear,
   * all attributes will be deleted from data set.
   */
  def clearRelationships: Document = {
    links.keySet.foreach { case (label, target) => remove(label, target) }
    this
  }
  
  /**
   * Removes a field.
   */
  def remove(key: String): Option[JsValue] = {
    val value = attributes.remove(key)
    if (value.isDefined) remove(attributeFamily, key, value.get)
    value
  }
  
  /**
   * Recursively removes the key-value pairs.
   */
  private def remove(columnFamily: String, key: String, value: JsValue): Unit = {
    updates((columnFamily, key)) = None
    
    value match {
      case JsObject(obj) =>
        obj.foreach {case (k, v) => remove(columnFamily, key + fieldSeparator + k, v)}
        
      case JsArray(array) =>
        array.zipWithIndex foreach {case (e, i) => remove(columnFamily, key + fieldSeparator + i, e)}
        
      case _ => ()
    }    
  }

  /**
   * Update a field.
   */
  def update(key: String, value: JsValue): Document = {
    attributes(key) = value
    logUpdate(attributeFamily, key, value)
    this
  }
 
  /**
   * Recursively records the mutations.
   */
  private def logUpdate(columnFamily: String, key: String, value: JsValue): Unit = {
    value match {
      case JsUndefined => remove(key)
      case _ => updates((columnFamily, key)) = Some(value)
    }
    
    value match {
      case JsObject(obj) =>
        obj.foreach {case (k, v) => logUpdate(columnFamily, key + fieldSeparator + k, v)}
        
      case JsArray(array) =>
        array.zipWithIndex foreach {case (e, i) => logUpdate(columnFamily, key + fieldSeparator + i, e)}
        
      case _ => ()
    }    
  }
  
  /**
   * Update a field with another document.
   */
  def update(key: String, value: Document) {
    update(key, value.json)
  }
   
  /**
   * Update a field with document array.
   */
  def update(key: String, values: Seq[Document]) {
    val array = values.map(_.json)
    update(key, JsArray(array: _*))
  }
    
  def updateDynamic(key: String)(value: Any) {
    value match {
      case value: String => update(key, value)
      case value: Int => update(key, value)
      case value: Double => update(key, value)
      case value: Boolean => update(key, value)
      case value: Long => update(key, value)
      case value: JsValue => update(key, value)
      case value: Document => update(key, value)
      case value: Array[String] => update(key, value)
      case value: Array[Int] => update(key, value)
      case value: Array[Double] => update(key, value)
      case value: Array[Boolean] => update(key, value)
      case value: Array[Long] => update(key, value)
      case value: Array[JsValue] => update(key, value)
      case value: Array[Document] => update(key, value)
      case Some(value: String) => update(key, value)
      case Some(value: Int) => update(key, value)
      case Some(value: Double) => update(key, value)
      case Some(value: Boolean) => update(key, value)
      case Some(value: Long) => update(key, value)
      case Some(value: Document) => update(key, value)
      case Some(value: JsValue) => update(key, value)
      case null | None => remove(key) 
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
  }
  
  /**
   * Loads both attributes and relationships from database.
   */
  def load(context: Dataset): Document = {
    dataset = Some(context)
    parseObject(context, attributeFamily, attributes)
    parseRelationships(context, relationshipFamily, links)
    this
  }
  
  /**
   * Loads only the attributes.
   */
  def loadAttributes: Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) => parseObject(context, attributeFamily, attributes)
    }
    this
  }
  
  /**
   * Loads only the relationships.
   */
  def loadRelationships: Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) => parseRelationships(context, relationshipFamily, links)
    }
    this
  }
  
  /**
   * Parses the byte array to a JSON value.
   */
  private def parse(key: String, value: Array[Byte], kv: collection.mutable.Map[String, Array[Byte]]): JsValue = {
    JsUndefined
    /*
    if (value.startsWith(JsString.prefix)) JsString(value)
    else if (value.startsWith(JsInt.prefix)) JsInt(value)
    else if (value.startsWith(JsDouble.prefix)) JsDouble(value)
    else if (value.startsWith(JsBoolean.prefix)) JsBoolean(value)
    else if (value.startsWith(JsLong.prefix)) JsLong(value)
    else if (value.startsWith(JsString.prefix)) JsString(value)
    else if (value.startsWith(JsObject.prefix)) {
      val child = collection.mutable.Map[String, JsValue]()
       
      val fields = JsObject(value)
      fields.foreach { field =>
        val fieldkey = key + fieldSeparator + field
        child(field) = parse(fieldkey, kv(fieldkey), kv)
      }
      
      new JsObject(child)
    } else if (value.startsWith(JsArray.prefix)) {
      val size = JsArray(value)
      val array = new Array[JsValue](size)
        
      for (i <- 0 until size) {
        val fieldkey = key + fieldSeparator + i
        array(i) = parse(fieldkey, kv(fieldkey), kv)
      }
        
      JsArray(array: _*)
    }
    else JsBinary(value)
    */
  }
  
  /**
   * Parses the JSON object/map into this document.
   */
  private def parseObject(context: Dataset, columnFamily: String, map: collection.mutable.Map[String, JsValue]): Unit = {
    val kv = context.get(id, columnFamily)
    kv.foreach { case(key, value) =>
      if (!key.contains(fieldSeparator))
        map(key) = parse(key, value, kv)
    }
  }
  
  /**
   * Parses the JSON object/map into this document.
   */
  private def parseRelationships(context: Dataset, columnFamily: String, map: collection.mutable.Map[(String, String), JsValue]): Unit = {
    val kv = context.get(id, columnFamily)
    kv.foreach { case(key, value) =>
      val token = key.split(relationshipKeySeparator)
      if (token.length == 2 && !token(1).contains(fieldSeparator))
        map((token(0), token(1))) = parse(key, value, kv)
    }
  }
  
  /**
   * Sets the context of this document (i.e. data set).
   */
  def from(context: Dataset): Document = {
    dataset = Some(context)
    this
  }

  /**
   * Loads given fields rather than the whole document.
   */
  def select(fields: String*): Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) =>
        val unloaded = Set(fields: _*) &~ attributes.keySet
        if (!unloaded.isEmpty) {
          val kv = context.get(id, attributeFamily, unloaded.toSeq: _*)
          kv.foreach { case (key, value) => attributes(key) = parse(key, value, kv) }
        }
        this
    }
  }
  
  /**
   * Flush changes to database backend.
   */
  def flush {
    if (updates.isEmpty) return
    
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) => {
        updates.foreach { case(familyCol, value) =>
          value match {
            case None => context.remove(id, familyCol._1, familyCol._2)
            case Some(value) => ()//TODO context.put(id, familyCol._1, familyCol._2, value.bytes)
          }
        }
        context.commit
        updates.clear
      }
    }
  }
  
  /**
   * Writes this documents (only updated/deleted fields) to the data set.
   */
  def into(context: Dataset) {
    dataset = Some(context)
    flush
  }

  /**
   * Creates the relationship column qualifier.
   */
  private def relationshipColumnQualifier(relationship: String, doc: String) =
    relationship + relationshipKeySeparator + doc
  
  /**
   * Returns all neighbors of given types of relationship.
   */
  def neighbors(relationships: String*): Map[Document, (String, JsValue)] = {
    var nodes = List[(Document, (String, JsValue))]()
    
    links.foreach { case ((relationship, id), value) =>
      if (relationships.contains(relationship)) {
        val doc = Document(id)
        dataset match {
          case Some(context) => doc.from(context)
          case _ => ()
        }
        nodes = (doc, (relationship, value)) :: nodes
      }
    }
    
    nodes.toMap
  }
   
  /**
   * Returns all relationships to a given neighbor.
   */
  def relationships(doc: String): Map[String, JsValue] = {
    var edges = List[(String, JsValue)]()

    links.foreach { case ((relation, id), value) =>
      if (id == doc) 
        edges = (relation, value) :: edges
    }

    edges.toMap
  }
  
  /**
   * Returns the value of a relationship if it exists or None.
   */
  def apply(relationship: String, doc: String): JsValue = {
    if (links.contains((relationship, doc)))
      links((relationship, doc))
    else
      JsUndefined
  }
  
  /**
   * Update a relationship.
   */
  def update(relationship: String, doc: String, value: JsValue): Document = {
    if (value == null) {
      remove(relationship, doc)
    } else {
      links((relationship, doc)) = value
      logUpdate(relationshipFamily, relationshipColumnQualifier(relationship, doc), value)
    }
    
    this
  }
  
  /**
   * Removes a relationship.
   */
  def remove(relationship: String, doc: String): Document = {
    val value = links.remove((relationship, doc))
    if (value.isDefined) remove(relationshipFamily, relationshipColumnQualifier(relationship, doc), value.get)
    this
  }
}

object Document {
  def apply(id: String): Document = new Document(id)
  def apply(id: Int): Document = new Document(id.toString)
  def apply(id: Long): Document = new Document(id.toString)
}
