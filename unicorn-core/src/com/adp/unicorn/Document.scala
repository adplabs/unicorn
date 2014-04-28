/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn

import scala.language.dynamics
import scala.language.implicitConversions
import com.adp.unicorn.store.DataSet

/**
 * A document can be regarded as a JSON object with a unique key.
 * 
 * @author Haifeng Li (293050)
 */
class Document(val id: String,
    attributeFamily: String = Document.AttributeFamily,
    relationshipFamily: String = Document.RelationshipFamily,
    fieldSeparator: String = Document.FieldSeparator,
    relationshipKeySeparator: String = Document.RelationshipKeySeparator
  ) extends Dynamic with Traversable[(String, JsonValue)] {
  
  /**
   * The database that this document binds to.
   */
  private var dataset: Option[DataSet] = None
  
  /**
   * Document attributes.
   */
  private lazy val attributes = collection.mutable.Map[String, JsonValue]()
  /**
   * The relationships to other documents.
   * The key is the (relationship, doc). The value is any JSON object
   * associated with the relationship.
   */
  private lazy val links = collection.mutable.Map[(String, String), JsonValue]()
  /**
   * Updates to commit into database.
   */
  private lazy val updates = collection.mutable.Map[(String, String), Option[JsonValue]]()

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
    val s = id + " = " + JsonObjectValue(attributes).toString("", ",\n")
    if (links.isEmpty) s
    else
      s + "\nrelationships = " +
      JsonObjectValue(links.map { case (key, value) => (relationshipColumnQualifier(key._1, key._2), value) }).toString("", ",\n")
  }
 
  /**
   * Returns a copy of this document with a new ID.
   */
  def copy(id: String): Document = {
    val doc = Document(id);
    foreach { case (attr, value) => doc(attr) = value }
    foreachRelationship { case ((label, target), value) => doc(label, target) = value }
    doc
  }
  
  /**
   * For each loop over attributes.
   */
  def foreach[U](f: ((String, JsonValue)) => U): Unit = attributes.foreach(f)
  
  /**
   * For each loop over relationships.
   */
  def foreachRelationship[U](f: (((String, String), JsonValue)) => U): Unit = links.foreach(f)
  
  /**
   * Returns all attributes as a JSON object.
   */
  def json = JsonObjectValue(attributes)

  /**
   * Returns the value of a field if it exists or JsonUndefinedValue.
   */
  def apply(key: String): JsonValue = {
    if (attributes.contains(key))
      attributes(key)
    else
      JsonUndefinedValue
  }
  
  def selectDynamic(key: String): JsonValue = {
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
  def remove(key: String): Option[JsonValue] = {
    val value = attributes.remove(key)
    if (value.isDefined) remove(attributeFamily, key, value.get)
    value
  }
  
  /**
   * Recursively removes the key-value pairs.
   */
  private def remove(columnFamily: String, key: String, value: JsonValue): Unit = {
    updates((columnFamily, key)) = None
    
    value match {
      case JsonObjectValue(obj) =>
        obj.foreach {case (k, v) => remove(columnFamily, key + fieldSeparator + k, v)}
        
      case JsonArrayValue(array) =>
        array.zipWithIndex foreach {case (e, i) => remove(columnFamily, key + fieldSeparator + i, e)}
        
      case _ => ()
    }    
  }

  /**
   * Update a field.
   */
  def update(key: String, value: JsonValue): Document = {
    attributes(key) = value
    logUpdate(attributeFamily, key, value)
    this
  }
 
  /**
   * Recursively records the mutations.
   */
  private def logUpdate(columnFamily: String, key: String, value: JsonValue): Unit = {
    value match {
      case JsonUndefinedValue => remove(key)
      case _ => updates((columnFamily, key)) = Some(value)
    }
    
    value match {
      case JsonObjectValue(obj) =>
        obj.foreach {case (k, v) => logUpdate(columnFamily, key + fieldSeparator + k, v)}
        
      case JsonArrayValue(array) =>
        array.zipWithIndex foreach {case (e, i) => logUpdate(columnFamily, key + fieldSeparator + i, e)}
        
      case _ => ()
    }    
  }
  
  /**
   * Update a field with boolean value.
   */
  def update(key: String, value: Boolean) {
    update(key, JsonBoolValue(value))
  }
   
  /**
   * Update a field with boolean array.
   */
  def update(key: String, values: Array[Boolean]) {
    val array: Array[JsonValue] = values.map {e => JsonBoolValue(e) }
    update(key, JsonArrayValue(array))
  }
   
  /**
   * Update a field with int value.
   */
  def update(key: String, value: Int) {
    update(key, JsonIntValue(value))
  }
   
  /**
   * Update a field with int array.
   */
  def update(key: String, values: Array[Int]) {
    val array: Array[JsonValue] = values.map {e => JsonIntValue(e) }
    update(key, JsonArrayValue(array))
  }
   
  /**
   * Update a field with long value.
   */
  def update(key: String, value: Long) {
    update(key, JsonLongValue(value))
  }
   
  /**
   * Update a field with date array.
   */
  def update(key: String, values: Array[Long]) {
    val array: Array[JsonValue] = values.map {e => JsonLongValue(e) }
    update(key, JsonArrayValue(array))
  }
   
  /**
   * Update a field with double value.
   */
  def update(key: String, value: Double) {
    update(key, JsonDoubleValue(value))
  }
   
  /**
   * Update a field with double array.
   */
  def update(key: String, values: Array[Double]) {
    val array: Array[JsonValue] = values.map {e => JsonDoubleValue(e) }
    update(key, JsonArrayValue(array))
  }
   
  /**
   * Update a field with string value.
   */
  def update(key: String, value: String) {
    update(key, JsonStringValue(value))
  }
   
  /**
   * Update a field with string array.
   */
  def update(key: String, values: Array[String]) {
    val array: Array[JsonValue] = values.map {e => JsonStringValue(e) }
    update(key, JsonArrayValue(array))
  }
   
  /**
   * Update a field with array value.
   */
  def update(key: String, value: Array[JsonValue]) {
    update(key, JsonArrayValue(value))
  }
   
  /**
   * Update a field with object map value.
   */
  def update(key: String, value: collection.mutable.Map[String, JsonValue]) {
    update(key, JsonObjectValue(value))
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
  def update(key: String, values: Array[Document]) {
    val array: Array[JsonValue] = values.map {e => e.json }
    update(key, JsonArrayValue(array))
  }
    
  def updateDynamic(key: String)(value: Any) {
    value match {
      case value: String => update(key, value)
      case value: Int => update(key, value)
      case value: Double => update(key, value)
      case value: Boolean => update(key, value)
      case value: Long => update(key, value)
      case value: JsonValue => update(key, value)
      case value: Document => update(key, value)
      case value: Array[String] => update(key, value)
      case value: Array[Int] => update(key, value)
      case value: Array[Double] => update(key, value)
      case value: Array[Boolean] => update(key, value)
      case value: Array[Long] => update(key, value)
      case value: Array[JsonValue] => update(key, value)
      case value: Array[Document] => update(key, value)
      case Some(value: String) => update(key, value)
      case Some(value: Int) => update(key, value)
      case Some(value: Double) => update(key, value)
      case Some(value: Boolean) => update(key, value)
      case Some(value: Long) => update(key, value)
      case Some(value: Document) => update(key, value)
      case Some(value: JsonValue) => update(key, value)
      case null | None => remove(key) 
      case _ => throw new IllegalArgumentException("Unsupport JSON value type")
    }
  }
  
  /**
   * Loads/Reloads both attributes and relationships of this document.
   */
  def refresh: Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) =>
        parseObject(context, attributeFamily, attributes)
        parseRelationships(context, relationshipFamily, links)
    }
    
    this
  }
  
  /**
   * Loads/Reloads only the attributes.
   */
  def refreshAttributes: Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) =>
        parseObject(context, attributeFamily, attributes)
    }
    
    this
  }
  
  /**
   * Loads/Reloads only the relationships.
   */
  def refreshRelationships: Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) =>
        parseRelationships(context, relationshipFamily, links)
    }
    
    this
  }
  
  /**
   * Loads this document from the given database.
   */
  def of(context: DataSet): Document = {
    dataset = Some(context)
    refresh
  }
  
  /**
   * Parses the byte array to a JSON value.
   */
  private def parse(key: String, value: Array[Byte], kv: collection.mutable.Map[String, Array[Byte]]): JsonValue = {
    if (value.startsWith(JsonStringValue.prefix)) JsonStringValue(value)
    else if (value.startsWith(JsonIntValue.prefix)) JsonIntValue(value)
    else if (value.startsWith(JsonDoubleValue.prefix)) JsonDoubleValue(value)
    else if (value.startsWith(JsonBoolValue.prefix)) JsonBoolValue(value)
    else if (value.startsWith(JsonLongValue.prefix)) JsonLongValue(value)
    else if (value.startsWith(JsonStringValue.prefix)) JsonStringValue(value)
    else if (value.startsWith(JsonObjectValue.prefix)) {
      val child = collection.mutable.Map[String, JsonValue]()
       
      val fields = JsonObjectValue(value)
      fields.foreach { field =>
        val fieldkey = key + fieldSeparator + field
        child(field) = parse(fieldkey, kv(fieldkey), kv)
      }
      
      new JsonObjectValue(child)
    }
    else if (value.startsWith(JsonArrayValue.prefix)) {
        val size = JsonArrayValue(value)    
        val array = new Array[JsonValue](size)
        
        for (i <- 0 until size) {
          val fieldkey = key + fieldSeparator + i
          array(i) = parse(fieldkey, kv(fieldkey), kv)
        }
        
        JsonArrayValue(array)
    }
    else JsonBlobValue(value)      
  }
  
  /**
   * Parses the JSON object/map into this document.
   */
  private def parseObject(context: DataSet, columnFamily: String, map: collection.mutable.Map[String, JsonValue]): Unit = {
    val kv = context.get(id, columnFamily)
    kv.foreach { case(key, value) =>
      if (!key.contains(fieldSeparator))
        map(key) = parse(key, value, kv)
    }
  }
  
  /**
   * Parses the JSON object/map into this document.
   */
  private def parseRelationships(context: DataSet, columnFamily: String, map: collection.mutable.Map[(String, String), JsonValue]): Unit = {
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
  def from(context: DataSet): Document = {
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
        val kv = context.get(id, attributeFamily, fields: _*)
        kv.foreach { case (key, value) => attributes(key) = parse(key, value, kv) }
        this
    }
  }
  
  /**
   * Commits changes to data set.
   */
  def commit {
    if (updates.isEmpty) return
    
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) => {
        updates.foreach { case(familyCol, value) =>
          value match {
            case None => context.remove(id, familyCol._1, familyCol._2)
            case Some(value) => context.put(id, familyCol._1, familyCol._2, value.bytes)
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
  def into(context: DataSet) {
    dataset = Some(context)
    commit
  }

  /**
   * Creates the relationship column qualifier.
   */
  private def relationshipColumnQualifier(relationship: String, doc: String) =
    relationship + relationshipKeySeparator + doc
  
  /**
   * Returns all neighbors of given types of relationship.
   */
  def neighbors(relationships: String*): Map[Document, (String, JsonValue)] = {
    var nodes = List[(Document, (String, JsonValue))]()
    
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
  def relationships(doc: String): Map[String, JsonValue] = {
    var edges = List[(String, JsonValue)]()

    links.foreach { case ((relation, id), value) =>
      if (id == doc) 
        edges = (relation, value) :: edges
    }

    edges.toMap
  }
  
  /**
   * Returns the value of a relationship if it exists or None.
   */
  def apply(relationship: String, doc: String): JsonValue = {
    if (links.contains((relationship, doc)))
      links((relationship, doc))
    else
      JsonUndefinedValue
  }
  
  /**
   * Update a relationship.
   */
  def update(relationship: String, doc: String, value: JsonValue): Document = {
    if (value == null) {
      remove(relationship, doc)
    } else {
      links((relationship, doc)) = value
      logUpdate(relationshipFamily, relationshipColumnQualifier(relationship, doc), value)
    }
    
    this
  }
  
  def update(relationship: String, doc: String, value: Boolean): Document = {
    update(relationship, doc, JsonBoolValue(value))
  }
   
  def update(relationship: String, doc: String, value: Int): Document = {
    update(relationship, doc, JsonIntValue(value))
  }
   
  def update(relationship: String, doc: String, value: Long): Document = {
    update(relationship, doc, JsonLongValue(value))
  }
   
  def update(relationship: String, doc: String, value: Double): Document = {
    update(relationship, doc, JsonDoubleValue(value))
  }
   
  def update(relationship: String, doc: String, value: String): Document = {
    update(relationship, doc, JsonStringValue(value))
  }
   
  def update(relationship: String, doc: String, value: Array[JsonValue]): Document = {
    update(relationship, doc, JsonArrayValue(value))
  }
   
  def update(relationship: String, doc: String, value: Document): Document = {
    update(relationship, doc, value.json)
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
  val AttributeFamily     = "doc"
  val RelationshipFamily  = "graph"
    
  val FieldSeparator            = "."
  val RelationshipKeySeparator  = "-->"

  def apply(id: String): Document = new Document(id)
  def apply(id: Int): Document = new Document(id.toString)
  def apply(id: Long): Document = new Document(id.toString)
}

object DocumentImplicits {
  implicit def String2Document(id: String) = new Document(id)
  implicit def Int2Document(id: Int) = new Document(id.toString)
  implicit def Long2Document(id: Long) = new Document(id.toString)
}
