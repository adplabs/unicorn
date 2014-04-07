package com.adp.cdg

import java.util.Date
import scala.language.dynamics
import scala.language.implicitConversions
import com.adp.cdg.store.DataSet

/**
 * A document can be regarded as a JSON object with a key.
 */
class Document(id: String) extends Dynamic {
  val RootAttributeFamily = "cdg.attributes"
  val RelationshipFamily = "cdg.relationships"
  
  private var attributeFamily = RootAttributeFamily
  private var dataset: Option[DataSet] = None
  
  private lazy val attributes = collection.mutable.Map[String, JsonValue]()
  private lazy val neighbors = collection.mutable.LinkedList[Relationship]()
  private lazy val updates = collection.mutable.Map[(String, String), Option[Array[Byte]]]()
  
  /**
   * Returns the JSON object.
   */
  def json = attributes

  /**
   * Returns the relationships to other documents.
   */
  def relationships = neighbors
  
  /**
   * Returns the value of a field if it exists or None.
   */
  def apply(key: String) {
    if (attributes.contains(key))
      Some(attributes(key))
    else
      None
  }
  
  def selectDynamic(key: String)() {
    apply(key)
  }
  
  /**
   * Removes all attributes. If commit immediately after clear,
   * all attributes will be deleted from data set.
   */
  def clear: Document = {
    attributes.keySet.foreach { key => remove(key) }
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
        val family = columnFamily + "." + key
        obj.foreach {case (k, v) => remove(family, k, v)}
        
      case JsonArrayValue(array) =>
        val family = columnFamily + "." + key
        array.zipWithIndex foreach {case (e, i) => remove(family, i.toString, e)}
        
      case _ => ()
    }    
  }

  /**
   * Update a field.
   */
  def update(key: String, value: JsonValue): Document = {
    attributes(key) = value
    update(attributeFamily, key, value)
    this
  }
 
  /**
   * Recursively records the mutations.
   */
  private def update(columnFamily: String, key: String, value: JsonValue): Unit = {
    updates((columnFamily, key)) = Some(value.bytes)
    
    value match {
      case JsonObjectValue(obj) =>
        val family = columnFamily + "." + key
        obj.foreach {case (k, v) => update(family, k, v)}
        
      case JsonArrayValue(array) =>
        val family = columnFamily + "." + key
        array.zipWithIndex foreach {case (e, i) => update(family, i.toString, e)}
        
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
   * Update a field with date value.
   */
  def update(key: String, value: Date) {
    update(key, JsonDateValue(value))
  }
   
  /**
   * Update a field with date array.
   */
  def update(key: String, values: Array[Date]) {
    val array: Array[JsonValue] = values.map {e => JsonDateValue(e) }
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
    update(key, JsonObjectValue(value.json))
  }
   
  /**
   * Update a field with document array.
   */
  def update(key: String, values: Array[Document]) {
    val array: Array[JsonValue] = values.map {e => JsonObjectValue(e.json) }
    update(key, JsonArrayValue(array))
  }
   
  def updateDynamic(key: String)(value: JsonValue) {
    update(key, value)
  }

  def updateDynamic(key: String)(value: Boolean) {
    update(key, value)
  }

  def updateDynamic(key: String)(value: Date) {
    update(key, value)
  }

  def updateDynamic(key: String)(value: Int) {
    update(key, value)
  }

  def updateDynamic(key: String)(value: Double) {
    update(key, value)
  }

  def updateDynamic(key: String)(value: String) {
    update(key, value)
  }
  
  def updateDynamic(key: String)(value: Document) {
    update(key, value)
  }

  def updateDynamic(key: String, value: collection.mutable.Map[String, JsonValue]) {
    update(key, value)
  }
  
  def updateDynamic(key: String, value: Array[JsonValue]) {
    update(key, value)
  }
  
  def updateDynamic(key: String, value: Array[Boolean]) {
    update(key, value)
  }
   
  def updateDynamic(key: String, value: Array[Date]) {
    update(key, value)
  }
   
  def updateDynamic(key: String, value: Array[Int]) {
    update(key, value)
  }
   
  def updateDynamic(key: String, value: Array[Double]) {
    update(key, value)
  }
   
  def updateDynamic(key: String, value: Array[String]) {
    update(key, value)
  }
   
  def updateDynamic(key: String, value: Array[Document]) {
    update(key, value)
  }
  
  /**
   * Loads this document from the given dataset.
   */
  def of(context: DataSet): Document = {
    dataset = Some(context)
    parseMap(context, attributeFamily, attributes)
    this
  }
  
  /**
   * Parses the byte array to a JSON value.
   */
  private def parse(context: DataSet, columnFamily: String, key: String, value: Array[Byte]): JsonValue = {
      val s = new String(value, "UTF-8")
      s.substring(0, 2) match {
        case JsonBoolValue.prefix => JsonBoolValue(s)
        case JsonDateValue.prefix => JsonDateValue(s)
        case JsonIntValue.prefix  => JsonIntValue(s)
        case JsonDoubleValue.prefix => JsonDoubleValue(s)
        case JsonStringValue.prefix => JsonStringValue.valueOf(s)
        case JsonObjectValue.prefix =>
          val family = columnFamily + "." + key
          val fields = JsonObjectValue(s)
          val doc = Document(id).from(context, family).select(fields: _*)
          JsonObjectValue(doc.json)
        case JsonArrayValue.prefix  =>
          val family = columnFamily + "." + key
          val size = JsonArrayValue(s)
          val array = parseArray(context, family, size)
          JsonArrayValue(array)
        case _ => JsonStringValue(s)
      }    
  }
  
  /**
   * Parses the JSON value map into this document.
   */
  private def parseMap(context: DataSet, columnFamily: String, map: collection.mutable.Map[String, JsonValue]): Unit = {
    val kv = context.get(id, columnFamily)
    kv.foreach { case(key, value) => map(key) = parse(context, columnFamily, key, value) }
  }
  
  /**
   * Parses a column family (column qualifiers must be integers starting from 0) to an array.
   */
  private def parseArray(context: DataSet, columnFamily: String, size: Int): Array[JsonValue] = {
    val array = new Array[JsonValue](size)
    val kv = context.get(id, columnFamily)
    kv.foreach { case(key, value) => array(key.toInt) = parse(context, columnFamily, key, value) }
    array
  }
  
  /**
   * Sets the context of this document (i.e. data set and column family).
   */
  def from(context: DataSet, columnFamily: String = RootAttributeFamily): Document = {
    dataset = Some(context)
    attributeFamily = columnFamily
    this
  }
  
  /**
   * Loads given fields rather than the whole document.
   */
  def select(fields: String*): Document = {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) =>
        context.get(id, attributeFamily, fields: _*).
          foreach { case (key, value) => attributes(key) = parse(context, attributeFamily, key, value) }
        this
    }
  }
  
  /**
   * Commits changes to data set.
   */
  def commit {
    dataset match {
      case None => throw new IllegalStateException("Document is not binding to a dataset")
      case Some(context) => into(context)
    }
  }
  
  /**
   * Writes this documents (only updated/deleted fields) to the data set.
   */
  def into(context: DataSet) {
    dataset = Some(context)
    
    updates.foreach { case(familyCol, value) =>
      value match {
        case None => context.delete(id, familyCol._1, familyCol._2)
        case Some(value) => context.write(id, familyCol._1, familyCol._2, value)
      }
    }

    updates.clear
    
    context.commit
  }
  
  override def toString = {
    id + " = " + JsonObjectValue(attributes).toString("", ",\n")
  }
}

object Document {
  def apply(id: String): Document = new Document(id)
}

object DocumentImplicits {
  implicit def String2Document (id: String) = new Document(id)
}