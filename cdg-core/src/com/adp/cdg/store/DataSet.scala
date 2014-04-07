package com.adp.cdg.store

import com.adp.cdg.Document

trait DataSet {
  /**
   * Cache a whole column family.
   */
  val cache = collection.mutable.Map[(String, String), collection.mutable.Map[String, Array[Byte]]]()
  
  /**
   * Cached read. Get all columns in a column family.
   */
  def get(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    if (cache.contains((row, columnFamily))) {
      cache((row, columnFamily))
    } else {
      val family = read(row, columnFamily)
      cache((row, columnFamily)) = family
      family
    }
  }
  
  /**
   * Cached read. Get a value.
   */
  def get(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    if (cache.contains((row, columnFamily))) {
      val map = collection.mutable.Map[String, Array[Byte]]()
      val family = cache((row, columnFamily))
      family.foreach { case (key, value) => if (columns.contains(key)) map(key) = value }
      map
    } else {
      read(row, columnFamily, columns: _*)
    }
  }
  
  /**
   * Get all columns in a column family.
   */
  def read(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]]
  /**
   * Get a value.
   */
  def read(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]]
  
  /**
   * Update the value of a column. NOTE no immediate effects until commit.
   */
  def write(row: String, columnFamily: String, column: String, value: Array[Byte], visibility: String = "public"): Unit
  /**
   * Delete a column. NOTE no immediate effects until commit.
   */
  def delete(row: String, columnFamily: String, column: String): Unit
  /**
   * Commit all mutations including both put and delete.
   */
  def commit: Unit

  /**
   * Delete rows between (start, end].
   */
  def deleteRows(start: String, end: String): Unit
}