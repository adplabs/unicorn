package com.adp.cdg.store

import com.adp.cdg.Document

trait DataSet {
  class CacheEntry(val keyValuePairs: collection.mutable.Map[String, Array[Byte]], var timestamp: Long) {
    
  }
  
  /**
   * Cache a whole column family.
   */
  val cache = collection.mutable.Map[(String, String), CacheEntry]()
  /**
   * Flag to turn cache on/off.
   */
  var cacheOn = false
  /**
   * Cache size to trigger compact cache.
   * Default to cache 1M column families.
   */
  var cacheSize = 1024 * 1024
  /**
   * If cache is full, we will remove entries which have not accessed
   * for this amount of milliseconds. Default is 10 minutes.
   */
  var cacheEntryTimeout = 10 * 60 * 1000
  
  private def getCacheEntry(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val entry = cache((row, columnFamily))
    entry.timestamp = System.currentTimeMillis
    entry.keyValuePairs
  }
  
  /**
   * Cached read. Get all columns in a column family.
   */
  def get(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    if (cacheOn == false) {
      return read(row, columnFamily)
    }
      
    if (cache.contains((row, columnFamily))) {
      getCacheEntry(row, columnFamily)
    } else {
      val keyValuePairs = read(row, columnFamily)
      cache((row, columnFamily)) = new CacheEntry(keyValuePairs, System.currentTimeMillis)
      
      if (cache.size > cacheSize) {
        val now = System.currentTimeMillis
        cache.filter { case (key, entry) => now - entry.timestamp > cacheEntryTimeout }
      }
      
      keyValuePairs
    }
  }
  
  /**
   * Cached read. Get a value.
   */
  def get(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    if (cacheOn == false) {
      return read(row, columnFamily, columns: _*)
    }
    
    if (cache.contains((row, columnFamily))) {
      val map = collection.mutable.Map[String, Array[Byte]]()
      getCacheEntry(row, columnFamily).foreach { case (key, value) => if (columns.contains(key)) map(key) = value }
      map
    } else {
      read(row, columnFamily, columns: _*)
    }
  }
  
  /**
   * Cached write.
   */
  def put(row: String, columnFamily: String, column: String, value: Array[Byte], visibility: String = "public"): Unit = {
    if (cacheOn && cache.contains((row, columnFamily))) {
      getCacheEntry(row, columnFamily)(column) = value
    }
    
    write(row, columnFamily, column, value, visibility)
  }
  
  /**
   * Cached deletion.
   */
  def remove(row: String, columnFamily: String, column: String, visibility: String = "public"): Unit = {
    if (cacheOn && cache.contains((row, columnFamily))) {
      getCacheEntry(row, columnFamily).remove(column)
    }
    
    delete(row, columnFamily, column, visibility)    
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
  def delete(row: String, columnFamily: String, column: String, visibility: String = "public"): Unit

  /**
   * Commit all mutations including both put and delete.
   */
  def commit: Unit
}