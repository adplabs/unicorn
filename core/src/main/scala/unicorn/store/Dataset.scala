/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.store

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success
import unicorn.Document

/**
 * Abstraction of column data table.
 * 
 * @author Haifeng Li (293050)
 */
trait Dataset {
  class CacheEntry(val keyValuePairs: collection.mutable.Map[String, Array[Byte]], var timestamp: Long) {
    
  }
  
  type Cache = collection.mutable.Map[(String, String), CacheEntry]
  
  /**
   * Cache a whole column family.
   */
  var cache = collection.mutable.Map[(String, String), CacheEntry]()
  /**
   * Flag to turn cache on/off.
   */
  var cacheEnabled = false
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
  
  /**
   * Turns cache mechanism on
   */
  def cacheOn = cacheEnabled = true
  
  /**
   * Turns cache mechanism off
   */
  def cacheOff = cacheEnabled = false
  
  private def getCacheEntry(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val entry = cache((row, columnFamily))
    entry.timestamp = System.currentTimeMillis
    entry.keyValuePairs
  }
  
  /**
   * Returns the document of given id.
   */
  def get(id: String): Document = Document(id).load(this)
  
  /**
   * Puts the document into the database.
   */
  def put(doc: Document) = doc into this
  
  /**
   * Cached read. Get all columns in a column family.
   * If refresh is true, force to read from database even if cache is on.
   */
  def get(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    if (!cacheEnabled) {
      return read(row, columnFamily)
    }
      
    if (cache.contains((row, columnFamily))) {
      getCacheEntry(row, columnFamily)
    } else {
      val keyValuePairs = read(row, columnFamily)
      cache((row, columnFamily)) = new CacheEntry(keyValuePairs, System.currentTimeMillis)
      
      if (cache.size > cacheSize) {
        val future: Future[collection.mutable.Map[(String, String), CacheEntry]] = Future {
          val now = System.currentTimeMillis
          cache.filter { case (key, entry) => now - entry.timestamp < cacheEntryTimeout }
        }
        
        future.onSuccess {case result => cache = result}
      }
      
      keyValuePairs
    }
  }
  
  /**
   * Cached read. Get a value.
   */
  def get(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    if (!cacheEnabled) {
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
  def put(row: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    if (cacheEnabled && cache.contains((row, columnFamily))) {
      getCacheEntry(row, columnFamily)(column) = value
    }
    
    write(row, columnFamily, column, value)
  }
  
  /**
   * Cached deletion.
   */
  def remove(row: String, columnFamily: String, column: String): Unit = {
    if (cacheEnabled && cache.contains((row, columnFamily))) {
      getCacheEntry(row, columnFamily).remove(column)
    }
    
    delete(row, columnFamily, column)    
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
  def write(row: String, columnFamily: String, column: String, value: Array[Byte]): Unit
  /**
   * Delete a column. NOTE no immediate effects until commit.
   */
  def delete(row: String, columnFamily: String, column: String): Unit

  /**
   * Commit all mutations including both put and delete.
   */
  def commit: Unit
}
