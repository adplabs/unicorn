package com.adp.unicorn.store.cassandra

import java.nio.ByteBuffer
import com.adp.unicorn.store.Dataset
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.Mutation
import org.apache.cassandra.thrift.Deletion
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.ColumnOrSuperColumn

/**
 * Cassandra keyspace adapter. Cassandra's keyspaces may be regarded as tables
 * in other NoSQL solutions such as Accumulo and HBase.
 * 
 * @author Haifeng Li (293050)
 */
class CassandraKeyspace(client: Cassandra.Client, consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) extends Dataset {
  val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]()
  
  override def read(row: String, columnFamily: String): collection.mutable.Map[String, Array[Byte]] = {
    val key = toByteBuffer(row)
    val parent = new ColumnParent(columnFamily)
    val predicate = new SlicePredicate
    val range = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, Int.MaxValue)
    predicate.setSlice_range(range)

    val result = client.get_slice(key, parent, predicate, consistency)  
    mapOf(result)
  }
  
  override def read(row: String, columnFamily: String, columns: String*): collection.mutable.Map[String, Array[Byte]] = {
    val key = toByteBuffer(row)
    val parent = new ColumnParent(columnFamily)
    val predicate = new SlicePredicate
    columns.foreach { column =>
      predicate.addToColumn_names(toByteBuffer(column))
    }

    val result = client.get_slice(key, parent, predicate, consistency)
    mapOf(result)
  }
  
  private def mapOf(result: java.util.List[ColumnOrSuperColumn]): collection.mutable.Map[String, Array[Byte]] = {
    val map = collection.mutable.Map[String, Array[Byte]]()
    
    val it = result.iterator
    while (it.hasNext) {
      val column = it.next.column
      val bytes = new Array[Byte](column.value.remaining)
      column.value.get(bytes)
      map(toString(column.name)) = bytes
    }

    map
  }
  
  override def write(row: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    val key = toByteBuffer(row)
        
    val put = new ColumnOrSuperColumn
    put.column = new Column(toByteBuffer(column))
    put.column.setValue(value)
    put.column.setTimestamp(System.currentTimeMillis)
    
    val mutation = new Mutation
    mutation.column_or_supercolumn = put
    
    createMutationMapEntry(key, columnFamily)
    updates.get(key).get(columnFamily).add(mutation)
  }

  override def delete(row: String, columnFamily: String, column: String): Unit = {
    val key = toByteBuffer(row)
    
    val predicate = new SlicePredicate
    predicate.addToColumn_names(toByteBuffer(column))
    
    val deletion = new Deletion
    deletion.setTimestamp(System.currentTimeMillis)
    deletion.setPredicate(predicate)
    
    val mutation = new Mutation
    mutation.deletion = deletion
    
    createMutationMapEntry(key, columnFamily)
    updates.get(key).get(columnFamily).add(mutation)
  }

  /**
   * Create mutation map entry if necessary.
   */
  private def createMutationMapEntry(key: ByteBuffer, columnFamily: String) {
    if (!updates.containsKey(key)) {
      val row = new java.util.HashMap[String, java.util.List[Mutation]]
      row.put(columnFamily, new java.util.ArrayList[Mutation])
      updates.put(key, row)
    } else if (!updates.get(key).containsKey(columnFamily)) {
      updates.get(key).put(columnFamily, new java.util.ArrayList[Mutation])
    }
  }
  
  override def commit: Unit = {
    client.atomic_batch_mutate(updates, consistency)
    updates.clear
  }
  
  private def toByteBuffer(value: String) = ByteBuffer.wrap(value.getBytes("UTF-8"))
  
  private def toString(buffer: ByteBuffer): String = {
    val bytes = new Array[Byte](buffer.remaining)
    buffer.get(bytes)
    new String(bytes, "UTF-8")
  }
}