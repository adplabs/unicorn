package unicorn.cassandra

import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import org.apache.cassandra.thrift.Cassandra.Client
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
 * @author Haifeng Li
 */
class CassandraTable(client: Client, consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) extends unicorn.bigtable.Table {
  /** Unsupported */
  override def setCellVisibility(expression: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def getCellVisibility: Option[String] = None

  /** Unsupported */
  override def setAuthorizations(labels: String*): Unit = {
    throw new UnsupportedOperationException
  }

  override def getAuthorizations: Option[Seq[String]] = None

  val null_range = ByteBuffer.wrap(Array[Byte]())
  override def get(row: Array[Byte], families: Array[Byte]*): Map[Key, Value] = {
    require(families.size == 1)
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(new String(families(0)))
    val predicate = new SlicePredicate
    predicate.setSlice_range(new SliceRange(null_range, null_range, false, Int.MaxValue))

    val result = client.get_slice(key, parent, predicate, consistency)
    getResults(row, families(0), result)
  }
  
  override def get(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Map[Key, Value] = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(new String(family))
    val predicate = new SlicePredicate
    columns.foreach { column =>
      predicate.addToColumn_names(ByteBuffer.wrap(column))
    }

    val result = client.get_slice(key, parent, predicate, consistency)
    getResults(row, family, result)
  }

  /** Unsupported */
  override def get(keys: Key*): Map[Key, Value] = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def scan(startRow: Array[Byte], stopRow: Array[Byte], families: Array[Byte]*): Scanner = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def scan(startRow: Array[Byte], stopRow: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Scanner = {
    throw new UnsupportedOperationException
  }

  private def getResults(row: Array[Byte], family: Array[Byte], result: java.util.List[ColumnOrSuperColumn]): Map[Key, Value] = {
    result.map { column =>
      val key = (row, family, column.getColumn.getName)
      val value = (column.getColumn.getValue, column.getColumn.getTimestamp)
      (key, value)
    }.toMap
  }

  val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]()

  override def put(row: Array[Byte], family: Array[Byte], columns: (Array[Byte], Array[Byte])*): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = new String(family)
    createMutationMapEntry(key, parent)

    columns.foreach { case (column, value) =>
      val put = new ColumnOrSuperColumn
      put.column = new Column(ByteBuffer.wrap(column))
      put.column.setValue(value)
      put.column.setTimestamp(System.currentTimeMillis)
      val mutation = new Mutation
      mutation.column_or_supercolumn = put
      updates.get(key).get(parent).add(mutation)
    }

    client.atomic_batch_mutate(updates, consistency)
    updates.clear
  }

  override def put(values: (Key, Array[Byte])*): Unit = {
    require(!values.isEmpty)
    values.foreach { case ((row, family, column), value) =>
      val key = ByteBuffer.wrap(row)
      val parent = new String(family)
      createMutationMapEntry(key, parent)

      val put = new ColumnOrSuperColumn
      put.column = new Column(ByteBuffer.wrap(column))
      put.column.setValue(value)
      put.column.setTimestamp(System.currentTimeMillis)
      val mutation = new Mutation
      mutation.column_or_supercolumn = put
      updates.get(key).get(parent).add(mutation)
    }

    client.atomic_batch_mutate(updates, consistency)
    updates.clear
  }

  override def delete(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = new String(family)
    createMutationMapEntry(key, parent)

    val predicate = new SlicePredicate
    columns.foreach { column => predicate.addToColumn_names(ByteBuffer.wrap(column)) }
    
    val deletion = new Deletion
    deletion.setTimestamp(System.currentTimeMillis)
    deletion.setPredicate(predicate)
    
    val mutation = new Mutation
    mutation.deletion = deletion
    updates.get(key).get(parent).add(mutation)

    client.atomic_batch_mutate(updates, consistency)
    updates.clear
  }

  override def delete(keys: Key*): Unit = {
    require(!keys.isEmpty)
    keys.foreach { case (row, family, column) =>
      val key = ByteBuffer.wrap(row)
      val parent = new String(family)
      createMutationMapEntry(key, parent)

      val predicate = new SlicePredicate
      predicate.addToColumn_names(ByteBuffer.wrap(column))

      val deletion = new Deletion
      deletion.setTimestamp(System.currentTimeMillis)
      deletion.setPredicate(predicate)

      val mutation = new Mutation
      mutation.deletion = deletion
      updates.get(key).get(parent).add(mutation)
    }

    client.atomic_batch_mutate(updates, consistency)
    updates.clear
  }

  /** Unsupported */
  override def delete(row: Array[Byte]): Unit = {
    throw new UnsupportedOperationException
  }

  /**
   * Create mutation map entry if necessary.
   */
  private def createMutationMapEntry(key: ByteBuffer, family: String) {
    if (!updates.containsKey(key)) {
      val row = new java.util.HashMap[String, java.util.List[Mutation]]
      row.put(family, new java.util.ArrayList[Mutation])
      updates.put(key, row)
    } else if (!updates.get(key).containsKey(family)) {
      updates.get(key).put(family, new java.util.ArrayList[Mutation])
    }
  }

  /** Unsupported */
  override def rollback(row: Array[Byte], family: Array[Byte], columns: Array[Byte]*): Unit = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def rollback(keys: Key*): Unit = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def append(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]): Unit = {
    throw new UnsupportedOperationException
  }

  /** Unsupported */
  override def increment(row: Array[Byte], family: Array[Byte], column: Array[Byte], value: Long): Unit = {
    throw new UnsupportedOperationException
  }
}
