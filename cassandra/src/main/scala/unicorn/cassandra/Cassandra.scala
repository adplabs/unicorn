package unicorn.cassandra

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.cassandra.thrift.{ConsistencyLevel, KsDef, CfDef}
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import unicorn.bigtable._
import unicorn.util.Logging

/**
 * Cassandra server adapter.
 *
 * @author Haifeng Li
 */
class Cassandra(transport: TFramedTransport) extends Database with Logging {
  val protocol = new TBinaryProtocol(transport)
  val client = new Client(protocol)

  override def close: Unit = transport.close

  override def apply(name: String): CassandraTable = {
    new CassandraTable(this, name)
  }

  def apply(name: String, consistency: ConsistencyLevel): CassandraTable = {
    new CassandraTable(this, name, consistency)
  }
  
  override def createTable(name: String, props: Properties, families: String*): CassandraTable = {
    val options = props.stringPropertyNames.map { p => (p, props.getProperty(p)) }.toMap
    
    val keyspace = new KsDef
    keyspace.setName(name)
    keyspace.setStrategy_class(props.getProperty("strategy"))
    keyspace.setStrategy_options(options)
    
    families.foreach { family =>
      val cf = new CfDef
      cf.setName(family)
      cf.setKeyspace(name)
      keyspace.addToCf_defs(cf)
    }
    
    client.system_add_keyspace(keyspace)
    apply(name)
  }
  
  override def dropTable(name: String): Unit = {
    client.system_drop_keyspace(name)
  }

  override def truncateTable(name: String): Unit = {
    client.describe_keyspace(name).getCf_defs.foreach { cf =>
      client.truncate(cf.getName)
    }
  }

  /**
   * Cassandra client API doesn't support compaction.
   * This is actually a nop.
   */
  override def compactTable(name: String): Unit = {
    // fail silently
    log.warn("Cassandra client API doesn't support compaction")
  }
}

object Cassandra {
  def apply(host: String, port: Int): Cassandra = {
    // For ultra-wide row, we set the maxLength to 16MB.
    // Note that we also need to set the server side configuration
    // thrift_framed_transport_size_in_mb in cassandra.yaml
    val transport = new TFramedTransport(new TSocket(host, port), 16 * 1024 * 1024)
    transport.open

    new Cassandra(transport)
  }
}
