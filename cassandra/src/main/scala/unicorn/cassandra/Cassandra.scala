package unicorn.cassandra

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.cassandra.thrift.KsDef
import org.apache.cassandra.thrift.CfDef
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import unicorn.bigtable.BigTable

/**
 * Cassandra server adapter.
 *
 * @author Haifeng Li
 */
class Cassandra(transport: TFramedTransport) extends unicorn.bigtable.Database {
  val protocol = new TBinaryProtocol(transport)
  val admin = new Client(protocol)

  override def close: Unit = transport.close

  override def apply(name: String): BigTable = {
    new CassandraTable(this, name)
  }
  
  override def createTable(name: String, props: Properties, families: String*): Unit = {
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
    
    admin.system_add_keyspace(keyspace)
  }
  
  override def dropTable(name: String): Unit = {
    admin.system_drop_keyspace(name)
  }
}

object Cassandra {
  def apply(host: String, port: Int): Cassandra = {
    // For ultra-wide row, we set the maxLength to 1G.
    // Note that we also need to set the server side configuration
    // thrift_framed_transport_size_in_mb in cassandra.yaml
    val transport = new TFramedTransport(new TSocket(host, port), 1024 * 1024 * 1024)
    transport.open

    new Cassandra(transport)
  }
}
