package com.adp.unicorn.store.cassandra

import com.adp.unicorn._
import com.adp.unicorn.store._
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.KsDef
import org.apache.cassandra.thrift.CfDef
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol

/**
 * Cassandra server adapter.
 *
 * @author Haifeng Li (293050)
 */
class CassandraServer(protocol: TProtocol) extends Database {
  val admin = new Cassandra.Client(protocol)
  
  override def dataset(name: String, visibility: Option[String], authorizations: Option[Seq[String]]): Dataset = {
    val client = new Cassandra.Client(protocol)
    client.set_keyspace(name)
    new CassandraKeyspace(client)
  }
  
  override def createDataSet(name: String): Unit = {
    createDataSet(name, "org.apache.cassandra.locator.SimpleStrategy", 1, Document.AttributeFamily, Document.RelationshipFamily)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): Unit = {
    val options = new java.util.HashMap[String, String]
    options.put("replication_factor", replication.toString)
    
    val keyspace = new KsDef
    keyspace.setName(name)
    keyspace.setStrategy_class(strategy)
    keyspace.setStrategy_options(options)
    
    columnFamilies.foreach { columnFamily =>
      val cf = new CfDef
      cf.setName(columnFamily)
      cf.setKeyspace(name)
      keyspace.addToCf_defs(cf)
    }
    
    admin.system_add_keyspace(keyspace)
  }
  
  override def dropDataSet(name: String): Unit = {
    admin.system_drop_keyspace(name)
  }
}

object CassandraServer {
  def apply(host: String, port: Int): CassandraServer = {
    // For ultra-wide row, we set the maxLength to 1G.
    // Note that we also need to set the server side configuration
    // thrift_framed_transport_size_in_mb in cassandra.yaml
    val transport = new TFramedTransport(new TSocket(host, port), 1024 * 1024 * 1024)
    transport.open
    
    val protocol = new TBinaryProtocol(transport)
    new CassandraServer(protocol)
  }
}
