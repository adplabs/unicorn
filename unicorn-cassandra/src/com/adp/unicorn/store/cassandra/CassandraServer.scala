package com.adp.unicorn.store.cassandra

import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.thrift.KsDef
import org.apache.cassandra.thrift.CfDef
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import com.adp.unicorn.store.DataStore
import com.adp.unicorn.store.DataSet
import com.adp.unicorn.Document

class CassandraServer(protocol: TProtocol) extends DataStore {
  val admin = new Cassandra.Client(protocol)
  
  override def dataset(name: String, auth: String): DataSet = {
    val client = new Cassandra.Client(protocol)
    client.set_keyspace(name)
    new CassandraKeyspace(client)
  }
  
  override def createDataSet(name: String): DataSet = {
    createDataSet(name, "org.apache.cassandra.locator.SimpleStrategy", 1, Document.AttributeFamily, Document.RelationshipFamily)
  }
  
  override def createDataSet(name: String, strategy: String, replication: Int, columnFamilies: String*): DataSet = {
    val options = new java.util.HashMap[String, String]
    options.put("replication_factor", replication.toString)
    
    val keyspace = new KsDef
    keyspace.setName(name)
    keyspace.setStrategy_class(strategy)
    keyspace.setStrategy_options(options)
    keyspace.setReplication_factor(replication)
    
    columnFamilies.foreach { columnFamily =>
      val cf = new CfDef
      cf.setName(columnFamily)
      cf.setKeyspace(name)
      keyspace.addToCf_defs(cf)
    }
    
    admin.system_add_keyspace(keyspace)
    dataset(name)
  }
  
  override def dropDataSet(name: String): Unit = {
    admin.system_drop_keyspace(name)
  }
}

object CassandraServer {
  def apply(host: String, port: Int): CassandraServer = {
    val transport = new TFramedTransport(new TSocket(host, port))
    transport.open
    
    val protocol = new TBinaryProtocol(transport)
    new CassandraServer(protocol)
  }
}
