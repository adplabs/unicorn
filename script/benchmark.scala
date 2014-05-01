import scala.io.Source

// measure running time of a function/block 
def time[A](runs: Int = 1)(f: => A) = {
  val s = System.nanoTime
  val results = 1 to runs map { i => f }
  println("time: " + (System.nanoTime - s)/1e6/runs + " ms")
  results(0)
}

val person = Document("293050")
person("name") = "Haifeng"
person("gender") = "Male"
person("salary") = 1.0

// Create another document 
val address = Document("293050")
address.street = "135 W. 18th ST"
address.city = "New York"
address.state = "NY"
address.zip = 10011

// add a doucment into another one
person.address = address
// add an array into a document
person.projects = Array("GHCM", "Analytics")

person("work with", "Jim") = true
person("work with", "Mike") = true
person("report to", "Jerome") = true

def writeAbstracts(table: DataSet, files: String*): Unit = {
  var n = 0
  files.foreach { file =>
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = line.split(" ", 3)
        val doc = Document(nt(0))
        doc("abstract") = nt(2)
        doc into table
        n += 1
        if (n % 10000 == 0) println(n)
        if (n > 100000) return
      }
    }
  }
}

def readAbstracts(table: DataSet, files: String*): Unit = {
  var n = 0
  files.foreach { file =>
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val nt = line.split(" ", 3)
        table get nt(0)
        n += 1
        if (n % 10000 == 0) println(n)
        if (n > 100000) return
      }
    }
  }
}

def benchmark(table: DataSet, runs: Int = 100, throughput: Boolean = true) {
  // warm up
  table get "row1"
  person into table

  println("get empty doc:")
  time(runs) { table get "row1" }
  println("put small doc:")
  time(runs) { table put person.copy("test") }
  println("get small doc:")
  time(runs) { table get "test" }

  val large = person.copy("large")
  large.wiki = """
  A [[w:Linux distribution|Linux distribution]] is a software set with the Linux operating system and [[w:GNU|GNU]] software. The source code of Linux and GNU software is freely available and can be legally distributed among users. Big Linux is a ''Live'' Compact Disc version of Linux, which means it may run directly from the CD or optionally be installed on the hard disk.
 
According to its developers, Big Linux aims to be easy to use and comes with many programs that are used by home users and offices. Some included programs are: [[w:KDE|KDE]], [[w:Java (programming language)|Java]], [[w:XMMS|XMMS]], [[w:Kaffeine|Kaffeine]], [[w:xine|xine]], [[w:Kopete|Kopete]], YES, [[w:XChat|XChat]], [[w:Fullt|Fullt]], [[w:aMSN|aMSN]], the Windows runtime environments [[w:Wine (software)|Wine]] and [[w:Cedega|Cedega]], the peer-to-peer applications [[w:xMule|xMule]], [[w:BitTorrent|BitTorrent]] and Apollon, [[w:OpenOffice.org|OpenOffice.org]], [[w:GIMP|GIMP]] and [[w:AbiWord|AbiWord]], the email clients [[w:Kmail|Kmail]] and [[w:Mozilla Thunderbird|Thunderbird]], and the internet browsers [[w:Mozilla|Mozilla]] and the [[w:proprietary software|proprietary]] [[w:Opera (web browser)|Opera]] browser.

Some features of version 2.0 beta 3 are: 3 versions of the Linux kernel -- 2.4.25 (for those who use [[w:Softmodem|Winmodems]]), 2.4.27 (stable) and 2.6.8 (performance) --, automatic detection of printers, higher boot speed, complete system of network and servers  and an automatic hardware detection mechanism. The graphical environment (known as [[w:X Window System|X11]]) can have several configurations separated by kernel. In this way, the user can have an accelerated graphical environment configuration and an unaccelerated graphical environment configuration at the same time. If there is some trouble with the accelerated graphical environment configuration, this allows the user to switch to the unaccelerated one.
"""
  large.nosql = """
Primary and master nodes are the nodes that can accept writes. MongoDB’s replication is "single-master:" only one node can accept write operations at a time.

In a replica set, if the current “primary” node fails or becomes inaccessible, the other members can autonomously elect one of the other members of the set to be the new “primary”.

By default, clients send all reads to the primary; however, read preference is configurable at the client level on a per-connection basis, which makes it possible to send reads to secondary nodes instead.

Secondary and slave nodes are read-only nodes that replicate from the primary.

Replication operates by way of an oplog, from which secondary/slave members apply new operations to themselves. This replication process is asynchronous, so secondary/slave nodes may not always reflect the latest writes to the primary. But usually, the gap between the primary and secondary nodes is just few milliseconds on a local network connection.

database files are committed every 60 seconds.
The journal of operations (a write-ahead log) is committed every 100 milliseconds.

a composite column of OriginalColumnName::TimeUUID. Then you could slice out all columns by the OriginalColumnName to get the historical values of that column.

RegionServer failover takes 10 to 15 minutes. HBase partitions rows into regions, each managed by a RegionServer. The RegionServer is a single point of failure for its region; when it goes down, a new one must be selected and write-ahead logs must be replayed before writes or reads can be served again.

According to http://blog.adku.com/2011/02/hbase-vs-cassandra.html, HBase region servers actually crashed on us consistently. Cassandra never crashed once
  """
  println("put large doc:")
  time(runs) { table put large.copy("large") }
  println("get large doc:")
  time(runs) { table get "large" }

  if (throughput) {
    println("put dbpedia abstracts:")
    time(1) { writeAbstracts(table, "/home/virtual/data/dbpedia/long_abstracts_en.nt") }
    println("get dbpedia abstracts:")
    time(1) { readAbstracts(table, "/home/virtual/data/dbpedia/long_abstracts_en.nt") }
  }
}

// connect to Accumulo server
val server = AccumuloServer("poc", "127.0.0.1:2181", "tester", "adpadp")
val table = server.dataset("small", "public", "public")
benchmark(table)

// HBase
val server = HBaseServer()
val table = server.dataset("small")
benchmark(table)

// Cassandra
val server = CassandraServer("127.0.0.1", 9160)
val table = server.dataset("small")
benchmark(table)
