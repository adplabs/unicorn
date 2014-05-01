import scala.io.Source

def astroph(server: DataStore, table: DataSet, files: String*): Unit = {
  files.foreach { file =>
    var doc: Document = null
    Source.fromFile(file).getLines.foreach { line =>
      if (!line.startsWith("#")) {
        val edge = line.split("\t")
        if (doc == null) doc = Document(edge(0))
        if (edge(0) != doc.id) {
          println(doc.id)
          doc into table
          doc = Document(edge(0))
        } else {
          doc("works with", edge(1)) = true
        }
      }
    }
  }
}

val server = CassandraServer("127.0.0.1", 9160)
server.createDataSet("astroph")
val table = server.dataset("astroph")

astroph(server, table, "/Users/lihb/data/ca-AstroPh.txt")
