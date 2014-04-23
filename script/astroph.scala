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

val server = AccumuloServer("poc", "cdldvtitavap015:2181,cdldvtitavap016:2181,cdldvtitavap017:2181", "tester", "adpadp")
server.createDataSet("wiki")
val table = server.dataset("astroph")

astroph(server, table, "/home/virtual/data/ca-AstroPh.txt")
