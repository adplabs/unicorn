import scala.io.Source

def triple(line: String): (String, String, String) = {
  val tokens = line.split(" ", 3)
  val subject = java.net.URLDecoder.decode(tokens(0).replace("<http://dbpedia.org/resource/", "")).replace(">", "")
  val predicate = tokens(1)
  var obj = tokens(2)

  (subject, predicate, obj)
}

val pages = scala.collection.mutable.Map[String, Int]()
import scala.util.control.Breaks._
Source.fromFile("/Users/lihb/data/dbpedia/long_abstracts_en.nt").getLines.foreach { line =>
  if (!line.startsWith("#")) {
    val nt = triple(line)
    if (!pages.contains(nt._1)) {
        pages(nt._1) = -1
        //if (pages.size > 50000) break
    }
  }
}

var pageIndex = 0
Source.fromFile("/Users/lihb/data/dbpedia/page_links_en.nt").getLines.foreach { line =>
  if (!line.startsWith("#")) {
    val nt = triple(line)
    if (pages.contains(nt._1) && pages(nt._1) == -1) {
        pages(nt._1) = pageIndex
        pageIndex += 1
        //if (pageIndex > 50000) break
    }
  }
}

val d = 0.85
val n = pageIndex
val len = 172308908
val colIndex = new Array[Int](n + 1)
val rowIndex = new Array[Int](len)
val value = new Array[Double](len)

var k = 0
var col = 0
colIndex(0) = 0
Source.fromFile("/Users/lihb/data/dbpedia/page_links_en.nt").getLines.foreach { line =>
  if (!line.startsWith("#")) {
    val nt = triple(line)
    val source = nt._1
    val sink = java.net.URLDecoder.decode(nt._3.replace("<http://dbpedia.org/resource/", "")).replace("> .", "")
    if (pages.contains(source) && pages.contains(sink)) {
      val j = pages(source)
      val i = pages(sink)
      if (i != -1 && j != -1) {
        value(k) = 1.0
        rowIndex(k) = i
        if (j < col) {
          println("smaller col index", j, col, "skip")
        } else {
          if (j > col) {
            (col+1) to j foreach { idx => colIndex(idx) = k }
            col = j
          }
          //if (j >= 50000) break
          k += 1
          if (k % 100000 == 0) println(k)
        }
      }
    }
  }
}
colIndex(col+1) = k

0 until n foreach { idx =>
  val l = colIndex(idx+1) - colIndex(idx)
  if (l > 0) colIndex(idx) until colIndex(idx+1) foreach { i => value(i) = value(i) / l}
  //else println(l, colIndex(idx+1), colIndex(idx), idx)
}

val matrix = new smile.math.matrix.SparseMatrix(n, n, value.slice(0,k), rowIndex.slice(0,k), colIndex)
val pr = smile.math.matrix.EigenValueDecomposition.pagerank(matrix)

val server = CassandraServer("127.0.0.1", 9160)
val table = server.dataset("dbpedia")

val rank = new Document("unicorn.text.corpus.text.page_rank", "text_index")
var batch = 0
pages.foreach { case (page, index) =>
  if (index != -1 && pr(index) > 0.0) {
    rank(page+"##abstract") = pr(index)
    batch = batch + 1
    if (batch % 10000 == 0) rank into table
  }
}
rank into table
