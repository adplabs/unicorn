#!/bin/bash
exec unicorn -nc -J-Xmx8192M "$0" "$@"
!#

import scala.io.Source

def triple(line: String): (String, String, String) = {
  val tokens = line.split(" ", 3)
  val subject = java.net.URLDecoder.decode(tokens(0).replace("<http://dbpedia.org/resource/", ""), "UTF-8").replace(">", "")
  val predicate = tokens(1)
  var obj = tokens(2)

  (subject, predicate, obj)
}

val pages = scala.collection.mutable.Map[String, Int]()
Source.fromFile("../../data/dbpedia/long_abstracts_en.nt").getLines.foreach { line =>
  if (!line.startsWith("#")) {
    val nt = triple(line)
    if (!pages.contains(nt._1)) {
        pages(nt._1) = -1
    }
  }
}

var pageIndex = 0
Source.fromFile("../../data/dbpedia/page_links_en.nt").getLines.foreach { line =>
  if (!line.startsWith("#")) {
    val nt = triple(line)
    if (pages.contains(nt._1) && pages(nt._1) == -1) {
        pages(nt._1) = pageIndex
        pageIndex += 1
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
Source.fromFile("../../data/dbpedia/page_links_en.nt").getLines.foreach { line =>
  if (!line.startsWith("#")) {
    val nt = triple(line)
    val source = nt._1
    val sink = java.net.URLDecoder.decode(nt._3.replace("<http://dbpedia.org/resource/", ""), "UTF-8").replace("> .", "")
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

          k += 1
          if (k % 1000000 == 0) println("build links matrix", k)
        }
      }
    }
  }
}
colIndex(col+1) = k

0 until n foreach { idx =>
  val l = colIndex(idx+1) - colIndex(idx)
  if (l > 0) colIndex(idx) until colIndex(idx+1) foreach { i => value(i) = value(i) / l}
}

val matrix = new smile.math.matrix.SparseMatrix(n, n, value.slice(0,k), rowIndex.slice(0,k), colIndex)
val rank = smile.math.matrix.EigenValueDecomposition.pagerank(matrix)

val pagerank = pages.toSeq.filter(_._2 != -1).map { case (page, index) =>
  (page, rank(index))
}.sortBy(-_._2)

pagerank.foreach { case (page, rank) =>
  println(page, rank)
}
