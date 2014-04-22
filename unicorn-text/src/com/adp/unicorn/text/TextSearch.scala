package com.adp.unicorn.text

import com.adp.unicorn._
import com.adp.unicorn.DocumentImplicits._
import com.adp.unicorn.store.DataSet
import smile.nlp.relevance.BM25
import smile.nlp.stemmer.Stemmer

class TextSearch(storage: DataSet) extends TextIndex {

  val textSize = collection.mutable.Map[String, Int]().withDefaultValue(0)
  
  /**
   * The number of words in the corpus.
   */
  var numWords: Long = 0

  /**
   * The number of texts in the corpus.
   */
  var numTexts: Long = 0

  (TextSizeKey of storage).json.value.foreach { case (key, value) =>
    val size = value match {
      case JsonIntValue(value) => value
      case _ => 0
    }
    
    if (size > 0) {
      numWords += size
      numTexts += 1
      textSize(key) = size
    }
  }
  
  /**
   * The average size of documents in the corpus.
   */
  val avgTextSize = numWords.toDouble / numTexts.toDouble

  /**
   * Relevance ranking algorithm.
   */
  val ranker = new BM25
  
  /**
   * Search terms in corpus. The results are sorted by relevance.
   */
  def search(terms: String*): Array[((Document, String), Double)] = {
    val rank = scala.collection.mutable.Map[(Document, String), Double]().withDefaultValue(0.0)
    
    terms.foreach { term =>
      val lower = term.toLowerCase
      val word = stemmer match {
        case Some(stemmer) => stemmer.stem(lower)
        case None => lower
      }
      
      val key = word + TermIndexSuffix
      val invertedFile = key of storage
      invertedFile.foreach { case (docField, value) =>
        val id = docField.split(DocFieldSeparator, 2)

        if (id.length == 2) {
          val doc = Document(id(0)).from(storage)
          val field = id(1).replace(DocFieldSeparator, Document.FieldSeparator)
        
          val tf = value match {
            case JsonIntValue(value) => value
            case JsonDoubleValue(value) => value
            case _ => 0
          }

          val score = ranker.rank(tf, textSize(docField), avgTextSize, numTexts, invertedFile.size.toLong)
          rank((doc, field)) += score        
        }
      }
    }
    
    rank.toArray.sortBy(_._2).reverse
  }
}

object TextSearch {
  def apply(storage: DataSet): TextSearch = {
    new TextSearch(storage)
  }

}