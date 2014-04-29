package com.adp.unicorn.text

import java.nio.ByteBuffer
import com.adp.unicorn._
import com.adp.unicorn.JsonValueImplicits._
import com.adp.unicorn.store.DataSet
import smile.nlp.relevance.BM25
import smile.nlp.stemmer.Stemmer

class TextSearch(storage: DataSet) extends TextIndex {

  val textLength = collection.mutable.Map[String, Int]().withDefaultValue(0)
  
  storage.get(TextBodyLengthKey, TextIndexFamily).foreach { case (key, value) =>
    val size: Int = ByteBuffer.wrap(value, 2, value.length - 2).getInt
    if (size > 0) {
      textLength(key) = size
    }
  }
  
  val titleLength = collection.mutable.Map[String, Int]().withDefaultValue(0)
  
  storage.get(TextTitleLengthKey, TextIndexFamily).foreach { case (key, value) =>
    val size: Int = ByteBuffer.wrap(value, 2, value.length - 2).getInt
    if (size > 0) {
      titleLength(key) = size
    }
  }
  
  val anchorLength = collection.mutable.Map[String, Int]().withDefaultValue(0)
  
  storage.get(TextAnchorLengthKey, TextIndexFamily).foreach { case (key, value) =>
    val size: Int = ByteBuffer.wrap(value, 2, value.length - 2).getInt
    if (size > 0) {
      anchorLength(key) = size
    }
  }
  
  /**
   * The number of texts in the corpus.
   */
  val numTexts = textLength.size
  
  /**
   * The number of words in the corpus.
   */
  val numWords: Long = textLength.values.foldLeft(0: Long) { _ + _ }

  /**
   * The average length of documents in the corpus.
   */
  val avgTextLength = if (numTexts > 0) numWords.toDouble / numTexts else 0

  val numTitles = titleLength.size
  val numTitleWords: Long = titleLength.values.foldLeft(0: Long) { _ + _ }
  val avgTitleLength = if (numTitles > 0) numTitleWords.toDouble / numTitles else 0

  val numAnchors = anchorLength.size
  val numAnchorWords: Long = anchorLength.values.foldLeft(0: Long) { _ + _ }
  val avgAnchorLength = if (numAnchors > 0) numAnchorWords.toDouble / numAnchors else 0

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
      val invertedText = new Document(word + TermIndexSuffix, TextIndexFamily).load(storage)
      val invertedTitle = new Document(word + TermTitleIndexSuffix, TextIndexFamily).load(storage)
      val invertedAnchor = new Document(word + TermAnchorIndexSuffix, TextIndexFamily).load(storage)
      invertedText.foreach { case (docField, value) =>
        val id = docField.split(DocFieldSeparator, 2)

        if (id.length == 2) {
          val doc = Document(id(0)).from(storage)
          val field = id(1).replace(DocFieldSeparator, Document.FieldSeparator)
          val termFreq: Int = value
          val titleTermFreq: Int = if (invertedTitle(id(0)) == JsonUndefinedValue) 0 else invertedTitle(id(0))
          val anchorTermFreq: Int = if (invertedAnchor(id(0)) == JsonUndefinedValue) 0 else invertedAnchor(id(0))
          val score = ranker.score(termFreq, textLength(docField), avgTextLength,
              titleTermFreq, titleLength(docField), avgTitleLength,
              anchorTermFreq, anchorLength(docField), avgAnchorLength,
              numTexts, invertedText.size)
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