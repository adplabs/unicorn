/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.text

import com.adp.unicorn._
import com.adp.unicorn.DocumentImplicits._
import com.adp.unicorn.store.DataSet
import smile.nlp.relevance.Relevance
import smile.nlp.relevance.RelevanceRanker
import smile.nlp.relevance.BM25
import smile.nlp.stemmer.Stemmer
import smile.nlp.tokenizer.SimpleTokenizer
import smile.nlp.tokenizer.SimpleSentenceSplitter
import smile.nlp.dictionary.EnglishStopWords
import smile.nlp.dictionary.EnglishPunctuations

/**
 * Text corpus for full text search and relevance ranking.
 * 
 * @author Haifeng Li (293050)
 */
class TextCorpus(context: DataSet) {
  private val TextCorpusTermIndexSuffix = " index"
  private val TextCorpusMetaDataKey = "unicorn.text.corpus.meta"
  private val meta = TextCorpusMetaDataKey of context
  
  /**
   * The number of words in the corpus.
   */
  var numWords: Long = meta.numWords match {
    case JsonIntValue(value) => value
    case JsonLongValue(value) => value
    case _ => 0
  }

  /**
   * The number of texts in the corpus.
   */
  var numTexts: Long = meta.numTexts match {
    case JsonIntValue(value) => value
    case JsonLongValue(value) => value
    case _ => 0
  }
  
  /**
   * The number of unique terms in the corpus.
   */
  var numTerms: Long = meta.numTerms match {
    case JsonIntValue(value) => value
    case JsonLongValue(value) => value
    case _ => 0
  }

  /**
   * The average size of documents in the corpus.
   */
  var avgTextSize: Int = meta.avgTextSize match {
    case JsonIntValue(value) => value
    case _ => 0
  }

  /**
   * The total frequency of the term in the corpus.
   */
  val freq = meta.freq match {
    case x: JsonObjectValue => x
    case _ => collection.mutable.Map[String, Double]()
  }

  /**
   * Optional stemmer.
   */
  var stemmer: Option[Stemmer] = None
  
  /**
   * Sentence splitter.
   */
  var sentenceSpliter = SimpleSentenceSplitter.getInstance
  
  /**
   * Tokenizer on sentences
   */
  var tokenizer = new SimpleTokenizer

  /**
   * Dictionary of stop words.
   */
  var stopWords = EnglishStopWords.DEFAULT
  
  /**
   * Punctuations.
   */
  var punctuations = EnglishPunctuations.getInstance
  
  /**
   * Add a text into corpus.
   */
  def add(key: String, column: String, text: String) {
    val freq = scala.collection.mutable.Map[String, Int]()
    var textSize = 0
    
    sentenceSpliter.split(text).foreach { sentence =>
      tokenizer.split(sentence).foreach { token =>
        val lower = token.toLowerCase
        if (!(punctuations.contains(lower) || stopWords.contains(lower))) {
          val word = stemmer match {
            case Some(stemmer) => stemmer.stem(lower)
            case None => lower
          }

          freq(word) += 1
          textSize += 1
        }
      }
    }

    freq.foreach { case (word, freq) =>
      context.put(word + TextCorpusTermIndexSuffix, Document.AttributeFamily, key + Document.FieldSeparator + column, JsonIntValue(freq).bytes)
    }
    context.commit
  }
  
  /**
   * Relevance ranking algorithm.
   */
  val ranker = new BM25
  
  /**
   * Search terms in corpus. The results are sorted by relevance.
   */
  def search(terms: String*): Array[(Document, Double)] = {
    val rank  = scala.collection.mutable.Map[Document, Double]()
    terms.foreach { term =>
      val key = term + TextCorpusTermIndexSuffix
      val invertedFile = key of context
      invertedFile.foreach { case (id, value) =>
        val doc = Document(id).from(context)
        val freq = value match {
          case JsonIntValue(value) => value
          case _ => 0
        }
        rank(doc) += 0.0 //ranker.rank(freq, textSize, avgTextSize, numTexts, invertedFile.size) 
      }
    }
    
    rank.toArray.sortBy(_._2).reverse
  }
}

object TextCorpus {
  def apply(context: DataSet): TextCorpus = {
    new TextCorpus(context)
  }
}
