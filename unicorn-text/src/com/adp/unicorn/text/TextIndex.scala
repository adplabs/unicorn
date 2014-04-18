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
 * Text inverted index for full text search and relevance ranking.
 * 
 * @author Haifeng Li (293050)
 */
class TextIndex(context: DataSet) {
  private val TermIndexSuffix = " index"
  private val DocFieldSeparator = "##"
  private val MetaDataKey = "unicorn.text.corpus.meta"
  private val TextSizeKey = "unicorn.text.corpus.text.size"

  val meta = MetaDataKey of context
  val textSize = TextSizeKey of context
  
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
   * The average size of documents in the corpus.
   */
  var avgTextSize: Int = meta.avgTextSize match {
    case JsonIntValue(value) => value
    case _ => 0
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
   * @param doc   The id of document that owns the text.
   * @param field The filed name of text in the document.
   * @param text  The text itself.
   */
  def add(doc: String, field: String, text: String) {
    val freq = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    var size = 0
    
    sentenceSpliter.split(text).foreach { sentence =>
      tokenizer.split(sentence).foreach { token =>
        val lower = token.toLowerCase
        if (!(punctuations.contains(lower) ||
              stopWords.contains(lower) ||
              lower.length == 1 ||
              lower.matches("[0-9\\.\\-\\|\\(\\)]+"))) {
          val word = stemmer match {
            case Some(stemmer) => stemmer.stem(lower)
            case None => lower
          }

          freq(word) += 1
          size += 1
        }
      }
    }
    
    val key = doc + DocFieldSeparator + field.replace(Document.FieldSeparator, DocFieldSeparator)
    if (textSize(key) != JsonUndefinedValue) {
      val oldSize = textSize(key) match {
        case JsonIntValue(value) => value
        case _ => 0
      }
      
      numWords = numWords - oldSize + size
      avgTextSize = ((avgTextSize * numTexts - oldSize + size) / numTexts).toInt
    } else {
      avgTextSize = ((avgTextSize * numTexts + size) / (numTexts + 1)).toInt
      numTexts += 1
      numWords += size
    }
    
    textSize(key) = size    
    meta.numWords = numWords
    meta.avgTextSize = avgTextSize
    meta.numTexts = numTexts
    
    freq.foreach { case (word, freq) =>
      context.put(word + TermIndexSuffix, Document.AttributeFamily, key, JsonIntValue(freq).bytes)
    }
    
    textSize into context
    meta into context
    context.commit
  }
  
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
      val invertedFile = key of context
      invertedFile.foreach { case (docField, value) =>
        val id = docField.split(DocFieldSeparator, 2)
        val doc = Document(id(0)).from(context)
        val field = if (id.length == 2) id(1).replace(DocFieldSeparator, Document.FieldSeparator) else ""
        
        val freq = value match {
          case JsonIntValue(value) => value
          case _ => 0
        }
        
        val docSize = textSize(docField) match {
          case JsonIntValue(value) => value
          case _ => 100
        }
        
        val score = ranker.rank(freq, docSize, avgTextSize, numTexts, invertedFile.size.toLong)
        rank((doc, field)) += score
      }
    }
    
    rank.toArray.sortBy(_._2).reverse
  }
}

object TextIndex {
  def apply(context: DataSet): TextIndex = {
    new TextIndex(context)
  }
}
