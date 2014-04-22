package com.adp.unicorn.text

import com.adp.unicorn._
import com.adp.unicorn.DocumentImplicits._
import com.adp.unicorn.store.DataSet
import smile.nlp.stemmer.Stemmer
import smile.nlp.tokenizer.SimpleTokenizer
import smile.nlp.tokenizer.SimpleSentenceSplitter
import smile.nlp.dictionary.EnglishStopWords
import smile.nlp.dictionary.EnglishPunctuations

class TextIndexBuilder(storage: DataSet) extends TextIndex {

  val textSize = TextSizeKey of storage

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
   * Punctuation.
   */
  var punctuations = EnglishPunctuations.getInstance
  
  /**
   * Add a text into index.
   * @param doc   The id of document that owns the text.
   * @param field The filed name of text in the document.
   * @param text  The text itself.
   * @param title The optional text title.
   * @param anchorText The optional (all) anchor labels of links to this document field.
   */
  def add(doc: String, field: String, text: String, title: String = "", anchorText: String = "") {
    val termFreq = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    val termPos = scala.collection.mutable.Map[String, Array[Int]]().withDefaultValue(Array[Int]())

    var size = 0
    var pos = 0
    
    sentenceSpliter.split(text).foreach { sentence =>
      tokenizer.split(sentence).foreach { token =>
        pos += 1
        val lower = token.toLowerCase
        if (!(punctuations.contains(lower) ||
              stopWords.contains(lower) ||
              lower.length == 1 ||
              lower.matches("[0-9\\.\\-\\+\\|\\(\\)]+"))) {
          val word = stemmer match {
            case Some(stemmer) => stemmer.stem(lower)
            case None => lower
          }

          size += 1
          termFreq(word) += 1
          termPos(word) :+ pos
        }
      }
      
      pos += 1
    }
    
    val key = doc + DocFieldSeparator + field.replace(Document.FieldSeparator, DocFieldSeparator)

    textSize(key) = size    
    
    termFreq.foreach { case (word, freq) =>
      storage.put(word + TermIndexSuffix, Document.AttributeFamily, key, JsonIntValue(freq).bytes)
    }

    termPos.foreach { case (word, pos) =>
      storage.put(word + TermPositionSuffix, Document.AttributeFamily, key, JsonBlobValue(pos).bytes)
    }

    // termFreq and termPos updates will also be commit here.
    textSize into storage
  }
}

object TextIndexBuilder {
  def apply(storage: DataSet): TextIndexBuilder = {
    new TextIndexBuilder(storage)
  }
}
