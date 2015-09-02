/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn.search

import unicorn._, json._
import unicorn.core.Document
import unicorn.store.Dataset
import smile.nlp.tokenizer.SimpleTokenizer
import smile.nlp.tokenizer.SimpleSentenceSplitter
import smile.nlp.dictionary.EnglishStopWords
import smile.nlp.dictionary.EnglishPunctuations

/**
 * @author Haifeng Li
 */
class TextIndexBuilder(storage: Dataset) extends TextIndex {

  val textLength = new Document(TextBodyLengthKey, TextIndexFamily)
  val titleLength = new Document(TextTitleLengthKey, TextIndexFamily)
  val anchorLength = new Document(TextAnchorLengthKey, TextIndexFamily)

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
   * Process each token (after filtering stop words, numbers, and optional stemming).
   */
  def foreach[U](text: String)(f: ((String, Int)) => U): Unit = {
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

          f(word, pos)
        }
      }
      
      pos += 1
    }    
  }
  
  /**
   * Add a text into index.
   * @param doc   The id of document that owns the text.
   * @param field The filed name of text in the document.
   * @param text  The text body.
   */
  private def add(doc: String, field: String, text: String, sizeDoc: Document, indexKeySuffix: String) {
    val termFreq = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    //val termPos = scala.collection.mutable.Map[String, Array[Int]]().withDefaultValue(Array[Int]())

    var size = 0
    foreach(text) { case (word, pos) =>
      size += 1
      termFreq(word) += 1
      //termPos(word) :+ pos
    }
      
    val key = doc + DocFieldSeparator + field.replace(Document.FieldSeparator, DocFieldSeparator)

    sizeDoc(key) = JsInt(size)
    
    termFreq.foreach { case (word, freq) =>
      //TODO storage.put(word + indexKeySuffix, TextIndexFamily, key, freq)
    }

    /*
    termPos.foreach { case (word, pos) =>
      storage.put(word + TermPositionSuffix, TextIndexFamily, key, JsonBlobValue(pos).bytes)
    }
    */

    // termFreq and termPos updates will also be commit here.
    sizeDoc into storage
  }
  
  /**
   * Add a text into index.
   * @param doc   The id of document that owns the text.
   * @param field The filed name of text in the document.
   * @param text  The text body.
   */
  def add(doc: String, field: String, text: String) {
    add(doc, field, text, textLength, TermIndexSuffix)
  }
  
  /**
   * Add a title into index.
   * @param doc   The id of document that owns the text.
   * @param field The filed name of text in the document.
   * @param title  The title.
   */
  def addTitle(doc: String, field: String, title: String) {
    add(doc, field, title, titleLength, TermTitleIndexSuffix)
  }
  
  /**
   * Add an anchor text into index.
   * @param doc   The id of document that owns the text.
   * @param field The filed name of text in the document.
   * @param anchor  The anchor text.
   */
  def addAnchor(doc: String, field: String, anchor: String) {
    add(doc, field, anchor, anchorLength, TermAnchorIndexSuffix)
  }
}

object TextIndexBuilder {
  def apply(storage: Dataset): TextIndexBuilder = {
    new TextIndexBuilder(storage)
  }
}
