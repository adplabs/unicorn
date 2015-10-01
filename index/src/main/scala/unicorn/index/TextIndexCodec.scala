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

package unicorn.index

import java.nio.ByteBuffer

import smile.nlp.dictionary.{EnglishPunctuations, EnglishStopWords}
import smile.nlp.stemmer.PorterStemmer
import smile.nlp.tokenizer.{SimpleTokenizer, SimpleSentenceSplitter}
import unicorn.bigtable.{Cell, Column}
import unicorn.util._

/**
 * Calculate the cell in the index table for a text index (may include multiple columns) in the base table.
 * Natuarlly, text index doesn't support unique constraint. It also ignores the column order specification.
 *
 * @author Haifeng Li
 */
class TextIndexCodec(index: Index) extends IndexCodec {
  // Text index doesn't support unique constraint. Text index also ignores
  // the column order specification.
  require(index.unique == false)

  val buffer = ByteBuffer.allocate(64 * 1024)

  override def apply(row: Array[Byte], columns: Map[ByteArray, Map[ByteArray, Column]]): Seq[Cell] = {

    var timestamp = 0L
    index.columns.flatMap { indexColumn =>
      val column = columns.get(indexColumn.family).map(_.get(indexColumn.qualifier)).getOrElse(None)
      if (column.isDefined) {
        if (column.get.timestamp > timestamp) timestamp = column.get.timestamp
        val text = new String(column.get.value, utf8)
        val terms = tokenize(text)
        terms.map { case (term, pos) =>
          val key = index.prefixedIndexRowKey(term.getBytes(utf8), row) ++ row
          buffer.reset
          pos.foreach(buffer.putInt(_))
          Cell(key, IndexMeta.indexColumnFamily, indexColumn.qualifier, buffer.array, timestamp)
        }
      } else Seq()
    }
  }

  /** Sentence splitter. */
  val sentenceSpliter = SimpleSentenceSplitter.getInstance

  /** Tokenizer on sentences. */
  val tokenizer = new SimpleTokenizer

  /** Dictionary of stop words. */
  val stopWords = EnglishStopWords.DEFAULT

  /** Punctuation. */
  val punctuations = EnglishPunctuations.getInstance

  /** Optional word stemmer. */
  val stemmer = new PorterStemmer

  /**
   * Process each token (after filtering stop words, numbers, and optional stemming).
   */
  private def foreach[U](text: String)(f: ((String, Int)) => U): Unit = {
    var pos = 0

    sentenceSpliter.split(text).foreach { sentence =>
      tokenizer.split(sentence).foreach { token =>
        pos += 1
        val lower = token.toLowerCase
        if (!(punctuations.contains(lower) ||
          stopWords.contains(lower) ||
          lower.length == 1 ||
          lower.matches("[0-9\\.\\-\\+\\|\\(\\)]+"))) {
          val word = stemmer.stem(lower)
          f(word, pos)
        }
      }

      pos += 1
    }
  }

  private def tokenize(text: String): collection.mutable.Map[String, List[Int]] = {
    val terms = collection.mutable.Map[String, List[Int]]().withDefaultValue(Nil)

    var size = 0
    foreach(text) { case (word, pos) =>
      size += 1
      terms(word) = pos :: terms(word)
    }

    terms
  }
}
