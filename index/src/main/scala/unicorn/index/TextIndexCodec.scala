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
import java.nio.charset.Charset
import smile.nlp.dictionary.{EnglishPunctuations, EnglishStopWords}
import smile.nlp.stemmer.{PorterStemmer, Stemmer}
import smile.nlp.tokenizer.{SimpleTokenizer, SimpleSentenceSplitter}
import unicorn.bigtable.Cell
import unicorn.json.JsonSerializerHelper
import unicorn.util._

/**
 * Calculate the cell in the index table for a text index (may include multiple columns) in the base table.
 * Naturally, text index doesn't support unique constraint. It also ignores the column order specification.
 *
 * @author Haifeng Li
 */
class TextIndexCodec(val index: Index, codec: TextCodec = new SimpleTextCodec, stemmer: Option[Stemmer] = Some(new PorterStemmer)) extends IndexCodec {
  require(index.indexType == IndexType.Text)

  val valueBuffer = ByteBuffer.allocate(64 * 1024)

  override def apply(tenant: Option[Array[Byte]], row: ByteArray, columns: ColumnMap): Seq[Cell] = {
    index.columns.flatMap { indexColumn =>
      val column = columns.get(index.family).map(_.get(indexColumn.qualifier)).getOrElse(None)
      if (column.isDefined) {
        val timestamp = column.get.timestamp
        val text = codec.decode(column.get.value)
        val terms = tokenize(text)
        terms.map { case (term, pos) =>
          resetBuffer(tenant)
          val bytes = term.getBytes(utf8)
          buffer.putInt(bytes.size)
          buffer.put(bytes)
          buffer.putInt(indexColumn.qualifier.bytes.size)
          buffer.put(indexColumn.qualifier)
          val key = ByteArray(buffer)

          valueBuffer.clear
          pos.foreach(valueBuffer.putInt(_))

          Cell(key, IndexColumnFamily, row, byteBuffer2ByteArray(valueBuffer), timestamp)
        }
      } else Seq.empty
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

trait TextCodec {
  def decode(bytes: Array[Byte]): String
}

class SimpleTextCodec(charset: Charset = utf8) extends TextCodec {
  override def decode(bytes: Array[Byte]): String = {
    new String(bytes, charset)
  }
}
/*
class JsStringTextCodec extends TextCodec with JsonSerializerHelper {
  override def decode(bytes: Array[Byte]): String = {
    string()(ByteBuffer.wrap(bytes)).value
  }
}
*/