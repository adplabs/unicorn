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

import smile.nlp.stemmer.Stemmer

/**
 * Text inverted index for full text search and relevance ranking.
 * 
 * @author Haifeng Li
 */
trait TextIndex {
  val TextIndexFamily = "text_index"
  val TermIndexSuffix = " idx"
  val TermTitleIndexSuffix = " tidx"
  val TermAnchorIndexSuffix = " aidx"
  val TermPositionSuffix = " pos"
  val DocFieldSeparator = "##"
    
  val CorpusMetaKey = "unicorn.text.corpus.meta"
  val TextBodyLengthKey = "unicorn.text.corpus.text.size"
  val TextTitleLengthKey = "unicorn.text.corpus.text.title.size"
  val TextAnchorLengthKey = "unicorn.text.corpus.text.anchor.size"
  val PageRankKey = "unicorn.text.corpus.text.page_rank"
  val BrowseRankKey = "unicorn.text.corpus.text.browse_rank"

  /**
   * Optional stemmer.
   */
  var stemmer: Option[Stemmer] = None
}
