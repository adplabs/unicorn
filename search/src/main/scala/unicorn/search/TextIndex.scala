/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package unicorn.search

import smile.nlp.stemmer.Stemmer

/**
 * Text inverted index for full text search and relevance ranking.
 * 
 * @author Haifeng Li (293050)
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
