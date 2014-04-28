/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.text

import smile.nlp.stemmer.Stemmer

/**
 * Text inverted index for full text search and relevance ranking.
 * 
 * @author Haifeng Li (293050)
 */
trait TextIndex {
  val TextIndexFamily = "text index"
  val TermIndexSuffix = " idx"
  val TermPositionSuffix = " pos"
  val DocFieldSeparator = "##"
  val TextBodySizeKey = "unicorn.text.corpus.text.size"
  val TextTitleSizeKey = "unicorn.text.corpus.text.title.size"
  val TextAnchorSizeKey = "unicorn.text.corpus.text.anchor.size"
  val TextPageRankKey = "unicorn.text.corpus.text.page_rank"
  val TextBrowseRankKey = "unicorn.text.corpus.text.browse_rank"

  /**
   * Optional stemmer.
   */
  var stemmer: Option[Stemmer] = None
}
