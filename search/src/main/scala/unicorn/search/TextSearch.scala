package unicorn.search

import unicorn._, json._
import unicorn.doc.Document
import unicorn.store.Dataset
import smile.nlp.relevance.BM25

class TextSearch(storage: Dataset, numTexts: Long) extends TextIndex {
  val pagerank = new Document("unicorn.text.corpus.text.page_rank", "text_index").from(storage)
  val defaultPageRank = math.log(0.85 / numTexts)
  
  val textLength = new Document(TextBodyLengthKey, TextIndexFamily).from(storage)
  val titleLength = new Document(TextTitleLengthKey, TextIndexFamily).from(storage)
  val anchorLength = new Document(TextAnchorLengthKey, TextIndexFamily).from(storage)
  
  /**
   * Relevance ranking algorithm.
   */
  val ranker = new BM25
  
  /**
   * Search terms in corpus. The results are sorted by relevance.
   */
  def search(terms: String*): Array[((Document, String), Double)] = {
    val rank = scala.collection.mutable.Map[(Document, String), Double]().withDefaultValue(0.0)
    terms.foreach { term => search(term, rank) }    
    rank.toArray.sortBy(_._2).reverse
  }

  def search(term: String, rank: scala.collection.mutable.Map[(Document, String), Double]) {
    val lower = term.toLowerCase
    val word = stemmer match {
      case Some(stemmer) => stemmer.stem(lower)
      case None => lower
    }
      
    val key = word + TermIndexSuffix
    val invertedText = new Document(word + TermIndexSuffix, TextIndexFamily).from(storage).loadAttributes
    if (invertedText.attributes.size == 0) return
      
    val invertedTitle = new Document(word + TermTitleIndexSuffix, TextIndexFamily).from(storage).loadAttributes
    val invertedAnchor = new Document(word + TermAnchorIndexSuffix, TextIndexFamily).from(storage).loadAttributes
      
    val docs = (invertedText.map { case (docField, value) => docField }).toSeq     
    textLength.select(docs: _*)
    titleLength.select(docs: _*)
    anchorLength.select(docs: _*)
    
    var avgTextLength = 0.0
    var avgTitleLength = 0.0
    var avgAnchorLength = 0.0
      
    var numMatchedTexts = 0
    var numMatchedTitles = 0
    var numMatchedAnchors = 0
      
    invertedText.foreach { case (docField, value) =>
      val n1: Int = textLength(docField)
      if (n1 > 0) {
        numMatchedTexts += 1
        avgTextLength += n1
      }
        
      val n2: Int = titleLength(docField)
      if (n2 > 0) {
        numMatchedTitles += 1
        avgTitleLength += n2
      }
        
      val n3: Int = anchorLength(docField)
      if (n3 > 0) {
        numMatchedAnchors += 1
        avgAnchorLength += n3
      }
    }
      
    if (numMatchedTexts > 0) avgTextLength /= numMatchedTexts
    if (numMatchedTitles > 0) avgTitleLength /= numMatchedTitles
    if (numMatchedAnchors > 0) avgAnchorLength /= numMatchedAnchors
    
    pagerank.select(invertedText.map { case (docField, _) => docField }.toArray : _*)
      
    invertedText.foreach { case (docField, value) =>
      val id = docField.split(DocFieldSeparator, 2)

      if (id.length == 2) {
        val doc = Document(id(0)).from(storage)
        val field = id(1).replace(DocFieldSeparator, Document.FieldSeparator)
          
        val termFreq: Int = value
        val titleTermFreq: Int = invertedTitle(docField)
        val anchorTermFreq: Int = invertedAnchor(docField)

        val bm25 = ranker.score(termFreq, textLength(docField), avgTextLength,
              titleTermFreq, titleLength(docField), avgTitleLength,
              anchorTermFreq, anchorLength(docField), avgAnchorLength,
              numTexts, invertedText.size)
              
        val pr = pagerank(docField) match {
          case JsDouble(value) => math.log(value)
          case _ => defaultPageRank
        }
        
        rank((doc, field)) += (bm25 + pr)
      }
    }
  }
}

object TextSearch {
  def apply(storage: Dataset, numTexts: Long): TextSearch = {
    new TextSearch(storage, numTexts)
  }
}
