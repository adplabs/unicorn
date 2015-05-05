package com.adp.unicorn.http

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.adp.unicorn._
import com.adp.unicorn.JsonValueImplicits._
import com.adp.unicorn.text.TextIndex

class LinkServlet extends HttpServlet {
  val data = Configuration.data
  val pagerank = new Document("unicorn.text.corpus.text.page_rank", "text_index").from(data)
  val pr = math.log(0.85 / Configuration.numTexts)
  val suffix = "##abstract"

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("application/json")
    response.setCharacterEncoding("UTF-8")
    
    val writer = response.getWriter
    
    val id = request.getParameter("id")
    if (id == null || id.isEmpty) {
      writer.write("""{"nodes": [], "links": []}""")
    } else {
      val doc = data.get(id)

      pagerank.select((doc.links.map { case ((_, target), _) => target + suffix }.toArray :+ (id + suffix)): _*)

      var idx = 0
      val escapedId = id.replace("\"", "\\\"")
      val rank = pagerank(id + suffix) match {
        case JsonDoubleValue(value) => math.log(value)
        case _ => pr
      }
      val center = s"""{"id": "$escapedId", "index": 0, "rank": $rank}"""
      
      val links = new Array[String](doc.links.size)
      val nodes = center +: doc.links.map { case ((_, target), value) =>
        val weight: Double = value
        idx += 1
        links(idx-1) = s"""{"source": 0, "target": $idx, "weight": $weight}"""
        
        val escaped = target.replace("\"", "\\\"")
        val rank = pagerank(target + suffix) match {
          case JsonDoubleValue(value) => math.log(value)
          case _ => pr
        }
        s"""{"id": "$escaped", "index": $idx, "rank": $rank}"""
      }.toArray
      
      writer.write("{\n")
      writer.write(""""nodes": [""")
      writer.write(nodes.deep.mkString(",\n  "))
      writer.write("],\n")
      
      writer.write(""""links": [""")
      writer.write(links.deep.mkString(",\n  "))
      writer.write("]\n}")
    }
  }
}