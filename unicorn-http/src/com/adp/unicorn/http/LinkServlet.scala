package com.adp.unicorn.http

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.adp.unicorn._
import com.adp.unicorn.JsonValueImplicits._
import com.adp.unicorn.text.TextIndex

class LinkServlet extends HttpServlet {
  val pagerank = new Document("unicorn.text.corpus.text.page_rank", "text_index").from(Configuration.data)
  val pr = -math.log(1.0 / Configuration.numTexts)

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("application/json")
    response.setCharacterEncoding("UTF-8")
    
    val writer = response.getWriter
    
    val id = request.getParameter("id")
    if (id == null || id.isEmpty) {
      writer.write("""{"nodes": [], "links": []}""")
    } else {
      val doc = Configuration.data.get(id)

      pagerank.select((doc.links.map { case ((_, target), _) => target }.toArray :+ id): _*)
      
      var idx = 0
      val escapedId = id.replace("\"", "\\\"")
      val rank = pagerank(id) match {
        case JsonDoubleValue(value) => -math.log(value)
        case _ => pr
      }
      val center = s"""{"id": "$escapedId", "index": 0, "weight": $rank}"""
      
      val links = new Array[String](doc.links.size)
      val nodes = center +: doc.links.map { case ((_, target), value) =>
        val weight: Double = value
        idx += 1
        links(idx-1) = s"""{"source": 0, "target": $idx, "weight": $weight}"""
        
        val escaped = target.replace("\"", "\\\"")
        val rank = pagerank(target) match {
          case JsonDoubleValue(value) => -math.log(value)
          case _ => pr
        }
        s"""{"id": "$escaped", "index": $idx, "weight": $rank}"""
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