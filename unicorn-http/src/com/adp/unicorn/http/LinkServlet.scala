package com.adp.unicorn.http

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.adp.unicorn.JsonValueImplicits._

class LinkServlet extends HttpServlet {

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("application/json")
    response.setCharacterEncoding("UTF-8")
    
    val writer = response.getWriter
    
    val id = request.getParameter("id")
    if (id == null || id.isEmpty) {
      writer.write("""{"nodes": [], "links": []}""")
    } else {
      writer.write("{\n")
      val doc = Configuration.data.get(id)

      writer.write(""""nodes": [""")
      var idx = 0
      val escapedId = id.replace("\"", "\\\"")
      val links = new Array[String](doc.links.size)
      val nodes = s"""{"id": "$escapedId", "index": 0}""" +: doc.links.map { case ((_, target), value) =>
        val weight: Double = value
        idx += 1
        links(idx-1) = s"""{"source": 0, "target": $idx, "weight": $weight}"""
        val escaped = target.replace("\"", "\\\"")
        s"""{"id": "$escaped", "index": $idx}"""
      }.toArray
      writer.write(nodes.deep.mkString(",\n  "))
      writer.write("],\n")
      
      writer.write(""""links": [""")
      writer.write(links.deep.mkString(",\n  "))
      writer.write("]\n}")
    }
  }
}