/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.http

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.adp.unicorn.JsonValueImplicits._

class DocumentServlet extends HttpServlet {

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    response.setCharacterEncoding("UTF-8")
    
    val id = request.getParameter("id")
    if (id == null || id.isEmpty) {
      request.getRequestDispatcher("index.html").forward(request, response)
      return
    }
    
    val doc = Configuration.data.get(id)

    val writer = response.getWriter
    writer.write(Configuration.skeletonTop)
    writer.write("""<pre><code class="javascript">""")
    writer.write(doc.json.toString("", ",<br>"))
    writer.write("</code></pre>")
    
    writer.write("<p><ol>")
    doc.foreachRelationship { case ((label, target), value) =>
      writer.write(s"""<li><a href="/doc/?id=$target">$target</a>""")
    }
    writer.write("</ol>")
    writer.write(Configuration.skeletonBottom)
  }
}