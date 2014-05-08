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
  val data = Configuration.data

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    response.setCharacterEncoding("UTF-8")
    
    val id = request.getParameter("id")
    if (id == null || id.isEmpty) {
      request.getRequestDispatcher("index.html").forward(request, response)
      return
    }
    
    val doc = data.get(id)

    val writer = response.getWriter
    writer.write(Configuration.skeletonTop(id, id))
    
    // doc
    writer.write("""<div id="doc" style="clear:both;"><pre><code class="javascript">""")
    writer.write(doc.json.toString("", ",<br>"))
    writer.write("</code></pre></div>")
    
    // links
    writer.write("""<div id="link" style="width:30%; float:left;"><ul>""")
    doc.foreachRelationship { case ((label, target), value) =>
      writer.write(s"""<li><a href="/doc/?id=$target">$target</a>""")
    }
    writer.write("</ul></div>")
    
    // d3
    writer.write("""<div id="network" style="width:70%; float:right;"></div>""")
    writer.write("""<script type="text/javascript" src="/network.js"></script>""")
  
    writer.write(Configuration.skeletonBottom)
  }
}