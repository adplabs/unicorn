/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.http

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.adp.unicorn.DocumentImplicits._

class DocumentServlet extends HttpServlet {

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    response.setCharacterEncoding("UTF-8")
    
    val id = request.getParameter("id")
    if (id == null || id.isEmpty) {
      request.getRequestDispatcher("index.html").forward(request, response)
      return
    }
    
    val doc = id of Configuration.data

    val writer = response.getWriter
    writer.write(Configuration.skeletonTop)
    writer.write("""<pre><code class="javascript">""")
    writer.write(doc.toString)
    writer.write("</code></pre>")
    writer.write(Configuration.skeletonBottom)
  }
}