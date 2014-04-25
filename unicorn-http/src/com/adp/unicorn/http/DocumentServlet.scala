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
    
    val q = request.getParameter("q")
    if (q == null || q.isEmpty) {
      request.getRequestDispatcher("index.html").forward(request, response)
      return
    }
    
    val doc = q of Configuration.data

    val writer = response.getWriter
    writer.write(Configuration.skeletonTop)
    writer.write(doc.toString)
    writer.write(Configuration.skeletonBottom)
  }
}