/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.http

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import scala.xml._
import scala.xml.transform.RewriteRule
import scala.xml.transform.RuleTransformer
import com.adp.unicorn._
import com.adp.unicorn.text.TextSearch


class TextSearchServlet extends HttpServlet {
  val index = TextSearch(Configuration.data, Configuration.numTexts)

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    response.setCharacterEncoding("UTF-8")
    
    val q = request.getParameter("q")
    if (q == null || q.isEmpty) {
      request.getRequestDispatcher("index.html").forward(request, response)
      return
    }

    val body =
      <dl>
      {
        for (hit <- index.search(q.split(" "): _*)) yield
        {
          val doc = hit._1._1
          doc.select("title")
          val title: String = doc("title") match {
            case JsonUndefinedValue => doc.id
            case _ => doc("title")
          }
          <dt><a href={"/doc/?id="+doc.id}>{title}</a></dt>
          <dd>{hit._2}</dd>
        }
      }
      </dl>

    val writer = response.getWriter
    writer.write(Configuration.skeletonTop(q))
    writer.write(body.toString)
    writer.write(Configuration.skeletonBottom)
  }
}