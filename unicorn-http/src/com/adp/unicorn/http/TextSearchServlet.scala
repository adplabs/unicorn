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
import com.adp.unicorn.text.TextSearch


class TextSearchServlet extends HttpServlet {
  val index = TextSearch(Configuration.data)

  class TransformArticle(title: String, link: String, summary: String) extends RewriteRule {
    override def transform(n: Node): Seq[Node] = n match {
      case child @ <a/> => child match {
        case elem: Elem if elem \ "@class" contains Text("title") =>
          elem copy (child = Text(title),
              attributes = elem.attributes.append(Attribute(null, "href", link, Null)))
        case other => other
      }
      case child @ <dd/> => child match {
        case elem: Elem if elem \ "@class" contains Text("summary") =>
          elem copy (child = Text(summary))
        case other => other
      }
      case other => other
    }
  }
  
  val template =
    <dl>
    <dt><a class="title"></a></dt>
    <dd class="summary"></dd>
    </dl>

  override def service(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    response.setCharacterEncoding("UTF-8")
    
    val q = request.getParameter("q")
    if (q == null || q.isEmpty) {
      request.getRequestDispatcher("index.html").forward(request, response)
      return
    }
    
    val terms = q.split(" ")
    val matches = index.search(terms: _*)
  
    val body = Group(
      matches.flatMap { case ((doc, field), score) =>
        doc.select("title")
        val title: String = doc("title")
        new RuleTransformer(new TransformArticle(title, "/doc/?id="+doc.id, score.toString)) transform template
      }
    )

    val writer = response.getWriter
    writer.write(Configuration.skeletonTop)
    writer.write(body.toString)
    writer.write(Configuration.skeletonBottom)
  }
}