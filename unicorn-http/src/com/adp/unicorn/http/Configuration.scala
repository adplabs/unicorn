/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.http

import com.adp.unicorn.store.DataSet
import com.adp.unicorn.store.cassandra.CassandraServer

object Configuration {
  val numTexts: Long = 4004478
  
  def data(): DataSet = {
    val server = CassandraServer("127.0.0.1", 9160)
    server.dataset("dbpedia")
  }
  
  def skeletonTop(query: String = "", d3Node: String = ""): String = {
    val onload = if (d3Node.isEmpty) "" else """onload="loadD3();""""
    s"""<html>
      <head>
        <title>Unicorn Full Text Search</title>
        <link rel="stylesheet" type="text/css" href="/css/style.css" />
        <script>
          // Sniff MSIE version
          // http://james.padolsey.com/javascript/detect-ie-in-js-using-conditional-comments/
          var ie = ( function() {
            var undef,
            v = 3,
            div = document.createElement('div'),
            all = div.getElementsByTagName('i');
            while (
              div.innerHTML='<!--[if gt IE ' + (++v) + ']><i></i><![endif]-->',all[0]
            );
            return v > 4 ? v : undef;
          }() );

          function loadD3() {
            if ( ie && ie < 9 ) {
              console.warn("D3 is not supported")
            } else {
              // Load D3.js, and once loaded do our stuff
              var head = document.getElementsByTagName('head')[0];
              var script = document.createElement('script');
              script.type = 'text/javascript';
	          script.src = "http://d3js.org/d3.v3.min.js";
              script.addEventListener('load', plot_graph, false);
              script.onload = "plot_graph();";
              script.node = '$d3Node'
	          head.appendChild(script);
            }
          }
        </script>
      </head>
      <body $onload>
        <div id="content" style="margin-top:10px;">
        <form method="get" action="/search">                                        
        <input type="text" id="search" name="q" value="$query" size="60"></input>  
        <input type="submit" value="Search" style="width:100px"></input> 
        </form>                                                                     
        <hr></hr>
    """
  }

  val skeletonBottom = 
    """
        </div>
      </body>
    </html>"""  
}