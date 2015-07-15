/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.console

import scala.sys.SystemProperties
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.Settings

/**
 * Unicorn console.
 * 
 * @author Haifeng Li (293050)
 */
object Console extends App {
  val settings = new Settings
  settings.usejavacp.value = true
  settings.deprecation.value = true

  // import unicorn packages
  new SystemProperties += ("scala.repl.autoruncode" -> "init.scala")
  new UnicornILoop().process(settings)

  class UnicornILoop extends ILoop  {
    override def prompt = "ADP Unicorn> "
    override def printWelcome = echo("""
                        . . . .
                        ,`,`,`,`,
  . . . .               `\`\`\`\;
  `\`\`\`\`,            ~|;!;!;\!
   ~\;\;\;\|\          (--,!!!~`!       .
  (--,\\\===~\         (--,|||~`!     ./
   (--,\\\===~\         `,-,~,=,:. _,//
    (--,\\\==~`\        ~-=~-.---|\;/J,       Welcome to the Unicorn Database
     (--,\\\((```==.    ~'`~/       a |          Column, Document and Graph
       (-,.\\('('(`\\.  ~'=~|     \_.  \              Full Text Search
          (,--(,(,(,'\\. ~'=|       \\_;>
            (,-( ,(,(,;\\ ~=/        \                  Haifeng Li
            (,-/ (.(.(,;\\,/          )             ADP Innovation Lab
             (,--/,;,;,;,\\         ./------.
               (==,-;-'`;'         /_,----`. \
       ,.--_,__.-'                    `--.  ` \
      (='~-_,--/        ,       ,!,___--. \  \_)
     (-/~(     |         \   ,_-         | ) /_|
     (~/((\    )\._,      |-'         _,/ /
      \\))))  /   ./~.    |           \_\;             
   ,__/////  /   /    )  /
    '===~'   |  |    (, <.
             / /       \. \
           _/ /          \_\
          /_!/            >_\
===============================================================================
    """)
  }
}
