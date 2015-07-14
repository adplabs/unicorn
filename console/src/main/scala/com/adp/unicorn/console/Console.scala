/******************************************************************************
 *                   Confidential Proprietary                                 *
 *          (c) Copyright ADP 2014, All Rights Reserved                       *
 ******************************************************************************/

package com.adp.unicorn.console

/**
 * Unicorn console.
 * 
 * @author Haifeng Li (293050)
 */
object Console extends App {

  val interpreter = new InterpreterWrapper() {
    def prompt = "Unicorn> "
    def welcomeMsg = """
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
    """
    def helpMsg = """Unicorn Console"""

    autoImport("com.adp.unicorn._")
    autoImport("com.adp.unicorn.JsonValueImplicits._")
    autoImport("com.adp.unicorn.store._")
    autoImport("com.adp.unicorn.store.accumulo._")
    autoImport("com.adp.unicorn.store.cassandra._")
    autoImport("com.adp.unicorn.store.hbase._")
    autoImport("com.adp.unicorn.graph._")
    autoImport("com.adp.unicorn.graph.document._")
    autoImport("com.adp.unicorn.text._")
    //org.apache.logging.log4j.LogManager.getRootLogger().setLevel(org.apache.logging.log4j.Level.ERROR);
  }

  interpreter.startInterpreting
}
