package com.adp.cdg.console

object Console extends App {
  
  override def main(args: Array[String]): Unit = {
    val interpreter = new InterpreterWrapper() {
      def prompt = "Unicorn> "
      def welcomeMsg = """
===============================================================================
                        . . . .
                        ,`,`,`,`,
  . . . .               `\`\`\`\;
  `\`\`\`\`,            ~|;!;!;\!
   ~\;\;\;\|\          (--,!!!~`!       .
  (--,\\\===~\         (--,|||~`!     ./
   (--,\\\===~\         `,-,~,=,:. _,//
    (--,\\\==~`\        ~-=~-.---|\;/J,       Welcome to the Unicorn Database
     (--,\\\((```==.    ~'`~/       a |          Column, Document and Graph
       (-,.\\('('(`\\.  ~'=~|     \_.  \
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

      autoImport("com.adp.cdg._")
      autoImport("com.adp.cdg.DocumentImplicits._")
      autoImport("com.adp.cdg.store._")
      autoImport("com.adp.cdg.store.accumulo._")
      autoImport("com.adp.cdg.store.hbase._")
      autoImport("com.adp.cdg.graph._")
      autoImport("com.adp.cdg.graph.document._")
      org.apache.log4j.LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
    }

    interpreter.startInterpreting
  }
}
