package com.adp.cdg.console

object Console extends App {
  
  override def main(args: Array[String]): Unit = {
    val interpreter = new InterpreterWrapper() {
      def prompt = "CDG> "
      def welcomeMsg = """Welcome to the Column-Document-Graph Console!"""
      def helpMsg = """CDG HELP"""

      autoImport("com.adp.cdg._")
      autoImport("com.adp.cdg.DocumentImplicits._")
      autoImport("com.adp.cdg.store._")
      autoImport("com.adp.cdg.store.accumulo._")
      autoImport("com.adp.cdg.store.hbase._")
      org.apache.log4j.LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
    }

    interpreter.startInterpreting
  }
}