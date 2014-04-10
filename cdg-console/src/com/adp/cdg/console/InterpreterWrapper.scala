package com.adp.cdg.console

import java.io._
import scala.tools.nsc.interpreter._
import scala.tools.nsc.{GenericRunnerSettings, Settings}
import scala.reflect._

/**
 * A trait to ease the embedding of the scala interpreter
 */
trait InterpreterWrapper {

  def helpMsg: String
  def welcomeMsg: String
  def prompt: String

  private var bindings : Map[String, (java.lang.Class[_], AnyRef)] = Map()
  private var packageImports : List[String] = List()
  private var files = List[String]()

  /**
   * Binds a given value into the interpreter when it starts with its most specific class
   */
  protected def bind[A <: AnyRef](name : String, value : A)(implicit m : Manifest[A]): Unit =
    bindings +=  (( name,  (m.runtimeClass, value)))
  /**
   * Binds a given value into the interpreter with a given interface/higher-level class.
   */
  protected def bindAs[A <: AnyRef, B <: A](name : String, interface : java.lang.Class[A], value : B): Unit =
    bindings += ((name,  (interface, value)))

  /**
   * adds an auto-import for the interpreter.
   */
  protected def autoImport(importString : String): Unit =
    packageImports = importString :: packageImports

  /**
   * Adds a file that will be interpreter at the start of the interpreter
   */
  protected def addScriptFile(fileName : String): Unit =
    files = fileName :: files


  /**
   * This class actually runs the interpreter loop.
   */
  class MyInterpreterLoop(out : PrintWriter) extends ILoop(None, out) {
     override val prompt = InterpreterWrapper.this.prompt

     override def loop() {
    	 if(isAsync) awaitInitialized
    	 bindSettings()
    	 // turn off debug output
    	 super.command("org.apache.log4j.LogManager.getRootLogger().setLevel(org.apache.log4j.Level.INFO)")
    	 super.loop()
     }
     
     /** Bind the settings so that evaluated code can modify them */
     def bindSettings() {
       intp beQuietDuring {
         for( (name, (clazz, value)) <- bindings) {
           intp.bind(name, clazz.getCanonicalName, value)
         }
         for( importString <- packageImports) {
        	 intp.interpret("import " + importString)
         }
       }
     }
     
     override def helpCommand(line: String): Result = {
       if (line == "") echo(helpMsg)
       super.helpCommand(line)
     }

     override def printWelcome: Unit = {
       out.println(welcomeMsg)
       out.flush
     }
  }

  def startInterpreting(): Unit = {
    val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)))
    val settings = new GenericRunnerSettings(out.println)
    files foreach settings.loadfiles.appendToValue
    settings.usejavacp.value = true
    val interpreter = new MyInterpreterLoop(out)
    interpreter process settings
    ()
  }
}