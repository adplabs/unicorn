name := "adp-unicorn-console"

import sbtassembly.AssemblyPlugin.defaultShellScript

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

mainClass in assembly := Some("com.adp.unicorn.console.Console")

test in assembly := {}

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.7"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.2"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.2"
