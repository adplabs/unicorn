name := "unicorn-index"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

libraryDependencies += "com.github.haifengl" % "smile-nlp" % "1.0.4"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test"

parallelExecution in Test := false