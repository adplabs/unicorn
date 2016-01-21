name := "unicorn-accumulo"

libraryDependencies += "org.apache.accumulo" % "accumulo-core" % "1.7.0"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test"

parallelExecution in Test := false