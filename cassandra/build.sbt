name := "unicorn-cassandra"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.2.1"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test"

parallelExecution in Test := false