name := "unicorn-hbase"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.1"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test"

parallelExecution in Test := false