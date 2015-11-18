name := "unicorn-hbase"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided"

parallelExecution in Test := false