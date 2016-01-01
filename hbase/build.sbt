name := "unicorn-hbase"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.1"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"

parallelExecution in Test := false