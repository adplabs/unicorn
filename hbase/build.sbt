name := "unicorn-hbase"

libraryDependencies ++= {
  val hbaseV = "1.2.1"
  val hadoopV = "2.6.4"
  Seq(
    // Spark conflicts with other servlet-api jars
    "org.apache.hbase"    %  "hbase-common"     % hbaseV,
    "org.apache.hbase"    %  "hbase-client"     % hbaseV,
    "org.apache.hbase"    %  "hbase-server"     % hbaseV  exclude("org.mortbay.jetty", "servlet-api-2.5"),
    "org.apache.hadoop"   %  "hadoop-common"    % hadoopV exclude("org.eclipse.jetty", "servlet-api") exclude("javax.servlet", "servlet-api")
  )
}
