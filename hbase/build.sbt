organization := "com.adp.lab.unicorn"

name := "hbase"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.0" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.0" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.0.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.5.1" % "provided"
