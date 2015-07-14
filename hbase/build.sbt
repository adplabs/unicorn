organization := "com.adp.lab.unicorn"

name := "hbase"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided"
