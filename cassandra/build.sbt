organization := "com.adp.lab.unicorn"

name := "cassandra"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "2.1.8"
