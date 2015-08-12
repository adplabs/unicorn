lazy val commonSettings = Seq(
  organization := "com.adp.unicorn",
  organizationName := "ADP, LLC",
  organizationHomepage := Some(url("http://www.adp.com")),
  version := "1.0.0",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-feature", "-language:_", "-unchecked", "-deprecation", "-encoding", "utf8"),
  libraryDependencies += "org.specs2" %% "specs2-core" % "3.6.3" % "test",
  scalacOptions in Test ++= Seq("-Yrangepos")
)

lazy val root = project.in(file(".")).aggregate(util, json, bigtable, hbase, cassandra, accumulo, doc, console, graph, search, demo)

lazy val util = project.in(file("util")).settings(commonSettings: _*)

lazy val json = project.in(file("json")).settings(commonSettings: _*).dependsOn(util)

lazy val bigtable = project.in(file("bigtable")).settings(commonSettings: _*)

lazy val doc = project.in(file("doc")).settings(commonSettings: _*).dependsOn(json)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val graph = project.in(file("graph")).settings(commonSettings: _*).dependsOn(doc)

lazy val search = project.in(file("search")).settings(commonSettings: _*).dependsOn(doc)

lazy val console = project.in(file("console")).settings(commonSettings: _*).dependsOn(doc, graph, search, hbase, cassandra, accumulo)

lazy val demo = project.in(file("demo")).enablePlugins(SbtTwirl).settings(commonSettings: _*).dependsOn(doc, graph, search, cassandra)

