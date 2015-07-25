lazy val commonSettings = Seq(
  organization := "com.adp.unicorn",
  organizationName := "ADP, LLC",
  organizationHomepage := Some(url("http://www.adp.com")),
  version := "1.0.0",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-feature", "-language:_", "-unchecked", "-deprecation", "-encoding", "utf8")
)

lazy val root = project.in(file(".")).aggregate(util, json, core, hbase, cassandra, accumulo, console, graph, search, demo)

lazy val util = project.in(file("util")).settings(commonSettings: _*)

lazy val json = project.in(file("json")).settings(commonSettings: _*).dependsOn(util)

lazy val core = project.in(file("core")).settings(commonSettings: _*).dependsOn(json)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(core)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(core)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(core)

lazy val graph = project.in(file("graph")).settings(commonSettings: _*).dependsOn(core)

lazy val search = project.in(file("search")).settings(commonSettings: _*).dependsOn(core)

lazy val console = project.in(file("console")).settings(commonSettings: _*).dependsOn(core, graph, search, hbase, cassandra, accumulo)

lazy val demo = project.in(file("demo")).enablePlugins(SbtTwirl).settings(commonSettings: _*).dependsOn(core, graph, search, cassandra)

