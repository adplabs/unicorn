lazy val commonSettings = Seq(
  organization := "com.adp.unicorn",
  organizationName := "ADP, LLC",
  organizationHomepage := Some(url("http://www.adp.com")),
  version := "1.0.0",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
)

lazy val root = project.in(file(".")).aggregate(core, hbase, accumulo, cassandra, console, graph, search, demo)

lazy val core = project.in(file("core")).settings(commonSettings: _*)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(core)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(core)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(core)

lazy val graph = project.in(file("graph")).settings(commonSettings: _*).dependsOn(core)

lazy val search = project.in(file("search")).settings(commonSettings: _*).dependsOn(core)

lazy val console = project.in(file("console")).settings(commonSettings: _*).dependsOn(core, graph, search, hbase, accumulo, cassandra)

lazy val demo = project.in(file("demo")).settings(commonSettings: _*).dependsOn(core, graph, search, cassandra)

