lazy val commonSettings = Seq(
  organization := "com.adp.unicorn",
  organizationName := "ADP, LLC",
  organizationHomepage := Some(url("http://www.adp.com")),
  version := "2.0.0-SNAPSHOT",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-feature", "-language:_", "-unchecked", "-deprecation", "-encoding", "utf8"),
  libraryDependencies += "org.specs2" %% "specs2-core" % "3.6.3" % "test",
  scalacOptions in Test ++= Seq("-Yrangepos"),
  parallelExecution in Test := false
)

lazy val root = project.in(file(".")).aggregate(util, oid, json, bigtable, hbase, cassandra, accumulo, unibase, shell, graph, rhino)

lazy val util = project.in(file("util")).settings(commonSettings: _*)

lazy val oid = project.in(file("oid")).settings(commonSettings: _*).dependsOn(util)

lazy val json = project.in(file("json")).settings(commonSettings: _*).dependsOn(oid)

lazy val bigtable = project.in(file("bigtable")).settings(commonSettings: _*).dependsOn(util)

lazy val index = project.in(file("index")).settings(commonSettings: _*).dependsOn(bigtable, json)

lazy val sql = project.in(file("sql")).settings(commonSettings: _*).dependsOn(util)

lazy val unibase = project.in(file("unibase")).settings(commonSettings: _*).dependsOn(json, oid, bigtable, index, hbase, cassandra, accumulo)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(bigtable, index)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(bigtable, util)

lazy val graph = project.in(file("graph")).settings(commonSettings: _*).dependsOn(unibase)

//lazy val search = project.in(file("search")).settings(commonSettings: _*).dependsOn(unibase)

lazy val shell = project.in(file("shell")).settings(commonSettings: _*).dependsOn(unibase, graph, hbase, cassandra, accumulo)

lazy val rhino = project.in(file("rhino")).enablePlugins(SbtTwirl).settings(commonSettings: _*).dependsOn(unibase, graph, hbase)

