organization := "com.adp.lab.unicorn"

name := "unicorn"

version := "1.0"

scalaVersion := "2.11.2"

lazy val root =
        project.in(file("."))
    .aggregate(core, hbase, accumulo, cassandra, console, graph, text, demo)

lazy val core = project.in(file("core"))

lazy val hbase = project.in(file("hbase")).dependsOn(core)

lazy val accumulo = project.in(file("accumulo")).dependsOn(core)

lazy val cassandra = project.in(file("cassandra")).dependsOn(core)

lazy val graph = project.in(file("graph")).dependsOn(core)

lazy val text = project.in(file("text")).dependsOn(core)

lazy val console = project.in(file("console")).dependsOn(core, graph)

lazy val demo = project.in(file("demo")).dependsOn(core, graph, text, cassandra)

