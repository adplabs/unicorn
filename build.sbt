organization := "com.adp.lab.unicorn"

name := "unicorn"

version := "1.0"

scalaVersion := "2.11.2"

lazy val root =
        project.in(file("."))
    .aggregate(core, hbase, accumulo, cassandra, console, graph, text, http)

lazy val core = project.in(file("unicorn-core"))

lazy val hbase = project.in(file("unicorn-hbase")).dependsOn(core)

lazy val accumulo = project.in(file("unicorn-accumulo")).dependsOn(core)

lazy val cassandra = project.in(file("unicorn-cassandra")).dependsOn(core)

lazy val graph = project.in(file("unicorn-graph")).dependsOn(core)

lazy val text = project.in(file("unicorn-text")).dependsOn(core)

lazy val console = project.in(file("unicorn-console")).dependsOn(core, graph)

lazy val http = project.in(file("unicorn-http")).dependsOn(core, graph, text, cassandra)

