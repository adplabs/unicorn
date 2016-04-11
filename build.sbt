name := "smile"

import com.typesafe.sbt.pgp.PgpKeys.{useGpg, publishSigned, publishLocalSigned}

lazy val commonSettings = Seq(
  organization := "com.adp.unicorn",
  organizationName := "ADP, LLC",
  organizationHomepage := Some(url("http://www.adp.com")),
  version := "2.0.0-SNAPSHOT",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-feature", "-language:_", "-unchecked", "-deprecation", "-encoding", "utf8"),
  scalacOptions in Test ++= Seq("-Yrangepos"),
  parallelExecution in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false ,
  publishMavenStyle := true,
  useGpg := true,
  pomIncludeRepository := { _ => false },
  pomExtra := (
    <url>https://github.com/haifengl/unicorn</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:haifengl/unicorn.git</url>
        <connection>scm:git:git@github.com:haifengl/unicorn.git</connection>
      </scm>
      <developers>
        <developer>
          <id>haifengl</id>
          <name>Haifeng Li</name>
          <url>http://haifengl.github.io/smile/</url>
        </developer>
      </developers>
    )
)

lazy val nonPubishSettings = commonSettings ++ Seq(
  publishArtifact := false,
  publishLocal := {},
  publish := {},
  publishSigned := {},
  publishLocalSigned := {}
)

lazy val root = project.in(file(".")).settings(nonPubishSettings: _*)
  .aggregate(util, oid, json, bigtable, hbase, cassandra, accumulo, unibase, narwhal, shell, graph, rhino)

lazy val util = project.in(file("util")).settings(commonSettings: _*)

lazy val oid = project.in(file("oid")).settings(commonSettings: _*).dependsOn(util)

lazy val json = project.in(file("json")).settings(commonSettings: _*).dependsOn(oid)

lazy val bigtable = project.in(file("bigtable")).settings(commonSettings: _*).dependsOn(util)

lazy val index = project.in(file("index")).settings(commonSettings: _*).dependsOn(bigtable, json)

lazy val unibase = project.in(file("unibase")).settings(commonSettings: _*).dependsOn(json, oid, bigtable, accumulo % "test")

lazy val narwhal = project.in(file("narwhal")).settings(commonSettings: _*).dependsOn(unibase, hbase, index)

lazy val sql = project.in(file("sql")).settings(commonSettings: _*).dependsOn(util, narwhal)

lazy val hbase = project.in(file("hbase")).settings(commonSettings: _*).dependsOn(bigtable, index)

lazy val accumulo = project.in(file("accumulo")).settings(commonSettings: _*).dependsOn(bigtable)

lazy val cassandra = project.in(file("cassandra")).settings(commonSettings: _*).dependsOn(bigtable, util)

lazy val graph = project.in(file("graph")).settings(commonSettings: _*).dependsOn(unibase)

//lazy val search = project.in(file("search")).settings(commonSettings: _*).dependsOn(unibase)

lazy val shell = project.in(file("shell")).settings(commonSettings: _*).dependsOn(unibase, graph, sql, hbase, cassandra, accumulo)

lazy val rhino = project.in(file("rhino")).enablePlugins(SbtTwirl).settings(commonSettings: _*).dependsOn(unibase, graph, hbase, cassandra, accumulo)

