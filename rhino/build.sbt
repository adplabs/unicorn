name := "unicorn-rhino"

enablePlugins(JavaServerAppPackaging)

maintainer := "Haifeng Li <Haifeng.Li@ADP.COM>"

packageName := "adp-unicorn-rhino"

packageSummary := "ADP Unicorn Rhino REST API"

packageDescription := "ADP Unicorn Rhino REST API"

executableScriptName := "rhino"

mainClass in Compile := Some("unicorn.rhino.Boot")

libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
    "io.spray"            %%  "spray-testkit" % sprayV  % "test",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-slf4j"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test"
  )
}

libraryDependencies += "org.specs2" %% "specs2-core" % "2.3.11" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13"