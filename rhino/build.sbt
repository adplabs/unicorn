name := "unicorn-rhino"

mainClass in Compile := Some("unicorn.rhino.Boot")

enablePlugins(JavaServerAppPackaging)

maintainer := "Haifeng Li <Haifeng.Li@ADP.COM>"

packageName := "unicorn-rhino"

packageSummary := "Unicorn REST API"

packageDescription := "Unicorn REST API"

executableScriptName := "rhino"

bashScriptConfigLocation := Some("${app_home}/../conf/rhino.ini")

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/rhino.conf""""

bashScriptExtraDefines += """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml""""

// G1 garbage collector
bashScriptExtraDefines += """addJava "-XX:+UseG1GC""""

// Optimize string duplication, which happens a lot when parsing a data file
bashScriptExtraDefines += """addJava "-XX:+UseStringDeduplication""""

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

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"