name := "unicorn-rhino"

mainClass in Compile := Some("unicorn.rhino.Boot")

enablePlugins(JavaServerAppPackaging)

maintainer := "Haifeng Li <Haifeng.Li@ADP.COM>"

packageName := "unicorn-rhino"

packageSummary := "Unicorn REST API"

packageDescription := "Unicorn REST API"

executableScriptName := "rhino"

mappings in Universal += {
  val conf = (resourceDirectory in Compile).value / "application.conf"
  conf -> "conf/rhino.conf"
}

mappings in Universal += {
  val conf = (resourceDirectory in Compile).value / "log4j.properties"
  conf -> "conf/log4j.properties"
}

mappings in Universal += {
  val conf = (resourceDirectory in Compile).value / "logback.xml"
  conf -> "conf/logback.xml"
}

bashScriptConfigLocation := Some("${app_home}/../conf/rhino.ini")

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/rhino.conf""""

bashScriptExtraDefines += """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml""""

libraryDependencies ++= {
  val akkaV = "2.4.4"
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

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"