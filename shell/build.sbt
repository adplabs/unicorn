name := "unicorn-shell"

mainClass in Compile := Some("unicorn.shell.Main")

// SBT native packager
enablePlugins(JavaAppPackaging)

maintainer := "Haifeng Li <Haifeng.Li@ADP.COM>"

packageName := "unicorn"

packageSummary := "Unicorn"

packageDescription := "Unicorn"

executableScriptName := "unicorn"

bashScriptConfigLocation := Some("${app_home}/../conf/unicorn.ini")

bashScriptExtraDefines += """addJava "-Dsmile.home=${app_home}""""

bashScriptExtraDefines += """addJava "-Dscala.repl.autoruncode=${app_home}/init.scala""""

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/unicorn.conf""""

// SBT BuildInfo
enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "unicorn.shell"

buildInfoOptions += BuildInfoOption.BuildTime

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.7"
