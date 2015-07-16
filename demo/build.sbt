name := "demo"

enablePlugins(JavaServerAppPackaging)

maintainer := "Haifeng Li <Haifeng.Li@ADP.COM>"

packageName := "adp-unicorn-demo"

packageSummary := "ADP Unicorn Search Demo Web Server"

packageDescription := "ADP Unicorn Search Demo Web Server"

executableScriptName := "unicorn-demo"

mainClass in Compile := Some("com.adp.unicorn.demo.Boot")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
    "io.spray"            %%  "spray-json"    % "1.3.1",
    "io.spray"            %%  "spray-testkit" % sprayV  % "test",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test"
  )
}
