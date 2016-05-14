name := "unicorn-narwhal"

libraryDependencies ++= {
  val sparkV = "1.6.1"
  Seq(
    "org.apache.spark"  %%  "spark-core"    % sparkV,
    "org.apache.spark"  %%  "spark-sql"     % sparkV,
    "org.apache.spark"  %%  "spark-graphx"  % sparkV
  )
}



