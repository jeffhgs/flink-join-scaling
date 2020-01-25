scalaVersion in ThisBuild := "2.11.8"
lazy val flinkVersion = "1.9.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-clients" % flinkVersion % "provided"
  ,"org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"

  // testing
  ,"org.scalatest" %% "scalatest" % "2.2.4" % "test"

)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
