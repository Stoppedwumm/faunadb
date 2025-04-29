import fauna.sbt._

lazy val tools = (project in file("."))
  .enablePlugins(RamdiskPlugin, RunCoreConfigExportPlugin)
  .dependsOn(
    LocalProject("api") % "compile;test->test",
    LocalProject("storage") % "compile;test->test",
    LocalProject("config"),
    LocalProject("exec"))

name := "tools"
organization := "fauna.tools"
version := "0.0.1"

// Permits the tool to exit with sys.exit without slaying sbt, when run with `sbt tools/run`.
run / fork := true

run / javaOptions ++= Seq(
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

// These tests do not play nice at all.
Test / parallelExecution := false

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-text" % "1.13.0",
  "com.carrotsearch" % "hppc" % "0.9.1")
