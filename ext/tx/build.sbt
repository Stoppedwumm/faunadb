import fauna.sbt._

lazy val tx = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("codex"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("logging") % "compile->compile;test->test",
    LocalProject("net"),
    LocalProject("prop") % "test->test",
    LocalProject("scheduler"),
    LocalProject("stats"),
    LocalProject("trace"))

name := "tx"
organization := "fauna"
version := "0.0.1"

Test / fork := true

Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dcom.sun.management.jmxremote.port=8988",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-Dcom.sun.management.jmxremote.authenticate=false")
