import fauna.sbt.CoreDefs

lazy val fuzz = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("logging"),
    LocalProject("net"),
    LocalProject("tx"))

name := "fuzz"
organization := "fauna"
version := "0.0.1"
