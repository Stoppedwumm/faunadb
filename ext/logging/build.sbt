import fauna.sbt.CoreDefs

lazy val logging = (project in file("."))
  .dependsOn(
    LocalProject("codex"),
    LocalProject("lang"))

name := "logging"
organization := "fauna"
version := "0.0.1"

libraryDependencies ++= Seq(CoreDefs.log4jCoreDependency)
