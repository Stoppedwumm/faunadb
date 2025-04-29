import fauna.sbt.CoreDefs

lazy val flags = (project in file("."))
  .dependsOn(
    LocalProject("codex"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("logging"),
    LocalProject("net"),
    LocalProject("prop") % "test->test",
    LocalProject("stats"),
    LocalProject("trace"))

name := "flags"
organization := "fauna"
version := "0.0.1"

libraryDependencies += CoreDefs.log4jCoreDependency
