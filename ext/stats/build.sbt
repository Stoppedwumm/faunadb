import fauna.sbt.CoreDefs

lazy val stats = (project in file("."))
  .dependsOn(
    LocalProject("cassandra"),
    LocalProject("codex"),
    LocalProject("lang"),
    LocalProject("logging"),
    LocalProject("prop") % "test->test")

name := "stats"
organization := "fauna"
version := "0.0.1"

libraryDependencies += CoreDefs.yammerMetricsDependency
