import fauna.sbt.CoreDefs

lazy val trace = (project in file("."))
  .dependsOn(
    LocalProject("codex"),
    LocalProject("lang"),
    LocalProject("logging"),
    LocalProject("stats"))

name := "trace"
organization := "fauna"
version := "0.0.1"

libraryDependencies ++= Seq(
  "org.parboiled" %% "parboiled" % "2.5.1",
  CoreDefs.log4jCoreDependency,
  "com.lmax" % "disruptor" % "3.4.4")
