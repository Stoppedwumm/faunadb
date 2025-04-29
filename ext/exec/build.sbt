import fauna.sbt.CoreDefs

lazy val exec = (project in file("."))
  .dependsOn(
    LocalProject("lang"),
    LocalProject("logging"),
    LocalProject("trace"))

name := "exec"
organization := "fauna"
version := "0.0.1"

libraryDependencies ++= Seq(
  CoreDefs.log4jCoreDependency,
  "io.netty" % "netty-common" % CoreDefs.nettyVersion)
