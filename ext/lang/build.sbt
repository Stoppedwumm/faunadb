import fauna.sbt.CoreDefs

name := "lang"
organization := "fauna"
version := "0.0.1"

libraryDependencies ++= Seq(
  CoreDefs.compilerDependency,
  "io.netty" % "netty-buffer" % CoreDefs.nettyVersion,
  "io.netty" % "netty-handler" % CoreDefs.nettyVersion,
  "io.netty" % "netty-transport" % CoreDefs.nettyVersion,
  CoreDefs.caffeineDependency,
  CoreDefs.log4jApiDependency)
