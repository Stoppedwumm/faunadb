import fauna.sbt.CoreDefs

lazy val prop = (project in file("."))
  .dependsOn(LocalProject("lang"))
  .settings(
    libraryDependencies += CoreDefs.reflectDependency
  )

name := "prop"
organization := "fauna"
version := "0.0.1"
