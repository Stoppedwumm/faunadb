import fauna.sbt.CoreDefs

lazy val propMacros = (project in file("macros"))
  .settings(
    name := "prop-macros",
    libraryDependencies += CoreDefs.compilerDependency
  )

lazy val propAPI = (project in file("."))
  .dependsOn(
    propMacros,
    LocalProject("codex"),
    LocalProject("flags"),
    LocalProject("limits"),
    LocalProject("net"),
    LocalProject("prop")
  )
  .settings(
    libraryDependencies += CoreDefs.reflectDependency
  )

name := "prop-api"
organization := "fauna"
version := "0.0.1"
