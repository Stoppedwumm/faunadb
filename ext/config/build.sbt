import fauna.sbt.CoreDefs

lazy val configMacros = (project in file("macros"))
  .settings(
    name := "config-macros",
    libraryDependencies += CoreDefs.reflectDependency,
    resolvers ++= Resolver.sonatypeOssRepos("releases"))

val config = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    configMacros,
    LocalProject("lang"),
    LocalProject("net"),
    LocalProject("storage"),
    LocalProject("trace")
  )
  .settings(
    name := "config",
    organization := "fauna",
    version := "0.0.1",
    resolvers ++= Resolver.sonatypeOssRepos("releases"))

libraryDependencies += CoreDefs.compilerDependency
