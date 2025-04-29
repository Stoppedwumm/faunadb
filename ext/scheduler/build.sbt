import fauna.sbt.CoreDefs

lazy val scheduler = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("stats"),
    LocalProject("trace"))
  .settings(
    name := "scheduler",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := true
  )
