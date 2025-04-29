import fauna.sbt.CoreDefs

lazy val snowflake = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("lang"))
  .settings(
    name := "snowflake",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := true
  )
