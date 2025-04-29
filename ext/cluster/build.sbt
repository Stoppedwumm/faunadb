import fauna.sbt.CoreDefs

lazy val cluster = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("logging") % "compile->compile;test->test",
    LocalProject("net"),
    LocalProject("storage"),
    LocalProject("tx"),
    LocalProject("trace")
  )
  .settings(
    name := "cluster",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := true
  )
