import fauna.sbt.CoreDefs

lazy val multicore = (project in file("."))
  .enablePlugins(RamdiskPlugin, RunCoreConfigExportPlugin)
  .dependsOn(
    LocalProject("api") % "test->test",
    LocalProject("prop") % "test")
  .settings(
    name := "multicore",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := false, // we communicate system properties in-process
    Test / parallelExecution := false
  )
