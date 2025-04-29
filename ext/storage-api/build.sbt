import fauna.sbt.CoreDefs

lazy val storageAPI = (project in file("."))
  .enablePlugins(RunCoreConfigExportPlugin)
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("flags"),
    LocalProject("lang"),
    LocalProject("prop") % "test->test",
    LocalProject("propAPI") % "test->test",
    LocalProject("storage") % "compile->compile;test->test")
  .settings(
    name := "storage-api",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := false,
    Test / parallelExecution := false)
