import fauna.sbt.CoreDefs

lazy val storageMacros = (project in file("macros"))
  .settings(libraryDependencies += CoreDefs.compilerDependency)
  .dependsOn(LocalProject("codex"))

lazy val storage = (project in file("."))
  .enablePlugins(RamdiskPlugin, RunCoreConfigExportPlugin)
  .dependsOn(
    storageMacros,
    LocalProject("cassandra"),
    LocalProject("codex"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("logging"),
    LocalProject("net"),
    LocalProject("prop") % "test->test",
    LocalProject("propAPI") % "test->test",
    LocalProject("scheduler"),
    LocalProject("snowflake") % "test->test",
    LocalProject("stats"),
    LocalProject("tx"))
  .settings(
    name := "storage",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := false,
    Test / parallelExecution := false)

libraryDependencies ++= Seq(
  CoreDefs.snakeyamlDependency,
  "com.carrotsearch" % "hppc" % "0.9.1")
