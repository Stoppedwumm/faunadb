import fauna.sbt.CoreDefs

lazy val repo = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("cassandra"),
    LocalProject("cluster"),
    LocalProject("codex"),
    LocalProject("exec"),
    LocalProject("flags") % "test->test;compile->compile",
    LocalProject("fql"),
    LocalProject("lang"),
    LocalProject("limits"),
    LocalProject("logging") % "compile->compile;test->test",
    LocalProject("net"),
    LocalProject("prop") % "test->test",
    LocalProject("scheduler"),
    LocalProject("snowflake"),
    LocalProject("stats"),
    LocalProject("storage"),
    LocalProject("storageAPI"),
    LocalProject("trace"),
    LocalProject("tx")
  )

name := "repo"
organization := "fauna.api"
version := "0.0.1"

Test / fork := true
Test / parallelExecution := false

Test / javaOptions ++= Seq(
  s"-javaagent:${baseDirectory.value / ".." / ".." / "service" / "lib" / "jamm.jar"}",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dcom.sun.management.jmxremote.port=8988",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-Dcom.sun.management.jmxremote.authenticate=false"
)

libraryDependencies ++= Seq(CoreDefs.reflectDependency)
