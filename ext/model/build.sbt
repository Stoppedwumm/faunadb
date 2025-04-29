import fauna.sbt.CoreDefs

lazy val modelMacros = (project in file("macros"))
  .dependsOn(LocalProject("atoms"))
  .settings(
    name := "model-macros",
    libraryDependencies += CoreDefs.compilerDependency)

lazy val model = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    modelMacros,
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("limits"),
    LocalProject("logging"),
    LocalProject("codex"),
    LocalProject("prop") % "test->test",
    LocalProject("propAPI") % "test->test",
    LocalProject("fql"),
    LocalProject("repo") % "test->test;compile->compile"
  )
  .settings(
    name := "model",
    organization := "fauna",
    version := "0.0.1",
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      s"-javaagent:${baseDirectory.value / ".." / ".." / "service" / "lib" / "jamm.jar"}",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    libraryDependencies ++= Seq(
      "org.parboiled" %% "parboiled" % "2.5.1",
      "org.scalacheck" %% "scalacheck" % "1.18.1" % "test",
      CoreDefs.jbcryptDependency,
      "org.apache.lucene" % "lucene-analyzers-common" % "8.11.4"
    )
  )
