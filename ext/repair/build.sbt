import fauna.sbt.CoreDefs

val repair = (project in file("."))
  .dependsOn(
    LocalProject("repo") % "compile;test->test",
    LocalProject("model"),
    LocalProject("propAPI") % "test",
    LocalProject("prop") % "test->test"
  )
  .settings(
    name := "repair",
    organization := "fauna",
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED"
    )
  )
