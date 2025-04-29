import fauna.sbt._

val runQA = InputKey[Unit]("run-qa", "Runs the given test")

val qa = (project in file("."))
  .enablePlugins(RunCoreConfigExportPlugin)
  .dependsOn(
    LocalProject("api") % "test->test",
    LocalProject("codex"),
    LocalProject("config"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("net"),
    LocalProject("propAPI"),
    LocalProject("snowflake")
  )

name := "qa"
organization := "fauna"
version := "0.0.1"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.3",
  "org.hdrhistogram" % "HdrHistogram" % "2.2.2",
  CoreDefs.slf4jLog4jDependency
)

Compile / run / mainClass := Some("fauna.qa.main.Supervisor")
runQA := withCore(Compile / run).evaluated

assembly / assemblyJarName := "faunadb-qa.jar"
assembly / test := None

assembly / assemblyMergeStrategy := {
  case x if x startsWith "reference.conf"    => MergeStrategy.concat
  case x if x startsWith "log4j2.properties" => MergeStrategy.last
  case x if x startsWith "application.conf"  => MergeStrategy.discard
  case x if x endsWith "module-info.class"   => MergeStrategy.discard
  // Necessary for bcpkix-jdk18on and friends who all have this path.
  case "META-INF/versions/9/OSGI-INF/MANIFEST.MF" => MergeStrategy.discard
  case x =>
    val strat = (assembly / assemblyMergeStrategy).value
    strat(x)
}

Test / parallelExecution := false
