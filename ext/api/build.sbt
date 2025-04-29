import fauna.sbt.CoreDefs
import java.io.{ BufferedInputStream, FileInputStream }
import java.util.Properties
import sys.process._

lazy val releaseProps = {
  val props = new Properties()
  val in = new FileInputStream("service/release.properties")
  props.load(in)
  in.close()
  props
}

val api = (project in file("."))
  .enablePlugins(RunCoreConfigExportPlugin)
  .dependsOn(
    LocalProject("exec"),
    LocalProject("flags") % "compile;test->test",
    LocalProject("lang"),
    LocalProject("logging"),
    LocalProject("net"),
    LocalProject("cluster"),
    LocalProject("codex"),
    LocalProject("config"),
    LocalProject("trace"),
    LocalProject("scheduler"),
    LocalProject("stats"),
    LocalProject("repair"),
    LocalProject("repo") % "compile;test->test",
    LocalProject("model"),
    LocalProject("propAPI") % "test",
    LocalProject("prop") % "test->test"
  )
  .settings(
    name := "api",
    organization := "fauna"
  )

libraryDependencies ++= Seq(
  CoreDefs.log4jApiDependency,
  CoreDefs.commonsCliDependency,
  CoreDefs.compilerDependency)
