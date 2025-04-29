import fauna.sbt.CoreDefs
import java.io.FileInputStream
import java.util.Properties
import scala.util.Try

lazy val releaseProps = {
  val props = new Properties()
  val in = new FileInputStream("service/release.properties")
  props.load(in)
  in.close()
  props
}

lazy val service = (project in file("."))
  .enablePlugins(RunCoreConfigExportPlugin)
  .dependsOn(
    LocalProject("api") % "test->test;compile->compile",
    LocalProject("prop") % "test",
    LocalProject("tools"))

// release settings

name := "service"
organization := "fauna.com"
version := releaseProps.getProperty("version")

// assembly settings

assembly / assemblyJarName := "faunadb.jar"
assembly / test := None
assembly / mainClass := Some("fauna.core.Service")

assembly := {
  val build = assembly.value
  if (!Try(System.getenv("FAUNADB_RELEASE").toBoolean).getOrElse(false)) {
    println(
      s"""|
          |============================ ATTENTION =============================
          |
          |Hold it right there cowboy!
          |
          |This build was NOT compiled in release mode. DO NOT USE this build
          |in production. To compile a build for production use run:
          |
          |  FAUNADB_RELEASE=true sbt service/assembly
          |
          |Search for `FAUNADB_RELEASE` and `fauna.release` to understand the
          |impact of using non-release builds.
          |
          |====================================================================
          |""".stripMargin
    )
  }
  build
}

// console helpers

initialCommands := """
  |import language._
  |import fauna.api._
  |import fauna.model._
  |import fauna.repo._
  |import fauna.lang._
  |import fauna.lang.syntax._
  |import fauna.codex.json._
  |import fauna.codex.cbor.CBOR
  |
  |def time[T](f: => T) = {
  |  val start = System.currentTimeMillis
  |  try f finally { println("Elapsed: "+ (System.currentTimeMillis - start)) }
  |}
  |
  |class Inspector(o: AnyRef) {
  |  def M = {
  |    println("Methods for "+o+":")
  |    o.getClass.getMethods foreach { m => println("  "+m.getName) }
  |    o
  |  }
  |}
  |
  |implicit def anyref2Ins(o: AnyRef) = new Inspector(o)
  |""".stripMargin

coreClasspath := (service / Compile / fullClasspath).value
Test / parallelExecution := false

assembly / assemblyMergeStrategy := {
  // Necessary for bcpkix-jdk18on and friends who all have this path.
  case "META-INF/versions/9/OSGI-INF/MANIFEST.MF" => MergeStrategy.discard
  case x =>
    val strat = (assembly / assemblyMergeStrategy).value
    strat(x)
}
