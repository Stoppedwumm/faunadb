package fauna.sbt

import sbt._
import sbt.Keys._
import scala.util.Try

object ResolverDefaultsPlugin extends AutoPlugin {
  override def trigger = allRequirements

  override val projectSettings = Seq(
    resolvers += Resolver.mavenLocal
  )
}

object TestDefaultsPlugin extends AutoPlugin {
  override def trigger = allRequirements

  // Restrict parallelism in test runs. Our tests (esp those that
  // exercise the network stack) can't handle it all at once.
  private lazy val testParallelism = {
    val available = java.lang.Runtime.getRuntime.availableProcessors
    val configured = Option(System.getenv("CORE_TEST_PARALLELISM"))
    configured.fold(available / 2) { _.toInt } max 1
  }

  override val projectSettings = Seq(
    Global / concurrentRestrictions := Seq(Tags.limitAll(testParallelism)),
    libraryDependencies ++= CoreDefs.scalatestDependencies,
    Test / testOptions ++= Seq(
      // CLI output for humans
      Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
      // HTML output for CI robots
      Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
    )
  )
}

object ScalaDefaultsPlugin extends AutoPlugin {
  override def trigger = allRequirements

  val isRelease = Try(System.getenv("FAUNADB_RELEASE").toBoolean)
    .getOrElse(false)

  // set release mode prop one way or another
  System.setProperty("fauna.release", isRelease.toString)
  val releasePropOption = s"-Dfauna.release=${isRelease}"

  /** Inlining substantially increases compilation time. Enable it
    * only during release builds.
    */
  val releaseOrNonSettings = if (isRelease) {
    // NOTE: Inlining IterableOps on 2.13.9 fails to compile
    // MapRouter, because of course library code should break user
    // code in a point release. :rolleyes:
    Seq(
      scalacOptions ++= Seq(
        "-opt:l:inline",
        "-opt-inline-from:**,!java.**,!javax.**,!jdk.**,!sun.**,!scala.collection.IterableOps")
    )
  } else {
    // enable semanticdb for language-server use
    Seq(
      semanticdbEnabled := true,
      semanticdbVersion := CoreDefs.semanticdbVersion
    )
  }

  override val projectSettings = releaseOrNonSettings ++ Seq(
    Keys.scalaVersion := CoreDefs.scalaVersion,
    scalacOptions ++= Seq(
      releasePropOption,
      "-release:11",
      "-Xfatal-warnings",

      // extra warnings
      "-unchecked",
      "-deprecation",
      "-feature",
      "-opt-warnings:at-inline-failed",

      // Introduced in 2.13.11, this warns when an implicit does not
      // have an explicit type annotation. This code base does this. A
      // lot. Silence it for now.
      "-Wconf:cat=other-implicit-type:s",

      // Lint
      "-Xlint:_",
      "-Xlint:-implicit-recursion", // FIXME: disabling this causes some errors I don't know how to fix
      "-Xlint:-pattern-shadow", // Added in 2.13.13, do we really want all those backticks?
      "-Xlint:-type-parameter-shadow", // FIXME: repo.ID is shadowed.
      "-Xlint:-valpattern",
      "-Ywarn-dead-code",
      "-Ywarn-macros:after", // see scala issue #10571
      "-Wunused",
      "-Ypatmat-exhaust-depth",
      "200",

      // others, default off
      // "-explaintypes", // better type error explanations, but verbose.
      // "-Xcheckinit", // add runtime checks for uninitialized field access.
      // "-Ywarn-numeric-widen",
      // "-Ywarn-value-discard",
      // "-Ymacro-debug-lite",
      // "-Ymacro-debug-verbose",
      "-Ymacro-annotations",

      // Just can't live without this.
      "-language:postfixOps"
    ),
    Compile / console / scalacOptions ++= Seq("-Ywarn-unused:-imports"),
    Test / console / scalacOptions ++= Seq("-Ywarn-unused:-imports")
  )
}
