enablePlugins(ScalaJSPlugin)

ThisBuild / scalaVersion := "2.13.10"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.fauna"
ThisBuild / organizationName := "fauna"

// FIXME: Our CI setup does not yet support scala-js projects.
lazy val enableJS = System.getenv("ENABLE_JS") ne null

lazy val agg: Seq[ProjectReference] =
  if (enableJS) Seq(fql.jvm, fql.js) else Seq(fql.jvm)

lazy val fqlMacros = crossProject(JVMPlatform, JSPlatform)
  .in(file("macros"))
  .settings(
    name := "fql-macros",
    libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value
  )

lazy val fqlRoot = (project in file("."))
  .aggregate(agg: _*)
  .settings(
    test := { if (enableJS) (Test / test).value else (fql.jvm / Test / test).value },
    publish := {},
    publishLocal := {}
  )

lazy val fql = crossProject(JVMPlatform, JSPlatform)
  .withoutSuffixFor(JVMPlatform)
  .in(file("."))
  .dependsOn(fqlMacros)
  .settings(
    name := "fql",
    Test / fork := true,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-language:higherKinds",
      "-Wvalue-discard",
      "-Wunused",
      "-Xlint:_",
      "-Xlint:-valpattern",
      "-Ywarn-macros:after", // see scala issue #10571
      "-Ypatmat-exhaust-depth",
      "off"
    ),
    libraryDependencies += "com.lihaoyi" %%% "fastparse" % "2.3.3",
    // NB. If updating scalatest version, also update it in `CoreDefs`.
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
  )
  .jsSettings(
    scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.ESModule) }
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
