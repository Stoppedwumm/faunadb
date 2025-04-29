resolvers += Classpaths.typesafeReleases

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.1")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.0")

// for ext/fql
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.17.0")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")

// dependencies for the ReleasePlugin
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.18.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2"

scalacOptions ++= Seq(
  "-language:postfixOps",
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfuture")

addDependencyTreePlugin
