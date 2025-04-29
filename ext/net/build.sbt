import fauna.sbt.CoreDefs

lazy val net = (project in file("."))
  .enablePlugins(RamdiskPlugin)
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("codex"),
    LocalProject("exec"),
    LocalProject("lang"),
    LocalProject("logging") % "compile->compile;test->test",
    LocalProject("prop") % "test->test",
    LocalProject("stats"),
    LocalProject("trace")
  )

name := "net"
organization := "fauna"
version := "0.0.1"

Test / fork := true

Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dcom.sun.management.jmxremote.port=8988",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-Dcom.sun.management.jmxremote.authenticate=false"
)

libraryDependencies ++= CoreDefs.nettyDependencies

libraryDependencies ++= Seq(
  "org.bouncycastle" % "bcprov-jdk18on" % "1.79",
  "org.bouncycastle" % "bcpkix-jdk18on" % "1.79",
  "org.apache.commons" % "commons-math3" % "3.6.1")

// test deps
libraryDependencies += "org.apache.logging.log4j" % "log4j-core-test" % CoreDefs.log4jVersion % "test"
