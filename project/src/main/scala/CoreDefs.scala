package fauna.sbt

import sbt._

object CoreDefs {
  val scalaVersion = "2.13.14"
  // This should be set to a version compatible with the above scala version.
  // See: https://mvnrepository.com/artifact/org.scalameta/semanticdb-scalac
  val semanticdbVersion = "4.9.7"

  val compilerDependency = "org.scala-lang" % "scala-compiler" % scalaVersion
  val reflectDependency = "org.scala-lang" % "scala-reflect" % scalaVersion

  val log4jVersion = "2.24.3"
  val log4jCoreDependency = "org.apache.logging.log4j" % "log4j-core" % log4jVersion
  val log4jApiDependency = "org.apache.logging.log4j" % "log4j-api" % log4jVersion
  val slf4jLog4jDependency =
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion

  val commonsCliDependency = "commons-cli" % "commons-cli" % "1.9.0"
  val snakeyamlDependency = "org.yaml" % "snakeyaml" % "2.3"

  val caffeineDependency = "com.github.ben-manes.caffeine" % "caffeine" % "3.2.0"

  val jbcryptDependency = "org.mindrot" % "jbcrypt" % "0.4"

  val yammerMetricsDependency =
    "com.yammer.metrics" % "metrics-core" % "2.2.0" exclude (
      "org.slf4j",
      "slf4j-api")

  val nettyVersion = "4.1.116.Final"
  val nettyTCNativeVersion = "2.0.69.Final"

  val nettyDependencies = Seq(
    "io.netty" % "netty-buffer" % nettyVersion,
    "io.netty" % "netty-codec" % nettyVersion,
    "io.netty" % "netty-codec-http" % nettyVersion,
    "io.netty" % "netty-codec-http2" % nettyVersion,
    "io.netty" % "netty-common" % nettyVersion,
    "io.netty" % "netty-handler" % nettyVersion,
    "io.netty" % "netty-transport" % nettyVersion,

    // https://github.com/netty/netty/wiki/Forked-Tomcat-Native
    "io.netty" % "netty-tcnative-boringssl-static" % nettyTCNativeVersion classifier "linux-x86_64",
    "io.netty" % "netty-tcnative-boringssl-static" % nettyTCNativeVersion classifier "linux-aarch_64",

    // https://github.com/netty/netty/wiki/Native-transports
    "io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64",
    "io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-aarch_64",
    "io.netty.incubator" % "netty-incubator-transport-native-io_uring" % "0.0.26.Final" classifier "linux-x86_64",
    "io.netty.incubator" % "netty-incubator-transport-native-io_uring" % "0.0.26.Final" classifier "linux-aarch_64"
  )

  val scalatestVersion = "3.2.19"
  val scalatestDependencies = Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
    // scalatest supports a very narrow set of versions of flexmark;
    // update this with caution.
    // See scalatest #1736.
    "com.vladsch.flexmark" % "flexmark-profile-pegdown" % "0.64.8" % "test"
  )
}
