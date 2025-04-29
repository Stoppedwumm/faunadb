import fauna.sbt.CoreDefs

ThisBuild / useCoursier := false

lazy val cassandra = project in file(".")

name := "cassandra"
organization := "fauna.api"
version := "0.0.1"

libraryDependencies ++= Seq(
  // <dependency groupId="net.jpountz.lz4" artifactId="lz4"/>
  "net.jpountz.lz4" % "lz4" % "1.3",

  // <dependency groupId="com.google.guava" artifactId="guava"/>
  "com.google.guava" % "failureaccess" % "1.0",
  "com.google.guava" % "guava" % "32.1.2-jre",
  "com.google.guava" % "guava-testlib" % "32.1.2-jre" % "test",

  // <dependency groupId="commons-cli" artifactId="commons-cli"/>
  CoreDefs.commonsCliDependency,

  // <dependency groupId="com.googlecode.concurrentlinkedhashmap"
  // artifactId="concurrentlinkedhashmap-lru"/>
  "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.4.2",

  // <dependency groupId="org.antlr" artifactId="antlr" version="3.5.2">
  //   <exclusion groupId="org.antlr" artifactId="stringtemplate"/>
  // </dependency>
  "org.antlr" % "antlr" % "3.5.3" exclude ("org.antlr", "stringtemplate"),

  // <dependency groupId="org.antlr" artifactId="antlr-runtime" version="3.5.2">
  //   <exclusion groupId="org.antlr" artifactId="stringtemplate"/>
  // </dependency>
  "org.antlr" % "antlr-runtime" % "3.5.3" exclude ("org.antlr", "stringtemplate"),

  // <dependency groupId="org.slf4j" artifactId="slf4j-api"/>
  CoreDefs.slf4jLog4jDependency,

  // <dependency groupId="org.codehaus.jackson" artifactId="jackson-mapper-asl"/>
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.11",
  "com.github.cliftonlabs" % "json-simple" % "4.0.1",

  // <dependency groupId="com.github.stephenc.high-scale-lib"
  // artifactId="high-scale-lib"/>
  "com.github.stephenc.high-scale-lib" % "high-scale-lib" % "1.1.4",

  // <dependency groupId="org.yaml" artifactId="snakeyaml"/>
  CoreDefs.snakeyamlDependency,

  // <dependency groupId="org.mindrot" artifactId="jbcrypt"/>
  CoreDefs.jbcryptDependency,

  // <dependency groupId="com.yammer.metrics" artifactId="metrics-core"/>
  CoreDefs.yammerMetricsDependency,

  // <dependency groupId="com.addthis.metrics" artifactId="reporter-config"/>
  "com.addthis.metrics" % "reporter-config" % "2.3.1" excludeAll (
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("org.yaml", "snakeyaml")),

  // <dependency groupId="net.sf.supercsv" artifactId="super-csv" version="2.1.0" />
  "net.sf.supercsv" % "super-csv" % "2.4.0",

  // <dependency groupId="log4j" artifactId="log4j"/>
  "org.apache.logging.log4j" % "log4j-core" % "1.2.17",

  // <!-- cassandra has a hard dependency on log4j, so force slf4j's log4j provider
  // at runtime -->
  // <dependency groupId="org.slf4j" artifactId="slf4j-log4j12" scope="runtime"/>
  // "org.slf4j" % "slf4j-log4j12" % "1.7.6",

  // <dependency groupId="org.apache.thrift" artifactId="libthrift"/>
  "org.apache.thrift" % "libthrift" % "0.9.3-1" exclude ("org.slf4j", "slf4j-api"),

  // <!-- don't need jna to run, but nice to have -->
  // <dependency groupId="net.java.dev.jna" artifactId="jna" optional="true"/>
  "net.java.dev.jna" % "jna" % "5.16.0",
  "net.java.dev.jna" % "jna-platform" % "5.16.0",

  // <dependency groupId="io.netty" artifactId="netty"/>
  // included by ext/net

  // <dependency groupId="com.clearspring.analytics" artifactId="stream"/>
  "com.clearspring.analytics" % "stream" % "2.9.8"
)
