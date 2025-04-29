import fauna.sbt.CodexHelpers._
import fauna.sbt.CoreDefs

lazy val codex = (project in file("."))
  .dependsOn(LocalProject("lang"), LocalProject("prop") % "test->test")

name := "codex"
organization := "fauna"
version := "0.0.1"

libraryDependencies ++= Seq(
  CoreDefs.compilerDependency,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.2",
  "com.ibm.icu" % "icu4j" % "73.2")

libraryDependencies ++= CoreDefs.nettyDependencies

(Compile / sourceGenerators) += Def.task {
  val dir = (Compile / sourceManaged).value

  val recordDecoders = dir / "codex" / "RecordDecoders.scala"
  IO.write(recordDecoders, genRecordDecoders)

  val recordEncoders = dir / "codex" / "RecordEncoders.scala"
  IO.write(recordEncoders, genRecordEncoders)

  val recordCodecs = dir / "codex" / "RecordCodecs.scala"
  IO.write(recordCodecs, genRecordCodecs)

  val tupleDecoders = dir / "codex" / "TupleDecoders.scala"
  IO.write(tupleDecoders, genTupleDecoders)

  val tupleEncoders = dir / "codex" / "TupleEncoders.scala"
  IO.write(tupleEncoders, genTupleEncoders)

  Seq(
    recordDecoders,
    recordEncoders,
    recordCodecs,
    tupleDecoders,
    tupleEncoders
  )
}
