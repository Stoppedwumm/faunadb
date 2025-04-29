import fauna.sbt.CoreDefs

lazy val limits = (project in file("."))
  .dependsOn(
    LocalProject("atoms"),
    LocalProject("codex"),
    LocalProject("flags"),
    LocalProject("stats"))

name := "limits"
organization := "fauna"
version := "0.0.1"
