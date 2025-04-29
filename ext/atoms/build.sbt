import fauna.sbt.CoreDefs
import java.io.FileInputStream
import java.util.Properties
import sys.process._

lazy val atoms = (project in file("."))
  .enablePlugins(RamdiskPlugin, BuildInfoPlugin)
  .dependsOn(LocalProject("codex"), LocalProject("lang"))
  .settings(
    name := "atoms",
    organization := "fauna",
    Test / fork := true,
    version := releaseProps.getProperty("version","unknown"),
    buildInfoKeys := Seq[BuildInfoKey](
      "product" -> releaseProps.getProperty("productName","unknown"),
      version, scalaVersion, sbtVersion,
      "revision" -> releaseProps.getProperty("revision","unknown"),
      "buildTimestamp" -> new java.util.Date(System.currentTimeMillis()),
      "buildStamp" -> java.lang.Long.valueOf(System.currentTimeMillis()),
      "gitFullHash" -> new Object() {
        override def toString() = Process("git rev-parse HEAD").lineStream.head
      },
      "gitBranch" -> new Object() {
        override def toString() = {
          val dirtyCount = Process("git status --porcelain").lineStream.size
          val dirtyList = ("git status --porcelain" !!).trim
          val branchName = ("git rev-parse --abbrev-ref HEAD" !!).trim
          if (dirtyCount > 0) {
            s"$branchName DIRTY($dirtyCount) \n $dirtyList"
          } else {
            branchName
          }
        }
      }
    ),
    buildInfoPackage := "fauna.atoms"
  )

lazy val releaseProps = {
  val props = new Properties()
  val in = new FileInputStream("service/release.properties")
  props.load(in)
  in.close()
  props
}
