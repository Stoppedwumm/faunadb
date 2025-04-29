package fauna.sbt

import fauna.sbt.GitPlugin.Keys._
import java.io.FileWriter
import java.util.Properties
import sbt._
import sbt.Keys._
import sbtassembly.{ AssemblyPlugin, MergeStrategy }
import sbtassembly.AssemblyKeys._

object ProjectSettingsPlugin extends AutoPlugin {
  override def requires = AssemblyPlugin
  override def trigger = allRequirements

  object Keys {
    val outputBuildProps = TaskKey[Unit]("output-build-props", "Outputs build props")
  }

  val autoImport = Keys

  override val projectSettings = Seq(
    Keys.outputBuildProps := {
      val sha = gitSHA.value
      val file = new File(baseDirectory.value + "/target/resources/build.properties")

      file.getParentFile.mkdirs()
      val properties = new Properties
      properties.setProperty("build.id", sha)
      properties.setProperty("build.version", version.value)
      val writer = new FileWriter(file)
      properties.store(writer, "Build properties")
      writer.close()
    },
    assembly / fullClasspath := {
      val cp = (assembly / fullClasspath).value
      val bd = baseDirectory.value

      cp :+ Attributed.blank(bd / "target" / "resources")
    },
    assembly := (assembly dependsOn Keys.outputBuildProps).value
  )
}

object AssemblyDefaultsPlugin extends AutoPlugin {
  override def requires = AssemblyPlugin
  override def trigger = allRequirements

  override val projectSettings = Seq(
    assembly / assemblyMergeStrategy := {
      case x if x endsWith "io.netty.versions.properties" => MergeStrategy.discard
      case x if x endsWith "module-info.class"            => MergeStrategy.discard
      case x =>
        val strat = (assembly / assemblyMergeStrategy).value
        strat(x)
    }
  )
}
