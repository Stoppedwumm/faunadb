package fauna.sbt

import sbt._
import scala.sys.process._

object GitPlugin extends AutoPlugin {
  import Keys._

  object Keys {
    val gitSHA = SettingKey[String]("git-sha", "Current Git SHA")
  }

  override def trigger = allRequirements
  val autoImport = Keys

  override val projectSettings =
    Seq(gitSHA := { ("git rev-parse --short=7 HEAD" !!).trim })
}
