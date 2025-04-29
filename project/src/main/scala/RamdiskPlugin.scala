package fauna.sbt

import com.fasterxml.jackson.databind._
import java.nio.file.{ Files, Path }
import sbt._
import sbt.plugins.JvmPlugin
import sbt.Keys._
import scala.collection.JavaConverters._
import scala.sys.process._

object RamdiskPlugin extends AutoPlugin {
  import Keys._

  object Keys {
    val setupRamdisk =
      TaskKey[Unit]("setup-ramdisk", "Initialize ramdisk for tests") in Global
    val destroyRamdisk =
      TaskKey[Unit]("destroy-ramdisk", "Destroy ramdisk for tests") in Global
    val resetRamdisk =
      TaskKey[Unit]("reset-ramdisk", "Reset ramdisk for tests") in Global
    val ramdiskFreeSpace = TaskKey[Double](
      "ramdisk-free-space",
      "Calculate free ramdisk space for tests") in Global
  }

  override def requires = JvmPlugin
  val autoImport = Keys

  override val globalSettings = Seq(
    setupRamdisk := Ramdisk.setupRamdisk(),
    destroyRamdisk := Ramdisk.destroyRamdisk(),
    resetRamdisk := {
      Ramdisk.destroyRamdisk()
      Ramdisk.setupRamdisk()
    },
    ramdiskFreeSpace := Ramdisk.ramdiskFreeSpace()
  )

  override val projectSettings = inConfig(Test)(
    Seq(
      test := (test dependsOn resetRamdisk).value,
      testOnly := (testOnly dependsOn resetRamdisk).evaluated,
      testQuick := (testQuick dependsOn resetRamdisk).evaluated))
}

object Ramdisk {
  private val impl = SystemSettings.os match {
    case SystemSettings.Linux  => LinuxHelperImplementation
    case SystemSettings.MacOSX => MacOSXHelperImplementation
    case _                     => UnknownHelperImplementation
  }

  def setupRamdisk() = impl.setupRamdisk()
  def destroyRamdisk() = impl.destroyRamdisk()
  def ramdiskFreeSpace() = impl.ramdiskFreeSpace()
}

trait HelperImplementation {
  def faunaRoot: Path
  def setupRamdisk(): Unit
  def destroyRamdisk(): Unit
  def ramdiskFreeSpace(): Double
}

object LinuxHelperImplementation extends HelperImplementation {
  val faunaRoot = SystemSettings.Linux.faunaRoot
  def setupRamdisk() = s"mkdir -p $faunaRoot" !
  def destroyRamdisk() = s"rm -rf $faunaRoot" !
  def ramdiskFreeSpace = 1.0
}

object MacOSXHelperImplementation extends HelperImplementation {
  val faunaRoot = SystemSettings.MacOSX.faunaRoot

  def setupRamdisk() = {
    getRamdiskInfo.headOption match {
      case Some(_) =>
        if (Files.isDirectory(faunaRoot) && Files.list(faunaRoot).count() == 0) {
          s"rmdir $faunaRoot" !
        }
      case None =>
        val device = ("hdiutil attach -nomount ram://8388608" !!)
        // Give the disk time to mount on OS X
        Thread.sleep(100)
        s"diskutil eraseVolume HFS+ fauna-api-test $device" !
    }
  }

  def destroyRamdisk() = {
    getRamdiskInfo foreach { ramdisk =>
      ("hdiutil" :: "detach" :: "-force" :: ramdisk :: Nil).!
    }

    if (Files.exists(faunaRoot)) (s"rm -rf $faunaRoot" !)
  }

  def ramdiskFreeSpace = {
    val store = Files.getFileStore(faunaRoot)
    store.getUnallocatedSpace.toDouble / store.getTotalSpace.toDouble
  }

  private def getRamdiskInfo: Seq[String] = {
    val output = ("diskutil list -plist" #| "plutil -convert json - -o -").!!
    val mapper = new ObjectMapper()
    val root = mapper.readTree(output)
    val allDisks = root.path("AllDisksAndPartitions").elements.asScala

    val ramdisks = Seq.newBuilder[String]

    allDisks foreach { disk =>
      if (disk.has("VolumeName")) {
        val vol = disk.get("VolumeName").asText

        if (vol == "fauna-api-test" && disk.has("MountPoint")) {
          ramdisks += disk.get("MountPoint").asText
        }
      }
    }

    ramdisks.result
  }
}

object UnknownHelperImplementation extends HelperImplementation {
  val faunaRoot = SystemSettings.Unknown.faunaRoot
  def setupRamdisk(): Unit = {}
  def destroyRamdisk(): Unit = {}
  def ramdiskFreeSpace(): Double = 1.0
}
