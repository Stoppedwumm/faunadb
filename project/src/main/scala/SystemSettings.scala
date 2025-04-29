package fauna.sbt

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import scala.sys.process._

object SystemSettings {
  sealed trait OS {
    val defaultRoot = Option(System.getenv("FAUNA_TEST_ROOT"))

    def name: String

    def faunaRoot: Path
    def kill(pid: String): Unit
    def getCorePids(): Seq[String]
  }

  case object Windows extends OS {
    val name = "windows"
    val faunaRoot = {
      val root = defaultRoot.getOrElse(System.getProperty("java.io.tmpdir"))
      Paths.get(root, "fauna-api-test")
    }

    def kill(pid: String) = {
      val pShellScript = s"(Get-Process -Pid $pid).Kill()"
      Process("powershell.exe" :: pShellScript :: Nil).!
    }

    def getCorePids() = {
      val pShellScript =
        "Get-WmiObject Win32_Process -Filter \\\"name = 'java.exe'\\\" | Where-Object {$_.CommandLine -match 'fauna.core.Service'} | select -ExpandProperty ProcessId"
      Process("powershell.exe" :: pShellScript :: Nil).lineStream_!
    }
  }

  abstract class UnixLike extends OS {
    def kill(pid: String) = (s"kill -9 ${pid}").!

    def getCorePids() = {
      Process("jps" :: "-lm" :: Nil).lineStream_!
        .filter { _.contains("fauna.core.Service") }
        .map { _.split(" ").head }
    }
  }

  case object MacOSX extends UnixLike {
    val name = "osx"
    val faunaRoot = {
      val root = defaultRoot.getOrElse("/Volumes/fauna-api-test")
      Paths.get(root)
    }
  }

  case object Linux extends UnixLike {
    val name = "linux"
    val faunaRoot = {
      val root = defaultRoot.getOrElse("/dev/shm/fauna-api-test")
      Paths.get(root)
    }
  }

  case object Unknown extends UnixLike {
    val name = "unknown"
    val faunaRoot = {
      val root = defaultRoot.getOrElse(System.getProperty("java.io.tmpdir"))
      Paths.get(root, "fauna-api-test")
    }
  }

  val os = System.getProperty("os.name") match {
    case "Linux"                      => Linux
    case "Mac OS X"                   => MacOSX
    case x if x.startsWith("Windows") => Windows
    case _                            => Unknown
  }

  def recursivelyDelete(path: Path) = {
    if (Files.exists(path)) {
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes) = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException) = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }
  }
}
