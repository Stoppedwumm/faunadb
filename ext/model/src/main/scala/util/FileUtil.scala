package fauna.util

import fauna.lang.syntax._
import java.io._
import java.nio.file._
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

object FileUtil {

  def move(source: Path, target: Path) = {
    if (Files.exists(source)) {
      Files.createDirectories(target.getParent)
      Files.move(source, target, REPLACE_EXISTING)
    }
  }

  def remove(f: File): Unit =
    if (f.exists) {
      if (f.isFile) {
        f.delete()
      } else {
        f.listFiles foreach remove
        f.delete()
      }
    }

  def mergeMove(from: Path, to: Path): Unit =
    if (Files.isDirectory(from)) {
      from.entries foreach { p =>
        mergeMove(p, to.resolve(p.getFileName.toString))
      }
    } else {
      Files.createDirectories(to.getParent)
      Files.move(from, to)
    }

}

