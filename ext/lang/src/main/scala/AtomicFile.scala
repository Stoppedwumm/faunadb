package fauna.lang

import fauna.lang.syntax.path._
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption._
import java.nio.file._
import scala.util.Using

object AtomicFile {
  def resetPaths(curr: Path, newP: Path, old: Path) = {
    // If currPath exists, then any other existing file is partial
    // state
    if (Files.exists(curr)) {
      Files.deleteIfExists(newP)
      Files.deleteIfExists(old)

    // If currPath does not exist, but newPath does, then
    // saveState succeeded.
    } else if (Files.exists(newP)) {
      newP.moveAtomically(curr)
      Files.deleteIfExists(old)

    // If neither newPath or currPath exists, the file has not been initialized.
    } else {
      throw new IllegalStateException("Missing or uninitialized file state.")
    }

    Using.resource(FileChannel.open(curr.getParent, READ)) { _.force(true) }
  }
}

/**
  * Read/write to a file in a manner that is crash-safe.
  */
case class AtomicFile(path: Path) extends AnyVal {
  def newPath = path + ".new"
  def oldPath = path + ".old"

  def reset(): Unit =
    path.synchronized {
      AtomicFile.resetPaths(path, newPath, oldPath)
    }

  def exists: Boolean =
    path.synchronized {
      try {
        reset()
        true
      } catch {
        case _: IllegalStateException => false
      }
    }

  def read: FileChannel =
    path.synchronized {
      reset()
      FileChannel.open(path, READ)
    }

  def read[T](f: FileChannel => T): T = {
    val c = read
    try f(c) finally c.close()
  }

  def moveFrom(src: Path): Unit =
    path.synchronized {
      reset()
      src.move(newPath)
      path.moveAtomically(oldPath)
      reset()
    }

  def write(f: FileChannel => Unit): Unit =
    path.synchronized {
      reset()
      writeToPath(newPath, f)
      path.moveAtomically(oldPath)
      reset()
    }

  def create(f: FileChannel => Unit): Boolean =
    path.synchronized {
      Files.createDirectories(path.normalize.getParent)
      if (exists) {
        false
      } else {
        writeToPath(oldPath, f)
        oldPath.moveAtomically(newPath)
        reset()
        true
      }
    }

  private def writeToPath(p: Path, f: FileChannel => Unit) = {
    val c = FileChannel.open(p, WRITE, CREATE_NEW, TRUNCATE_EXISTING)

    f(c)
    c.force(true)
    c.close()
  }
}
