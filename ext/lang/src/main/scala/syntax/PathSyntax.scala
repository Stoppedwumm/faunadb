package fauna.lang

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file._
import java.nio.file.{ Files, StandardCopyOption }
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.StandardOpenOption.READ
import scala.language.implicitConversions

trait PathSyntax {
  implicit def asRichPath(p: Path): PathSyntax.RichPath =
    PathSyntax.RichPath(p)
}

object PathSyntax {
  case class RichPath(p: Path) extends AnyVal {
    def /(sub: String): Path = p.resolve(sub)

    def /(sub: Path): Path = p.resolve(sub)

    def +(suffix: String): Path = p.resolveSibling(p.getFileName.toString + suffix)

    def move(dest: Path): Path =
      Files.move(p, dest)

    def moveAtomically(dest: Path, mkdirs: Boolean = false): Path = {
      var tries = 0
      var ret: Path = null
      while (ret eq null) {
        tries += 1
        try {
          Files.createDirectories(dest.getParent())
          ret = Files.move(p, dest, StandardCopyOption.ATOMIC_MOVE)
        } catch {
          // this means the directory did not exist, so try again.
          // Loop here, since a coordinating process may be trying to clear the
          // destination directory, but don't spin forever. See ExportDataTask
          case _: NoSuchFileException if mkdirs && tries < 100 =>
        }
      }
      ret
    }

    def mkdirs(): Unit =
      Files.createDirectories(p)

    def touch(): Unit =
      Files.createFile(p)

    def findAll: List[Path] = {
      val b = List.newBuilder[Path]
      foreachFile { b += _ }
      b.result()
    }

    def findAllRecursively: List[Path] = {
      val b = List.newBuilder[Path]

      def find0(p: Path): Unit = {
        if (Files.isDirectory(p)) {
          RichPath(p).foreachFile { find0(_) }
        } else if (Files.isSymbolicLink(p) || Files.isRegularFile(p)) {
          b += p
        }
      }

      find0(p)

      b.result()
    }

    def delete() =
      try Files.delete(p) catch {
        case _: NoSuchFileException => ()
      }

    def deleteRecursively(): Unit =
      if (Files.isSymbolicLink(p)) {
        delete()
      } else if (Files.isDirectory(p)) {
        entries foreach { RichPath(_).deleteRecursively() }
        try {
          delete()
        } catch {
          case _: DirectoryNotEmptyException =>
            // race with another thread that is still adding entries to this directory, just recur/retry
            deleteRecursively()
        }
      } else {
        delete()
      }

    def copyRecursively(dest: Path): Unit = {
      require(!Files.isSymbolicLink(p), "Cannot copy symbolic links")
      if (Files.isDirectory(p)) {
        entries foreach { path =>
          val destPath = dest.resolve(p.relativize(path))
          RichPath(path).copyRecursively(destPath)
        }
      } else {
        Files.createDirectories(dest.getParent)
        Files.copy(p, dest)
      }
    }

    def entries: List[Path] = {
      val b = List.newBuilder[Path]
      val dir = Files.newDirectoryStream(p)

      try {
        val iter = dir.iterator
        while (iter.hasNext) b += iter.next
      } finally {
        dir.close()
      }

      b.result()
    }

    def isEmpty: Boolean =
      if (!Files.exists(p)) {
        true
      } else if (!Files.isDirectory(p)) {
        false
      } else {
        val stream = Files.newDirectoryStream(p)

        try {
          !stream.iterator.hasNext
        } finally {
          stream.close()
        }
      }

    /** Search in `p` for files matching the provided predicate,
      * yielding each to the closure.
      */
    def search(predicate: String)(f: Path => Unit): Unit = {
      val stream = Files.newDirectoryStream(p, predicate)
      try {
        val iter = stream.iterator
        while (iter.hasNext) {
          f(iter.next)
        }
      } finally {
        stream.close()
      }
    }

    def foreachFile[U](f: Path => U, ignoreErrors: Boolean = false): Unit = Files.walkFileTree(p, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes) = { f(file); FileVisitResult.CONTINUE }
      override def visitFileFailed(file: Path, e: IOException) =
        if (ignoreErrors) {
          FileVisitResult.CONTINUE
        } else {
          super.visitFileFailed(file, e)
        }
    })

    def readString: String = new String(Files.readAllBytes(p), "UTF-8")

    def read[T](f: FileChannel => T): T = {
      val c = FileChannel.open(p, READ)
      try {
        f(c)
      } finally {
        c.close()
      }
    }
  }
}
