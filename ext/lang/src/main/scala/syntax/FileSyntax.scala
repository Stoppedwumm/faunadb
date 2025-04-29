package fauna.lang

import java.io.File
import scala.language.implicitConversions

trait FileSyntax {
  implicit def asRichFile(f: File): FileSyntax.RichFile =
    FileSyntax.RichFile(f)
}

object FileSyntax {
  case class RichFile(f: File) extends AnyVal {
    def listing: Array[File] =
      Option(f.listFiles) getOrElse Array.empty
  }
}
