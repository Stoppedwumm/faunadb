package fql.error

import fql.ast.Src
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.ArrayBuffer

case class Source(val src: String) {
  // A list of newline indices in `src`.
  val lineNumbers: ArrayBuffer[Int] = ArrayBuffer.empty

  private def populateCacheFrom(from: Int) = {
    if (lineNumbers.isEmpty || lineNumbers.last < from) {
      var i = lineNumbers.lastOption match {
        case Some(last) => last + 1
        case None       => 0
      }
      while (i < (from min src.length)) {
        if (src(i) == '\n') {
          lineNumbers.append(i)
        }
        i += 1
      }
    }
  }

  /** Given a character index, returns the line number, starting from 0.
    */
  def lineIndexOf(from: Int): Int = {
    populateCacheFrom(from)
    lineNumbers.search(from).insertionPoint
  }

  /** Given a character index, returns the line number, starting from 1.
    */
  def lineNumberOf(from: Int): Int = lineIndexOf(from) + 1

  /** Given a character index, returns the character index at the start of that line.
    */
  def lineStart(from: Int): Int = {
    val idx = lineIndexOf(from)
    if (idx >= lineNumbers.length) {
      lineNumbers.lastOption match {
        case Some(last) => last + 1
        case None       => 0
      }
    } else if (idx == 0) {
      0
    } else {
      lineNumbers(idx - 1) + 1
    }
  }
}

case class RenderCtx(private val sourceCtx: MMap[Src.Id, Source]) {
  def apply(id: Src.Id): Option[Source] = sourceCtx.get(id)
  def apply(src: Src): Option[Source] =
    src match {
      case Src.Inline(name, str) =>
        Some(sourceCtx.getOrElseUpdate(Src.Id(name), Source(str)))
      case id: Src.Id => this(id)
    }
}

object RenderCtx {
  def apply(sourceCtx: Iterable[(Src.Id, String)]) = new RenderCtx(
    sourceCtx
      .map { case (id, src) => id -> Source(src) }
      .to(MMap))
}
