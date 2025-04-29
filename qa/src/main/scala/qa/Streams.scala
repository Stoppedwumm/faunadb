package fauna.qa

import java.util.concurrent.atomic.AtomicLong

/**
  * RequestIterator is a named iterator that will produce FaunaQuerys. Generally
  * this should produce a finite number of requests.
  */
trait RequestIterator extends Iterator[FaunaQuery] {
  val name: String
  def hasNext: Boolean
  def next(): FaunaQuery

  override def toString: String = s"RequestIterator($name, $hasNext)"
}

// Helper for creating a RequestIterator from any Iterator[FaunaQuery]
object RequestIterator {

  def apply(_name: String, iter: Iterator[FaunaQuery]): RequestIterator =
    new RequestIterator {
      val name = _name
      def hasNext = iter.hasNext
      def next() = iter.next()
    }

  def apply(name: String, iter: Seq[Iterator[FaunaQuery]]): RequestIterator =
    apply(name, iter.reduce[Iterator[FaunaQuery]] { case (l, r) => l ++ r })

  def apply(name: String, iter: Iterable[FaunaQuery]): RequestIterator =
    apply(name, iter.iterator)

  val Empty = apply("empty", Iterator.empty)
}

/**
  * RequestStream is a named iterator that produces an infinite number of
  * FaunaQuerys. This allows for sophisticated, non-blocking
  * stream behavior.
  */
trait RequestStream extends RequestIterator {
  final def hasNext: Boolean = true
  override def toString: String = s"RequestStream($name)"
}

object RequestStream {

  def apply(_name: String, f: () => FaunaQuery): RequestStream =
    new RequestStream {
      val name = _name
      def next() = f().copy(name = Some(name))
    }
}

class MultiStream(streams: IndexedSeq[RequestStream]) extends RequestStream {
  private[this] val _idx = new AtomicLong(0)
  override val size: Int = streams.length

  val name = s"MultiStream(${streams.map(_.name).mkString(", ")})"
  def next() = streams((_idx.incrementAndGet % size).toInt).next()
}
