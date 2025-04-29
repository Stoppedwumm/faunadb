package fauna.lang

import fauna.lang.syntax.string._
import scala.math.Ordering
import scala.collection.{ AbstractIterator, Iterator }
import scala.collection.mutable.PriorityQueue
import scala.language.implicitConversions

trait SeqSyntax {
  implicit def asRichSeqOfString(s: Seq[String]): SeqSyntax.RichSeqOfString =
    SeqSyntax.RichSeqOfString(s)

  implicit def asRichSeqOfIters[T](s: Seq[(Iterable[T], Int)]): SeqSyntax.RichSeqOfIters[T] =
    SeqSyntax.RichSeqOfIters(s)
}

object SeqSyntax {
  case class RichSeqOfString(s: Seq[String]) extends AnyVal {
    def asJSPath = s map { _.escapeDot } mkString "."
  }

  case class RichSeqOfIters[T](s: Seq[(Iterable[T], Int)]) extends AnyVal {
    def mergedOrderedIterator(implicit ord: Ordering[T]): Iterator[(T, Int)] = {

      val iters = s collect {
        case (ts, idx) if ts.nonEmpty =>
          val iter = ts.iterator
          (iter.next(), iter, idx)
      }

      // scala's PriorityQueue orders *greatest* first.
      // FIXME: See if java.util.PriorityQueue is faster
      val heapOrd = ord.reverse.on[(T, Iterator[T], Int)] { _._1 }
      val heap = PriorityQueue[(T, Iterator[T], Int)](iters: _*)(heapOrd)

      new AbstractIterator[(T, Int)] {
        def next() = {
          val (t, rest, idx) = heap.dequeue()
          if (rest.hasNext) heap.enqueue((rest.next(), rest, idx))
          (t, idx)
        }

        def hasNext = heap.nonEmpty
      }
    }
  }
}
