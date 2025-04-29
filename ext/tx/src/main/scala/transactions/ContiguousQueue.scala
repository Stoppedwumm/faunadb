package fauna.tx.transaction

import java.util.PriorityQueue
import java.util.concurrent.ArrayBlockingQueue

object ContiguousQueue {
  final case class Entry[V](prev: Long, idx: Long, value: V) extends Ordered[Entry[V]] {
    def compare(o: Entry[V]) = prev compare o.prev
  }
}

/**
  * The contiguous queue is used to paper over out of order receipt of
  * events from an upstream source. It maintains a buffer of pending
  * entries which are still in the future, a queue of downstream
  * consumable entries, and a cursor to indicate the next entry
  * required to allow progress.
  */
final class ContiguousQueue[V](start: Long, maxPending: Int, maxBuffered: Int) {

  import ContiguousQueue._

  @volatile private[this] var cursor = start

  private[this] val q = new ArrayBlockingQueue[Entry[V]](maxPending)
  private[this] val buffer = new PriorityQueue[Entry[V]]

  override def toString = synchronized { s"ContiguousQueue($q, $buffer)" }

  def lastIdx = cursor

  def prevIdx = q.peek match {
    case null => cursor
    case e    => e.prev
  }

  // FIXME: This may need to release() entries that have been dropped.
  def add(prev: Long, idx: Long, value: V) = {
    require(prev < idx)
    synchronized { add0(prev, idx, value) }
  }

  @annotation.tailrec
  private def add0(prev: Long, idx: Long, value: V): Unit =
    (prev compare cursor).sign match {
      case -1 => ()
        buffer.poll match {
          case null           => ()
          case Entry(p, i, v) => add0(p, i, v)
        }

      case 0 =>
        if (q.offer(Entry(prev, idx, value))) {
          cursor = idx

          buffer.poll match {
            case null           => ()
            case Entry(p, i, v) => add0(p, i, v)

          }
        }

      case 1 =>
        // just drop the buffer if its full.
        if (buffer.size > maxBuffered) buffer.clear()
        buffer.offer(Entry(prev, idx, value))
    }

  def poll = q.poll

  def peek = q.peek
}
