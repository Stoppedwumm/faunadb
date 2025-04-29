package fauna.net.gossip

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

object FinitePriorityDeque {
  case class Elem[T] private (priority: Int, value: T)
}

/**
  * Specialized priority deque thingy for supporting gossip propagation.
  *
  * The goal is to make sure to give priority to the freshest hot-takes the
  * cluster has to offer. It does this by acting a LIFO queue that will shed
  * the oldest elements first.by priority.
  */
final class FinitePriorityDeque[T](capacity: Int, priorities: Int) {
  import FinitePriorityDeque.Elem

  private[this] val queues = Array.fill[ConcurrentLinkedDeque[T]](priorities) {
    new ConcurrentLinkedDeque[T]
  }
  private[this] val load = new AtomicInteger(0)

  /**
    * Take item from the queue.
    */
  def pop(): Option[Elem[T]] = {
    var idx = 0
    do {
      val queue = queues(idx)
      val value = queue.pollFirst()
      if (value != null) {
        load.decrementAndGet
        return Some(Elem(idx, value))
      }
      idx += 1
    } while (idx < priorities)
    None
  }

  /**
    * Push a value onto the queue for the first time.
    */
  def push(value: T): Unit = {
    insert(0, value)
  }

  /**
    * Return item with same priority.
    *
    * @return true if the element was accepted.
    */
  def replace(elem: Elem[T]): Boolean = {
    insert(elem.priority, elem.value)
  }

  /**
    * Return item with lower priority
    *
    * @return true if element was accepted.
    */
  def requeue(elem: Elem[T]): Boolean = {
    if (elem.priority < priorities - 1) {
      insert(elem.priority + 1, elem.value)
    } else {
      false
    }
  }

  def isEmpty: Boolean =
    load.get == 0

  def nonEmpty: Boolean =
    load.get != 0

  def size: Int =
    load.get

  private def insert(priority: Int, value: T): Boolean = {
    val canInsert = load.incrementAndGet <= capacity || dropLast(priority)
    if (canInsert) {
      queues(priority).offerFirst(value)
    } else {
      load.decrementAndGet
    }
    canInsert
  }

  private def dropLast(floor: Int): Boolean = {
    var idx = priorities - 1
    do {
      val queue = queues(idx)
      val value = queue.pollLast()
      if (value != null) {
        load.decrementAndGet
        return true
      }
      idx -= 1
    } while (idx >= floor)
    false
  }
}
