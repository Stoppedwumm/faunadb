package fauna.lang

import java.util.{ Collection, Queue }
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedQueue, CopyOnWriteArrayList }

/**
 * A priority tree queue
 *
 * Priority for any particular entry is set by that item's priority group id and
 * ancestor priority groups.
 *
 * Entries are stored in a tree of groups. A group contains a queue of entries
 * for its priority group as well as links to queues for any children of its
 * priority group. Each group tracks its overall size - the number of entires it
 * has in its queue plus the sum of all of its child groups.
 *
 * Entries are inserted via their priority group. A group's size is not updated
 * until the item has been stored in its queue ensuring counters can under
 * report, but never over report queued entries.
 *
 * Retrieving entries is done by providing the root group a buffer and max
 * number of entries to retrieve. Groups are free to decide how best to fill the
 * buffer based on priority group. A group will either provide entries from its
 * own queue or retrieve items from its child groups. Groups may retrieve
 * fewer entries than requested, but never more.
 *
 * Items are prioritised via Deficit Round Robin. We have strayed from the standard
 * implementation of DRR in two ways: 1) items are always of size 1, we consider
 * the number of items to remove per round rather than the size of an item; 2)
 * this is a concurrent, non-blocking implementation. The group itself keeps
 * track of its deficit rather than the group's parent. When items are requested
 * of a group it will either provide everything it has, only an amount equal to
 * the deficit it has available, or only the maximium amount requested. Once it
 * is determined how many items will be returned, that amount is taken from the
 * size and the deficit counters. The size counter is gaurded by a CAS, as such
 * it is not possible for a request to take more than what's available. However,
 * the deficit counter can be pushed negative by an intentional race condition
 * where multiple threads may collectively take more items than are available in the
 * deficit counter. The counter is increased only when the deficit is less than
 * one. Increasing the deficit is an unchecked CAS. We assume that if the check
 * fails, another thread has come through and increased the deficit already. A
 * similar assumption is made with the current queue index counter. A thread may
 * attempt to move the index and, if the check fails, it will assume another
 * thread has already moved it.
 */

case class PriorityTreeGroup(
  id: Any,
  ancestry: List[PriorityTreeGroup],
  weight: Int
) extends Ordered[PriorityTreeGroup] {
  assert(weight > 0)
  def compare(other: PriorityTreeGroup) = {
    this.ancestry.length compare other.ancestry.length match {
      case 0 => this.weight compare other.weight
      case w => w
    }
  }

  override def hashCode = id.hashCode

  override def equals(o: Any) =
    o match {
      case o: PriorityTreeGroup => id == o.id
      case _                    => false
    }

  def descendant(id: Any, weight: Int) =
    PriorityTreeGroup(id, ancestry :+ this, weight)
}

sealed trait PriorityTreeQueue[T] extends Any {
  def drainTo(buf: Collection[T], maxTake: Int): Int
}

class EntryQueue[T](
  val queue: Queue[T] = new ConcurrentLinkedQueue[T]
) extends AnyVal with PriorityTreeQueue[T] {

  def put(entry: T): Unit =
    queue.offer(entry)

  def drainTo(buf: Collection[T], maxTake: Int): Int =
    if (maxTake > 0) {
      drainTo(buf, maxTake, 0)
    } else {
      0
    }

  @annotation.tailrec
  final def drainTo(buf: Collection[T], maxTake: Int, got: Int): Int = {
    if (got == maxTake) return got

    val next = queue.poll()
    if (next == null) return got

    buf.add(next)
    drainTo(buf, maxTake, got + 1)
  }
}

/**
 * A queue group is a node in the priority group tree. When entries are
 * requested the group will either pull from its own entry queue or delegate to
 * child groups
 */
case class PriorityTreeQueueGroup[T](
  pGroup: PriorityTreeGroup
) extends PriorityTreeQueue[T] {
  @volatile private[this] var weight: Int = pGroup.weight
  private def updateWeight(w: Int): Unit = weight = w

  // The amount of available entries this group has access to. That is, entries on
  // behalf of itself, as well as entries it's children (and grandchildren) have
  // available.
  private[this] val _size = new AtomicInteger
  def size: Int = _size.get

  @volatile private[this] var hasSubGroups: Boolean = false
  private[this] val index = new ConcurrentHashMap[PriorityTreeGroup, PriorityTreeQueueGroup[T]]

  private[this] val curQueue = new AtomicInteger
  private[this] val queues = new CopyOnWriteArrayList[PriorityTreeQueue[T]]

  private[this] val entriesQueue = new EntryQueue[T]
  queues.add(entriesQueue)

  def put(group: PriorityTreeGroup, entry: T): Unit =
    put(group.ancestry, group, entry)

  private def getQueue(pg: PriorityTreeGroup): PriorityTreeQueueGroup[T] = {
    val queue = index.computeIfAbsent(pg, { pg =>
      val wg = PriorityTreeQueueGroup[T](pg)
      queues.add(wg)
      hasSubGroups = true
      wg
    })

    queue.updateWeight(pg.weight)
    queue
  }

  private def put(lineage: List[PriorityTreeGroup], group: PriorityTreeGroup, entry: T): Unit = {
    lineage match {
      case head :: tail =>
        getQueue(head).put(tail, group, entry)

      case Nil if group != pGroup =>
        getQueue(group).put(Nil, group, entry)

      case Nil =>
        entriesQueue.put(entry)
    }
    _size.incrementAndGet()
  }

  private[this] val deficit = new AtomicInteger
  private def reduceDeficit(amount: Int): Unit =
    deficit.addAndGet(-amount)

  private def increaseDeficit(cur: Int): Unit =
    if (cur < 1) deficit.compareAndSet(cur, cur + weight)

  private def nextIdx(cur: Int): Int = {
    curQueue.compareAndSet(cur, (cur + 1) % queues.size)
    curQueue.get
  }

  @annotation.tailrec
  final def drainTo(buf: Collection[T], maxTake: Int): Int = {
    val curSize = size
    val curDeficit = deficit.get
    val toTake = maxTake min curSize min curDeficit

    if (toTake < 1) {
      increaseDeficit(curDeficit)
      0

    } else if (_size.compareAndSet(curSize, curSize - toTake)) {
      reduceDeficit(toTake)

      if (hasSubGroups) {
        @annotation.tailrec
        def loop(needed: Int, idx: Int): Unit = {
          val got = queues.get(idx).drainTo(buf, needed)
          val left = needed - got
          if (left > 0) loop(left, nextIdx(idx))
        }
        loop(toTake, curQueue.get)

      } else {
        entriesQueue.drainTo(buf, toTake)
      }

      toTake
    } else {
      drainTo(buf, maxTake)
    }
  }
}
