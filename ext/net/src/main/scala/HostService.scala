package fauna.net

import fauna.atoms._
import fauna.net.ScoreKeeper
import java.util.concurrent.ThreadLocalRandom
import scala.collection.immutable.ArraySeq
import scala.util.{ Random, Sorting }

object HostService {

  def preferredOrder(
    hosts: Iterable[HostID],
    isLive: HostID => Boolean,
    isLocal: HostID => Boolean,
    isNear: HostID => Boolean): ArraySeq[HostID] = {

    val rnd = new Random(ThreadLocalRandom.current)
    val b = ArraySeq.newBuilder[HostID]

    def add(hs: Iterable[HostID]) = {
      val (local, nonLocal) = hs partition isLocal
      val (near, far) = nonLocal partition isNear
      b ++= rnd.shuffle(local)
      b ++= rnd.shuffle(near)
      b ++= rnd.shuffle(far)
    }

    val (live, dead) = hosts partition isLive
    add(live)
    add(dead)

    b.result()
  }
}

trait HostService {
  // a host must score this much better than another to be preferred
  val StickyFactor = 1.5

  /**
    * Returns true if the provided host is suspected to be prepared to
    * participate in the cluster.
    */
  def isLive(host: HostID): Boolean

  /** Returns true if the provided host is suspected to be in the same replica. */
  def isLocal(host: HostID): Boolean

  /** Returns true if the provided host is in a neighbor replica. */
  def isNear(host: HostID): Boolean

  /**
    * Notify `f` with a HostID when a process (re-)start is detected.
    */
  def subscribeStartsAndRestarts(f: HostID => Unit): Unit

  final def preferredOrder(hosts: Iterable[HostID]): Seq[HostID] =
    HostService.preferredOrder(hosts, isLive, isLocal, isNear)

  final def preferredOrder(hosts: Iterable[HostID], sk: ScoreKeeper): Seq[HostID] = {
    val res = hosts.toArray
    Sorting.stableSort(res, lessThan(_, _, sk))
    ArraySeq.unsafeWrapArray(res)
  }

  /**
    * This method is meant to be more efficient for choosing a single node to
    * make a request to than calling orderedNodesForSegment take 1. Offset is
    * used to choose nodes round-robin fashion across subsequent retries.
    *
    * NB: This expects `hosts.size` to be efficient, which is true of Vector and Set.
    */
  final def preferredNode(hosts: Iterable[HostID], offset: Int = 0) = {
    assert(offset >= 0)
    val sz = hosts.size
    if (sz <= 1) {
      hosts.headOption
    } else {
      val ordered = preferredOrder(hosts)
      Some(ordered(offset % sz))
    }
  }

  private def lessThan(a: HostID, b: HostID, sk: ScoreKeeper): Boolean =
    compare(a, b, sk) < 0

  private def compare(a: HostID, b: HostID, sk: ScoreKeeper): Int = {
    (isLive(a), isLive(b)) match {
      case (true, false) => -1
      case (false, true) => 1
      case _ =>
        (sk.score(a), sk.score(b)) match {
          case (a, b) if a < b && (StickyFactor * a) < b => -1
          case (a, b) if b < a && (StickyFactor * b) < a => 1
          case _ => 0
        }
    }
  }
}
