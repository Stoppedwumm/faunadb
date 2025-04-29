package fauna.lang

import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }

/** A simple non-thread safe LRU cache based on a `LinkedHashMap`. In addition to
  * evicting the last recent used entry when at capacity, this LRU implementation
  * supports pinning entries for cached values that implements `Pinnable`.
  */
sealed abstract class LRU[K, V]
    extends JLinkedHashMap[K, V](
      LRU.InitialCapacity,
      LRU.LoadFactor,
      LRU.AccessOrder
    )

object LRU {

  /** Initial hash map's capacity (JVM's default value). */
  val InitialCapacity = 16

  /** Hash map's load factor (JVM's default value). */
  val LoadFactor = .75f

  /** Sort entries in the map by access order (a.k.a.: LRU). */
  val AccessOrder = true

  /** The amount of entries to search inline for unpinned values when evicting. */
  val MaxInlineSearch = 64

  /** Cache values implementing the `Pinnable` trait can be pinned to cache until
    *  `isPinned` returns `false`. Note that pinning entries does NOT guarantee their
    *  presence in the cache. Upon finding a pinned entry at its tail, the LRU does a
    *  bounded inline search for unpinned entries to evict instead. If none is found,
    *  the pinned entry at the tail will be evicted.
    */
  trait Pinnable { def isPinned: Boolean }

  @inline def unpinneable[K, V](maxCapacity: Int): LRU[K, V] =
    new UnpinneableLRU(maxCapacity)

  private final class UnpinneableLRU[K, V](maxCapacity: Int) extends LRU[K, V] {
    override protected def removeEldestEntry(entry: JMap.Entry[K, V]): Boolean =
      size > maxCapacity
  }

  @inline def pinneable[K, V <: Pinnable](maxCapacity: Int): LRU[K, V] =
    new PinnableLRU(maxCapacity)

  private final class PinnableLRU[K, V <: Pinnable](maxCapacity: Int)
      extends LRU[K, V] {

    override protected def removeEldestEntry(entry: JMap.Entry[K, V]): Boolean =
      size > maxCapacity && (!entry.getValue.isPinned || searchAndDropUnpinned())

    private def searchAndDropUnpinned(): Boolean = {
      var i = maxCapacity.min(LRU.MaxInlineSearch)
      val iter = values.iterator
      while (i > 0) {
        iter.next() match {
          case p: LRU.Pinnable if p.isPinned => ()
          case _ =>
            iter.remove()
            return false // unpinned entry found; keep the tail.
        }
        i -= 1
      }
      true // no unpinned entry found; drop the tail.
    }
  }
}
