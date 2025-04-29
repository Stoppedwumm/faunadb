package fauna.lang

import java.lang.{ Integer => JInt }
import java.util.Arrays
import scala.reflect.ClassTag

object SpinedSeq {

  /** Minimum power-of-two for the first chunk.
    *
    * Note the relationship between this value and
    * ValueSet.DefaultPageSize.
    */
  val MinChunkPower = 4

  /** Maximum power-of-two for chunks. */
  val MaxChunkPower = 30

  /** Minimum capacity for the array-of-chunks. */
  val MinSpineSize = 8

  def apply[T: ClassTag](initialCapacity: Int): SpinedSeq[T] = {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException(
        s"initial capacity must be >= 0, but got $initialCapacity")
    }

    new SpinedSeq[T](
      MinChunkPower max
        JInt.SIZE - JInt.numberOfLeadingZeros(initialCapacity - 1))
  }

  def empty[T: ClassTag]: SpinedSeq[T] = new SpinedSeq(MinChunkPower)
}

/** An ordered collection. Elements may be appended, but not updated or removed.
  *
  * Elements are stored in a series of arrays. Using multiple arrays
  * has better performance characteristics than a single array, like
  * ArraySeq, as when capacity increases no copying is required.
  */
final class SpinedSeq[T: ClassTag] private (initialChunkPower: Int) extends Seq[T] {
  import SpinedSeq._

  // Index of the next element to write, possibly off the RHS of the
  // current chunk.
  private[this] var elementIndex: Int = 0

  // Index of the current chunk in the array-of-chunks.
  private[this] var spineIndex: Int = 0

  // Count of all elements in prior chunks.
  private[this] var priorElementCount: Array[Long] = _

  private[this] var current: Array[T] = new Array[T](1 << initialChunkPower)

  // An array-of-chunks, or null if there is only a single chunk.
  private[this] var spine: Array[Array[T]] = _

  override def isEmpty: Boolean = (spineIndex == 0) && (elementIndex == 0)

  def length: Int = if (spineIndex == 0) {
    elementIndex
  } else {
    (priorElementCount(spineIndex) + elementIndex).intValue
  }

  override def knownSize: Int = length

  def capacity: Long = if (spineIndex == 0) {
    current.length
  } else {
    priorElementCount(spineIndex) + spine(spineIndex).length
  }

  def apply(index: Int): T = {
    def oob() = throw new IndexOutOfBoundsException(index.toString)

    if (spineIndex == 0) {
      if (index < elementIndex) {
        return current(index)
      } else {
        oob()
      }
    }

    if (index >= length) {
      oob()
    }

    var j = 0
    while (j <= spineIndex) {

      if (index < priorElementCount(j) + spine(j).length) {
        return spine(j)((index - priorElementCount(j)).intValue)
      }

      j += 1
    }

    oob()
  }

  def append(elem: T): SpinedSeq[T] = {
    if (elementIndex == current.length) {
      inflateSpine()

      if (spineIndex + 1 >= spine.length || (spine(spineIndex + 1) eq null)) {
        ensureCapacity(capacity + 1)
      }

      elementIndex = 0
      spineIndex += 1
      current = spine(spineIndex)
    }

    current(elementIndex) = elem
    elementIndex += 1
    this
  }

  def iterator: Iterator[T] = {
    val buf = this
    new Iterator[T] {
      private[this] var index = 0
      private[this] val len = buf.length

      def hasNext: Boolean = index < len

      def next(): T = {
        val elem = buf(index)
        index += 1
        elem
      }
    }
  }

  // How big should the nth chunk be?
  private def chunkSize(n: Int): Int = {
    val power: Int = if (n == 0 || n == 1) {
      initialChunkPower
    } else {
      MaxChunkPower min (initialChunkPower + n - 1)
    }

    1 << power
  }

  // When expanding to the second chunk, inflate the spine to
  // accomodate more than one chunk. This is lazy to allow for common
  // patterns which remain below the capacity of a single chunk.
  private def inflateSpine() =
    if (spine eq null) {
      spine = new Array[Array[T]](MinSpineSize)
      priorElementCount = new Array[Long](MinSpineSize)
      spine(0) = current
    }

  // Expand capacity to accomodate at least the target size.
  private def ensureCapacity(targetSize: Long) = {
    var cap = capacity
    if (targetSize > capacity) {
      inflateSpine()

      var i = spineIndex + 1
      while (targetSize > cap) {

        if (i >= spine.length) {
          val sz = spine.length * 2
          spine = Arrays.copyOf(spine, sz)
          priorElementCount = Arrays.copyOf(priorElementCount, sz)
        }

        val nextSize = chunkSize(i)
        spine(i) = new Array[T](nextSize)
        priorElementCount(i) = priorElementCount(i - 1) + spine(i - 1).length
        cap += nextSize

        i += 1
      }
    }
  }
}
