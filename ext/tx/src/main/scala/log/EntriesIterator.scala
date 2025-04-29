package fauna.tx.log

import io.netty.util.{ AbstractReferenceCounted, ReferenceCounted }
import scala.collection.BufferedIterator

import EntriesIterator._

/**
  * An iterator that tracks references of to an underlying resource,
  * used for Log entry views.
  *
  * Like normal iterators, it is not safe to re-use the iterator after
  * calling most methods. The exceptions, where it is safe to call are
  * the safe Iterator methods `hasNext`, `next`, and those inherited
  * from ReferenceCounted, `refCnt`, `release`, and `retain`.
  *
  * FIXME: This would not need to be ReferenceCounted if BinaryLog (or
  * any other hypothetical file-backed log) did not maintain one file
  * descriptor per iterator. Major caveat is truncating
  * AggregateBinaryLog... we don't necessarily want to invalidate any
  * iterators for a file segment that is being truncated out. It might
  * be OK to leave the FD dangling to eventually be closed when the
  * iterator passes over the file (or is GCed).
  */
trait EntriesIterator[+A] extends Iterator[A] with ReferenceCounted { self =>

  // EntriesIterator extras

  def delegateTo[B](iter: Iterator[B]): EntriesIterator[B] =
    new Delegate(self, iter)

  def delegateTo[B](iter: BufferedIterator[B]): BufferedEntriesIterator[B] =
    new BufferedDelegate(self, iter)

  def lastIdx[I, V](last: I)(implicit I: Ordering[I], ev: A <:< LogEntry[I, V]): EntriesIterator[A] =
    new RefProxy[A](self) with BufferedEntriesIterator[A] {
      val delegate = self.buffered

      def hasNext = delegate.hasNext && I.lteq(ev(delegate.head).idx, last)

      def next() = if (hasNext) delegate.next() else empty.next()

      def head = if (hasNext) delegate.head else empty.next()
    }

  // Iterator overrides

  override def buffered: BufferedEntriesIterator[A] =
    delegateTo(super.buffered)

  override def collect[B](pf: PartialFunction[A, B]): EntriesIterator[B] =
    delegateTo(super.collect(pf))

  override def drop(n: Int): EntriesIterator[A] = {
    super.drop(n)
    this
  }

  override def dropWhile(p: A => Boolean): EntriesIterator[A] = {
    val iter = buffered
    while (iter.hasNext && p(iter.head)) iter.next()
    iter
  }

  override def filter(p: A => Boolean): EntriesIterator[A] =
    delegateTo(super.filter(p))

  override def filterNot(p: A => Boolean): EntriesIterator[A] =
    delegateTo(super.filterNot(p))

  override def flatMap[B](f: A => IterableOnce[B]): EntriesIterator[B] =
    delegateTo(super.flatMap(f))

  override def map[B](f: A => B): EntriesIterator[B] =
    delegateTo(super.map(f))

  override def padTo[A1 >: A](len: Int, elem: A1): EntriesIterator[A1] =
    delegateTo(super.padTo(len, elem))

  override def patch[B >: A](from: Int, patchElems: Iterator[B], replaced: Int): EntriesIterator[B] =
    delegateTo(super.patch(from, patchElems, replaced))

  override def scanLeft[B](z: B)(op: (B, A) => B): EntriesIterator[B] =
    delegateTo(super.scanLeft(z)(op))

  override def slice(from: Int, until: Int): EntriesIterator[A] =
    delegateTo(super.slice(from, until))

  override def take(n: Int): EntriesIterator[A] =
    delegateTo(super.take(n))

  override def takeWhile(p: A => Boolean): EntriesIterator[A] =
    delegateTo(super.takeWhile(p))

  override def withFilter(p: A => Boolean): EntriesIterator[A] = filter(p)

  override def zipWithIndex: EntriesIterator[(A, Int)] =
    delegateTo(super.zipWithIndex)
}

/**
  * BufferedIterator version of EntriesIterator
  */
trait BufferedEntriesIterator[+A] extends EntriesIterator[A] with BufferedIterator[A] {
  override def buffered = this

  // downcast back to BufferedEntriesIterator
  override def lastIdx[I, V](last: I)
    (implicit I: Ordering[I], ev: A <:< LogEntry[I, V]): BufferedEntriesIterator[A] =
    super.lastIdx(last).asInstanceOf[BufferedEntriesIterator[A]]
}

// Abstract superclasses, consolidates generated trait impl into these
// two class files.

abstract class AbstractEntriesIterator[A]
    extends AbstractReferenceCounted
    with EntriesIterator[A]

abstract class AbstractBufferedEntriesIterator[A]
    extends AbstractReferenceCounted
    with BufferedEntriesIterator[A]

/**
  * Companion object for EntriesIterator
  */
object EntriesIterator {
  val empty: BufferedEntriesIterator[Nothing] = new BufferedEntriesIterator[Nothing] {
    def hasNext = false
    def next() = Iterator.empty.next()
    def head = next()
    def refCnt() = 1
    def release(i: Int) = false
    def release() = false
    def retain(i: Int) = this
    def retain() = this
    def touch() = this
    def touch(hint: Any) = this
  }

  def apply[A](self: ReferenceCounted, delegate: Iterator[A]): EntriesIterator[A] =
    new Delegate(self, delegate)

  def apply[A](self: ReferenceCounted, delegate: BufferedIterator[A]): BufferedEntriesIterator[A] =
    new BufferedDelegate(self, delegate)

  // Delegation machinery

  private[EntriesIterator] sealed abstract class RefProxy[A](self: ReferenceCounted)
      extends EntriesIterator[A] {

    def refCnt() = self.refCnt()
    def release(i: Int) = self.release(i)
    def release() = self.release()
    def retain(i: Int) = { self.retain(i); this }
    def retain() = { self.retain(); this }
    def touch() = { self.touch(); this }
    def touch(hint: Any) = { self.touch(hint); this }

    // pass through original ReferenceCounted

    override def delegateTo[B](iter: Iterator[B]) = new Delegate(self, iter)

    override def delegateTo[B](iter: BufferedIterator[B]) = new BufferedDelegate(self, iter)
}

  private[EntriesIterator] sealed class Delegate[A](
    self: ReferenceCounted,
    delegate: Iterator[A]) extends RefProxy[A](self) {

    def hasNext = delegate.hasNext
    def next() = delegate.next()

    // Override iterator methods to dispatch to delegate rather than superclass impls

    override def buffered: BufferedEntriesIterator[A] =
      delegateTo(delegate.buffered)

    override def collect[B](pf: PartialFunction[A, B]): EntriesIterator[B] =
      delegateTo(delegate.collect(pf))

    override def filter(p: A => Boolean): EntriesIterator[A] =
      delegateTo(delegate.filter(p))

    override def filterNot(p: A => Boolean): EntriesIterator[A] =
      delegateTo(delegate.filterNot(p))

    override def flatMap[B](f: A => IterableOnce[B]): EntriesIterator[B] =
      delegateTo(delegate.flatMap(f))

    override def map[B](f: A => B): EntriesIterator[B] =
      delegateTo(delegate.map(f))

    override def padTo[A1 >: A](len: Int, elem: A1): EntriesIterator[A1] =
      delegateTo(delegate.padTo(len, elem))

    override def patch[B >: A](from: Int, patchElems: Iterator[B], replaced: Int): EntriesIterator[B] =
      delegateTo(delegate.patch(from, patchElems, replaced))

    override def scanLeft[B](z: B)(op: (B, A) => B): EntriesIterator[B] =
      delegateTo(delegate.scanLeft(z)(op))

    override def slice(from: Int, until: Int): EntriesIterator[A] =
      delegateTo(delegate.slice(from, until))

    override def take(n: Int): EntriesIterator[A] =
      delegateTo(delegate.take(n))

    override def takeWhile(p: A => Boolean): EntriesIterator[A] =
      delegateTo(delegate.takeWhile(p))

    override def zipWithIndex: EntriesIterator[(A, Int)] =
      delegateTo(delegate.zipWithIndex)
  }

  private[EntriesIterator] final class BufferedDelegate[A](
    self: ReferenceCounted,
    delegate: BufferedIterator[A])
      extends Delegate[A](self, delegate)
      with BufferedEntriesIterator[A] {

    def head = delegate.head
  }
}
