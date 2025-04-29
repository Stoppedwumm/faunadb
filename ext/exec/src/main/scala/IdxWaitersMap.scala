package fauna.exec

import fauna.lang.syntax._
import java.util.concurrent.ConcurrentSkipListMap

/**
  * Satisfies consumers of poll() via updates to idx.
  */
class IdxWaitersMap[I](init: I)(implicit I: Ordering[I]) extends AsyncWaitersMap[I, Boolean] {

  protected val timer = Timer.Global
  protected val pendingPromises = new ConcurrentSkipListMap[I, PromiseRef](I)

  protected def timeoutForKey(idx: I) = FutureFalse
  protected def existingForKey(idx: I) = if (I.lteq(idx, _idx)) Some(FutureTrue) else None

  @volatile private[this] var _idx: I = init
  def idx: I = _idx

  /**
    * Updates {@code idx} to the specified value {@code i}, if current
    * {@code idx} is less than it. Completes with True all pending
    * promises waiting for index values up to and including {@code i}.
    * @param i the new idx value
    */
  def update(i: I): Unit =
    if (I.gt(i, _idx)) {
      _idx = i

      val iter = pendingPromises.headMap(i, true).values.iterator

      while (iter.hasNext) {
        iter.next.trySuccess(true)
      }
    }
}
