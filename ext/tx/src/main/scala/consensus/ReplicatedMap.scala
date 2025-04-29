package fauna.tx.consensus

import fauna.codex.cbor.CBOR
import fauna.tx.log.{ LogEntry, TX }
import java.util.concurrent.TimeoutException
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration

case class CompareAndSet[K, V](key: K, expect: Option[V], update: Option[V])

object CompareAndSet {
  implicit def codec[K: CBOR.Codec, V: CBOR.Codec] = CBOR.TupleCodec[CompareAndSet[K, V]]
}

object ReplicatedMap {
  def apply[K, V](log: ReplicatedLog[CompareAndSet[K, V]], timeout: Duration)(implicit ec: ExecutionContext) =
    new ReplicatedMap(log, timeout)
}

/**
  * A replicated map providing atomic compare-and-set operation.
  *
  * @param log the replicated log for distributed consensus
  * @tparam K the type of keys.
  * @tparam V the type of values.
  * @param ec an implicit execution context for asynchronous operations
  */
class ReplicatedMap[K, V](log: ReplicatedLog[CompareAndSet[K, V]], timeout: Duration)(implicit ec: ExecutionContext) {
  @volatile private[this] var map = Map[K, V]()
  private[this] var _lastApplied = TX.MinValue

  /**
    * Returns the last TX that was applied to this replicated map's state.
    * @return the last TX that was applied to this replicated map's state.
    */
  def lastApplied = synchronized {
    _lastApplied
  }

  /**
    * Returns a value associated with a key without synchronizing with the
    * consensus cluster. The value reflects some past consensual value but
    * there is no guarantee it is current.
    *
    * @param key the key of the value to retrieve
    * @return the local value associated with the key, or None if no value is
    *         associated.
    */
  def getUnsynced(key: K): Option[V] = map.get(key)

  /**
    * Returns a future for the current value associated with a key. The value
    * will reflect the consensus at some time not earlier than invoking the
    * method.
    *
    * @param key the key of the value to retrieve
    * @return a future to the value associated with the key. The future
    *         succeeds when a consensual value has been established.
    */
  def get(key: K): Future[Option[V]] = syncThen { getUnsynced(key) }

  /**
    * Returns an immutable current view of the underlying entries.
    *
    * @return an immutable current view of the underlying entries.
    */
  def entries: Future[Map[K, V]] = syncThen { map }

  /**
    * Returns an immutable view of the underlying entries without synchronizing
    * with the consensus cluster. The view reflects some past consensual state
    * but there is no guarantee it is current.
    *
    * @return an immutable view of the underlying entries.
    */
  def unsyncedEntries: Map[K, V] = map

  /**
    * Returns a future that will be fulfilled when the map's state has changes
    * applied to it at least until the specified TX.
    * @param atLeast the minimum TX for which changes need to be applied to the
    *                map's state.
    * @return a future that completes when the changes have been applied to the
    *         map's state at least until the specified TX. The future completes
    *         with the map itself for easy chaining.
    */
  def poll(atLeast: TX): Future[ReplicatedMap[K, V]] = doThen { Future.successful(atLeast) } { this }

  /**
    * Atomically sets the value associated with the key to the given updated
    * value if the current value equals the expected value.
    *
    * @param key the key associated with the value
    * @param expect the expected value
    * @param update the update value
    * @return a future representing either success or failure or setting the value
    *         after reaching consensus.
    */
  def compareAndSet(key: K, expect: Option[V], update: Option[V]): Future[Boolean] = {
    doThen { log.add(CompareAndSet(key, expect, update), timeout) } { getUnsynced(key) == update }
  }

  private def syncThen[T](action: => T) = doThen { log.sync(timeout) } { action }

  private def doThen[T](firstAction: => Future[TX])(secondAction: => T) =
    firstAction flatMap { tx =>
      /* catch up the log until relevant TX */
      log.poll(tx - 1, timeout)
    } map {
      case true =>
        applyCommitted()
        secondAction
      case false =>
        throw new TimeoutException()
    }

  private def applyCommitted() = {
    val until = log.lastIdx
    synchronized {
      if (_lastApplied < until) {
        map = (log.entries(_lastApplied) takeWhile { _.idx <= until } foldLeft map) { (newMap, entry) =>
          entry match {
            case LogEntry(_, Some(CompareAndSet(key, expect, update))) if newMap.get(key) == expect =>
              update match {
                case None    => newMap - key
                case Some(v) => newMap.updated(key, v)
              }
            case _ => newMap
          }
        }
        _lastApplied = until
      }
    }
  }
}
