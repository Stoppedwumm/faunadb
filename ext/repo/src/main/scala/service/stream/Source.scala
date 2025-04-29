package fauna.repo.service.stream

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import io.netty.util.{
  AbstractReferenceCounted,
  IllegalReferenceCountException,
  ReferenceCounted,
  Timeout
}
import java.util.{ BitSet, PriorityQueue }
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.{ ArrayBuffer, Map => MMap }
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.Duration

/**
  * A stream source.
  *
  * There is one source per sink host and sink generation pair. A source gets closed
  * automatically after a timeout if it gets no sync message from its sink with a
  * matching generation.
  *
  * Upon registering new stream keys, the source assigns them an index. Sinks will
  * then send a synchronization message containing a bitmap of keys' indexes that are
  * no longer registered at the sink side. To keep the bitmap space efficient over
  * large sets of registered keys, the source re-uses the lower keys after they get
  * released by the sink.
  *
  * After pushing values to a source, it filters the values for which there are
  * stream keys registered at its sink host. When forwarding values to the sink, the
  * source sends the last sent transaction timestamp for each key along with the
  * values so that the sink can identify dropped network messages and act
  * accordingly.
  *
  * Sources keep track of idle keys. Upon publishing values to a source, in addition
  * to sending the last sent transaction timestamp of the values' keys, it also sends
  * the last sent transaction timestamps for a bound set of idle keys. Sinks use
  * these timestamps to detect and synchronize partitioned transactions.
  */
object Source {

  type ForwardFn[K, V] = (HostID, Sink.Generation, Source[K, V]) => Unit

  final case class Output[K, V](
    lastSentTS: Map[K, Timestamp],
    minAppliedTS: Timestamp,
    transactionTS: Timestamp,
    dispatchTS: Timestamp,
    values: Vector[V]
  )

  final case class Index(toInt: Int) extends AnyVal

  object Index {
    implicit val Codec = CBOR.TupleCodec[Index]
  }

  final class Manager[K, V](
    minAppliedTS: () => Timestamp,
    forward: ForwardFn[K, V],
    maxBuffer: Int,
    ttl: Duration,
    idlePeriod: Duration,
    maxIdleOverhead: Int) {

    private[this] val sourcesBySinkHost =
      new ConcurrentHashMap[(HostID, Sink.Generation), Source[K, V]]()

    def register(
      sinkHostID: HostID,
      generation: Sink.Generation,
      key: K): Option[Index] = {

      var isNew = false
      val source = sourcesBySinkHost.compute(
        (sinkHostID, generation),
        {
          case (_, null) =>
            isNew = true
            new Source(minAppliedTS, maxBuffer, ttl, idlePeriod, maxIdleOverhead)
          case (_, source) =>
            isNew = false
            source
        }
      )

      if (isNew) {
        forward(sinkHostID, generation, source)
        source onClose {
          sourcesBySinkHost.remove((sinkHostID, generation), source)
        }
      }

      source.register(key)
    }

    def publish(
      transactionTS: Timestamp,
      dispatchTS: Timestamp,
      valuesByKey: Map[K, Vector[V]]): Unit =
      sourcesBySinkHost.forEach((_, source) => {
        source.publish(transactionTS, dispatchTS, valuesByKey)
      })

    def synchronize(
      sinkHostID: HostID,
      generation: Sink.Generation,
      mask: Vector[Long]): Boolean = {

      Option(sourcesBySinkHost.get((sinkHostID, generation))).fold(false) { source =>
        source.synchronize(mask)
      }
    }

    def close(): Unit =
      sourcesBySinkHost.forEach((_, source) => {
        source.close()
      })
  }

  private final class KeyTracker extends AbstractReferenceCounted {
    @volatile var lastSentTS = Timestamp.Epoch
    def touch(hint: Any): ReferenceCounted = this
    protected def deallocate(): Unit = ()
  }
}

final class Source[K, V](
  minAppliedTS: () => Timestamp,
  maxBuffer: Int,
  ttl: Duration,
  idlePeriod: Duration,
  maxIdleOverhead: Int) {

  private[this] val lock = new ReentrantReadWriteLock()
  private[this] val trackerByKey = MMap.empty[K, Source.KeyTracker]
  private[this] val keySlots = ArrayBuffer.empty[K]
  private[this] val keyIndices = new BitSet()
  private[this] val closeP = Promise[Unit]()
  private[this] var closeTimeout: Timeout = _
  @volatile private[this] var closed = false

  // Java's default capacity is 11.
  private[this] val oldestSentKeys =
    new PriorityQueue[(Timestamp, K)](11, Ordering.by(_._1))

  private val (publisher, observable) = {
    implicit val ec = FaunaExecutionContext.Implicits.global
    Observable.gathering(OverflowStrategy.bounded[Source.Output[K, V]](maxBuffer))
  }

  resetCloseTimeout()
  scheduleIdleNotifications()

  private def resetCloseTimeout(): Unit =
    closeTimeout = Timer.Global.scheduleTimeout(ttl) {
      close()
    }

  private def scheduleIdleNotifications(): Unit = {
    if (idlePeriod.isFinite) {
      Timer.Global.scheduleRepeatedly(idlePeriod, !closed) {
        // NB. Pushes idle key notifications to a separate thread to avoid contention
        // at the timer thread. Scheduling multiple idle key notifications is safe
        // since `notifyIdleKeys` don't block if it fails to aquire a write lock. In
        // practice, there should only be one thread pushing idle key notifications
        // at the time.
        implicit val ec = FaunaExecutionContext.Implicits.global
        Future(notifyIdleKeys())
      }
    }
  }

  def output: Observable[Source.Output[K, V]] = observable

  def keys: Set[K] = {
    lock.readLock().lock()
    try {
      trackerByKey.keySet.toSet
    } finally {
      lock.readLock().unlock()
    }
  }

  def publish(
    transactionTS: Timestamp,
    dispatchTS: Timestamp,
    valuesByKey: Map[K, Vector[V]]): Unit = {

    val filtered = Vector.newBuilder[V]
    val lastSent = Map.newBuilder[K, Timestamp]

    // Multiple threads applying transactions will race to call `publish`. It is safe
    // to process their contents in parallel as data-nodes will sequence transactions
    // touching the same keys.
    lock.readLock().lock()
    try {
      valuesByKey foreach {
        case (key, values) =>
          trackerByKey.get(key) foreach { tracker =>
            val lastSentTS = tracker.lastSentTS
            tracker.lastSentTS = transactionTS
            lastSent += key -> lastSentTS
            filtered ++= values
            if (idlePeriod.isFinite) {
              oldestSentKeys.add((transactionTS, key))
            }
          }
      }
    } finally {
      lock.readLock().unlock()
    }

    // TODO: cleanup these checks once idle tracking is enabled by default.
    if (idlePeriod.isFinite) {
      lock.writeLock().lock()
      try {
        val appliedTS = minAppliedTS()
        lastSent ++= gatherIdleKeys(appliedTS)

        val toSend = lastSent.result()
        if (toSend.nonEmpty) {
          val output =
            Source.Output(
              toSend,
              appliedTS,
              transactionTS,
              dispatchTS,
              filtered.result()
            )
          publisher.publish(output)
        }
      } finally {
        lock.writeLock().unlock()
      }
    } else {
      val toSend = lastSent.result()
      if (toSend.nonEmpty) {
        val output =
          Source.Output(
            toSend,
            minAppliedTS(),
            transactionTS,
            dispatchTS,
            filtered.result()
          )
        publisher.publish(output)
      }
    }
  }

  private def notifyIdleKeys(): Unit = {
    if (lock.writeLock().tryLock()) { // do not compete with `publish`
      try {
        val appliedTS = minAppliedTS()
        val idles = gatherIdleKeys(appliedTS)
        if (idles.nonEmpty) {
          // NB. Txn time is ignored on idle keys.
          // See Coordinator.recvResult(..).
          val output =
            Source.Output(
              lastSentTS = idles,
              minAppliedTS = appliedTS,
              transactionTS = Timestamp.Epoch,
              dispatchTS = Clock.time,
              values = Vector.empty[V]
            )
          publisher.publish(output)
        }
      } finally {
        lock.writeLock().unlock()
      }
    }
  }

  // Mutual exclusion is required when draining idle keys since racing to update
  // their last sent timestamp could produce unordered outputs for the same key which
  // sinks interpret as message loss/re-ordering. In other words, this algorithm must
  // ensure that values sent for a given key are in increasing order with regards to
  // the last sent transaction timestamp.
  private def gatherIdleKeys(appliedTS: Timestamp): Map[K, Timestamp] = {
    val idles = Map.newBuilder[K, Timestamp]
    val idleMark = appliedTS - idlePeriod
    var slots = maxIdleOverhead

    while (slots > 0 && !oldestSentKeys.isEmpty) {
      val (oldestSentTS, key) = oldestSentKeys.peek
      if (oldestSentTS >= idleMark) {
        slots = 0 // no more idle keys to drain. stop.
      } else {
        oldestSentKeys.poll()
        trackerByKey.get(key) foreach { tracker =>
          if (tracker.lastSentTS == oldestSentTS) {
            // Uses the last applied timestamp to ensure that no concurrent
            // transaction application has a lower timestamp so that an idle key
            // notification would mislead sinks into thinking that there was a
            // message loss/re-ordering.
            oldestSentKeys.add((appliedTS, key))
            tracker.lastSentTS = appliedTS
            idles += key -> oldestSentTS
            slots -= 1
          }
        }
      }
    }

    idles.result()
  }

  def register(key: K): Option[Source.Index] = {
    def register0() = {
      val isRegistered =
        trackerByKey.get(key) match {
          case None => false
          case Some(tracker) =>
            try {
              tracker.retain()
              true
            } catch {
              case _: IllegalReferenceCountException =>
                false
            }
        }

      if (!isRegistered) {
        val tracker = new Source.KeyTracker()
        trackerByKey.put(key, tracker)
        if (idlePeriod.isFinite) {
          oldestSentKeys.add((tracker.lastSentTS, key))
        }
      }

      val index = keyIndices.nextClearBit(0)
      keyIndices.set(index)

      if (index < keySlots.length) {
        keySlots(index) = key
      } else {
        keySlots += key
      }

      Source.Index(index)
    }

    lock.writeLock().lock()
    try {
      Option.when(!closed) {
        register0()
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  def synchronize(mask: Vector[Long]): Boolean = {
    def synchronize0(): Unit = {
      val indexesToRemove = BitSet.valueOf(mask.toArray).stream()
      indexesToRemove.forEach(idx => {
        if (idx < keySlots.length) {
          val key = keySlots(idx)
          keySlots(idx) = null.asInstanceOf[K]
          keyIndices.clear(idx)
          if (trackerByKey(key).release()) {
            trackerByKey.remove(key)
          }
        }
      })
    }

    if (closeTimeout.cancel()) {
      lock.writeLock().lock()
      try {
        synchronize0()
      } finally {
        lock.writeLock().unlock()
      }
      resetCloseTimeout()
      true
    } else {
      false
    }
  }

  def onClose(fn: => Any): Unit = {
    implicit val ec = ImmediateExecutionContext
    closeP.future ensure { fn }
  }

  def close(): Unit = {
    lock.writeLock().lock()
    try {
      closed = true
    } finally {
      lock.writeLock().unlock()
    }
    publisher.close()
    closeP.setDone()
  }
}
