package fauna.repo.service.stream

import fauna.atoms._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks._
import fauna.lang.syntax._
import io.netty.util.{
  AbstractReferenceCounted,
  IllegalReferenceCountException,
  ReferenceCounted
}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.BitSet
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace

/** A stream sink.
  *
  * There is one sink per stream key. Multiple streams can be created with the same
  * sink, however only one instance of the subscription protocol will take place. A
  * sink gets closed once all its observable subscriptions are canceled. Multiple
  * sinks are grouped by source host. A sink group gets closed once every sink in it
  * has been closed.
  *
  * When subscribing to a key, the sink group will keep track of the key's index on
  * the source host. Sink groups periodically synchronize its subscriptions with the
  * source host by sending a bitmap of keys' indexes that the sink is no longer
  * interested, thus allowing the source to interrupt the write flow for those stream
  * keys.
  *
  * Once created, a sink group will hold on to its generation until closed. The sink
  * group generation ensures both sinks and sources are synchronized as sinks discard
  * messages from sources with diverting generation, and sources will stop sending
  * writes to non-synchronized sink generations.
  */
object Sink {

  type LocateFn[K] = K => HostID
  type SubscribeFn[K] = (K, HostID, Generation) => Future[(Timestamp, Source.Index)]
  type SyncFn = (HostID, Generation, Vector[Long]) => Future[Unit]

  private val log = getLogger()

  final class SinkClosed extends NoStackTrace
  final class ClosedBySource extends NoStackTrace

  final case class Generation(toLong: Long) extends AnyVal

  object Generation {
    // Pick a different initial generation at each restart.
    private[this] val generation = new AtomicLong(Clock.time.seconds)
    def next(): Generation = Generation(generation.getAndIncrement())
    implicit val Codec = CBOR.TupleCodec[Generation]
  }

  final class Manager[K, V: ClassTag](
    locate: LocateFn[K],
    subscribe: SubscribeFn[K],
    sync: SyncFn,
    maxBufferSize: Int,
    nextGeneration: => Generation = Generation.next()) {

    private[this] val sinksByKey = new ConcurrentHashMap[K, Sink[K, V]]()
    private[this] val groupBySource = new ConcurrentHashMap[HostID, Group[K, V]]()

    def register(key: K): Observable[V] =
      registerSink(key).watch()

    private def registerSink(key: K): Sink[K, V] = {
      var isNew = false
      val sink = sinksByKey.compute(
        key,
        {
          case (_, null) =>
            isNew = true
            new Sink(key, maxBufferSize)
          case (_, sink) =>
            try {
              sink.retain()
              isNew = false
              sink
            } catch {
              case _: IllegalReferenceCountException =>
                isNew = true
                new Sink(key, maxBufferSize)
            }
        }
      )

      if (isNew) {
        subscribeSink(key, sink)
      }

      sink
    }

    private def subscribeSink(key: K, sink: Sink[K, V]): Unit = {
      implicit val ec = ImmediateExecutionContext
      val sourceHostID = locate(key)
      val group =
        groupBySource.computeIfAbsent(
          sourceHostID,
          _ => new Group(nextGeneration)
        )

      subscribe(key, sourceHostID, group.generation) andThen {
        case Success((_, index)) => group.registerSink(key, sink, index)
        case Failure(err)        => sink.close(err)
      }
    }

    def receive(
      sourceHostID: HostID,
      generation: Generation,
      lastSentTS: Timestamp,
      currentTS: Timestamp,
      key: K,
      value: V): Unit = {

      Option(groupBySource.get(sourceHostID)) foreach { group =>
        if (group.generation == generation) {
          group.receive(lastSentTS, currentTS, key, value)
        }
      }
    }

    def synchronize(): Unit =
      groupBySource.forEach((sourceHostID, group) => {
        implicit val ec = ImmediateExecutionContext
        sync(sourceHostID, group.generation, group.mask()) andThen {
          case Failure(_) =>
            groupBySource.remove(sourceHostID, group)
            group.close(new ClosedBySource())
        }
      })

    def close(): Unit =
      groupBySource.forEach((_, group) => {
        group.close()
      })
  }

  private final class Group[K, V](val generation: Generation) {

    private[this] val sinks = MMap.empty[K, (Sink[K, V], Source.Index)]
    private[this] val removals = new BitSet()
    private[this] var closed = false

    def receive(
      lastSentTS: Timestamp,
      currentTS: Timestamp,
      key: K,
      value: V): Unit =
      synchronized {
        sinks.get(key) foreach { case (sink, index) =>
          if (!sink.receive(lastSentTS, currentTS, value)) {
            removals.set(index.toInt)
            sinks.remove(key)
          }
        }
      }

    def registerSink(key: K, sink: Sink[K, V], index: Source.Index): Unit =
      synchronized {
        if (closed) {
          sink.close(new SinkClosed())
        } else {
          removals.clear(index.toInt)
          sinks.put(key, (sink, index)) foreach { case (oldSink, oldIdx) =>
            removals.set(oldIdx.toInt)
            oldSink.close(new IllegalStateException("Duplicated sink."))
          }
        }
      }

    def mask(): Vector[Long] =
      synchronized {
        val vec = removals.toLongArray.toVector
        removals.clear()
        vec
      }

    def close(err: Throwable = null): Unit =
      synchronized {
        closed = true
        sinks foreach { case (_, (sink, _)) =>
          sink.close(err)
        }
        sinks.clear()
      }
  }
}

final class Sink[K, V: ClassTag](key: K, maxBufferSize: Int)
    extends AbstractReferenceCounted {

  private[this] val buffer = new AsyncRingBuffer[V](maxBufferSize)
  private[this] var lastSeenTS: Timestamp = _

  def watch(): Observable[V] =
    buffer ensure release()

  def receive(lastSentTS: Timestamp, currentTS: Timestamp, value: V): Boolean =
    synchronized {
      if ((lastSeenTS eq null) || lastSeenTS == lastSentTS) {
        lastSeenTS = currentTS
        buffer.publish(value)
      } else {
        Sink.log.warn(
          s"Closing sink due to out-of-order event: " +
            s"key:${key}, " +
            s"lastSeenTS=${lastSeenTS}, " +
            s"lastSentTS=${lastSentTS}")
        close(
          new IllegalStateException("Closed due to missing or out-of-order event."))
        false
      }
    }

  def touch(hint: Any): ReferenceCounted = this

  protected def deallocate(): Unit = close()

  def close(cause: Throwable = null): Unit =
    buffer.close(cause)
}
