package fauna.net.gossip

import fauna.codex.cbor.CBOR
import java.util.concurrent.atomic.AtomicLong

object Lamport {

  object Timestamp {
    implicit lazy val codec: CBOR.Codec[Timestamp] = CBOR.TupleCodec[Timestamp]
  }
  case class Timestamp(ts: Long) extends AnyVal

  def apply() = {
    new Lamport(new AtomicLong)
  }

  def apply(ts: Long) = {
    new Lamport(new AtomicLong(ts))
  }
}

/**
  * Very simple thread-safe lamport clock implementation.
  */
final class Lamport private (private[this] val ts: AtomicLong) {

  /**
    * Supply the current lamport timestamp.
    */
  def apply(): Lamport.Timestamp = Lamport.Timestamp(ts.get)

  /**
    * Increment the current lamport timestamp.
    */
  def increment(): Lamport.Timestamp = Lamport.Timestamp(ts.incrementAndGet)

  /**
    * Witness a lamport timestamp from another event.
    *
    * Ensures this clock has ticked to one more than the witnessed time.
    */
  def witness(ts: Lamport.Timestamp) = {
    val next = ts.ts + 1
    var curr = 0L
    do {
      curr = this.ts.get
    } while (curr < next && !this.ts.compareAndSet(curr, next))
  }
}
