package fauna.tx

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.net.bus.{ HandlerID, Protocol, SignalID }
import fauna.trace._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.Arrays

package transaction {
  final case class SegmentID(toInt: Int) extends AnyVal with Ordered[SegmentID] {
    def compare(o: SegmentID) = Integer.compare(toInt, o.toInt)
    def max(o: SegmentID) = if (toInt >= o.toInt) this else o
  }

  object SegmentID {
    implicit val CBORCodec = CBOR.TupleCodec[SegmentID]
  }

  final case class Transaction[W](ts: Timestamp, expr: W, origin: HandlerID, trace: Option[TraceContext])

  /**
   * An encodeable filter. LogNodes and ScopeBrokers use this to filter transactions into
   * associated streams.
   */
  trait DataFilter[W] {
    def covers(expr: W): Boolean
  }

  /** A stream handler `ScopeBroker` will use to pass transactions to the stream
    * coordinator.
    */
  trait DataStream[W, WV] {

    /** A log node filder for streams subscribing to log nodes. `None` switches the
      * stream to subscribing to data-node transactions only.
      */
    def filter: Option[DataFilter[W]]

    /** A callback for when txns are send to the data-node even though it doesn't
      * participate in its execution. When using a log-node filter, these can be txns
      * sent to data-node that match the filter. Note that during topology changes
      * some txns may hit this callback as nodes segment ownership changes. Make sure
      * all txns in the callback match the filter before handling them.
      */
    def onUncovered(txn: Transaction[W]): Unit

    /** A txn is being schedulued by the `ScopeBroker`. Scheduling happens in a single
      * thread and in txn ts order.
      */
    def onSchedule(txn: Transaction[W]): Unit

    /** A scheduled txn has been executed. Txn execution happens in parallel. */
    def onResult(txn: Transaction[W], result: Option[WV]): Unit
  }

  // Batches

  final case class Batch(
    epoch: Epoch,
    scope: ScopeID,
    txns: Vector[Batch.Txn],
    count: Int) {

    def ceilTimestamp = epoch.ceilTimestamp

    def transactions[W: CBOR.Codec] = txns map {
      case Batch.Txn(e, origin, _, pos, trace) =>
        val ts = epoch.timestamp(pos, count)
        Transaction(ts, CBOR.parse(e), origin, trace)
    }
  }

  object Batch {

    final case class Txn(
      expr: Array[Byte],
      origin: HandlerID,
      expiry: Timestamp,
      pos: Int = 0,
      trace: Option[TraceContext] = None) {

      override def equals(o: Any) = o match {
        case o: Txn => origin == o.origin && expiry == o.expiry && pos == o.pos && Arrays.equals(expr, o.expr)
        case _      => false
      }

      override def hashCode = origin.hashCode * expiry.hashCode * pos.hashCode

      // array byte + expr + handler + ts + pos
      def bytesSize = 1 + 4 + expr.length + 16 + 16 + 4

      override def toString = s"Txn(${expr.toList},$origin,$expiry,$pos)"
    }

    object Txn {
      implicit val TxnCodec = CBOR.TupleCodec[Txn]
    }

    implicit val CBORCodec = CBOR.TupleCodec[Batch]
  }

  // Message Types

  sealed trait CoordMessage

  object CoordMessage {
    final case class TxnCommitted(
      result: Boolean,
      backoff: Boolean,
      leader: Option[HostID])
        extends CoordMessage

    final case class TxnApplied(
      ts: Timestamp,
      result: Array[Byte],
      digest: Array[Byte])
        extends CoordMessage

    implicit val CBORCodec =
      CBOR.SumCodec[CoordMessage](
        CBOR.TupleCodec[TxnCommitted],
        CBOR.TupleCodec[TxnApplied]
      )
  }

  sealed trait LogMessage

  object LogMessage {
    final case class GetBatches(segment: SegmentID, pvers: Long, after: Epoch, limit: Option[ScopeID], filters: ByteBuf) extends LogMessage

    // FIXME: `pvers` is unused
    final case class AcceptBatches(pvers: Long, acc: Epoch, filters: ByteBuf) extends LogMessage

    final case class AddTxn(
      scope: ScopeID,
      txn: Batch.Txn,
      occReads: Int,
      priority: GlobalID)
        extends LogMessage

    final case class LeaderNotification(segment: SegmentID, leader: HostID) extends LogMessage
    final case class FilterAction(filter: ByteBuf, isAdd: Boolean) extends LogMessage

    // Placeholder for no longer used command in SumCodec
    final case object Unused extends LogMessage

    /**
      * Request type for fetching the leader of a Segment.
      *
      * TODO - Can these messages also go to other Nodes that are not Log nodes? Need to test.
      */
    final case class GetLeader(signalID: SignalID) extends LogMessage
    final case class GetLeaderResponse(segmentID: SegmentID,
                                       leader: Option[HostID])

    implicit val getLeaderResponseCodec = CBOR.TupleCodec[GetLeaderResponse]

    implicit val GetLeaderReplyProtocol = Protocol.Reply[LogMessage.GetLeader, Option[GetLeaderResponse]]("tx.getleader.reply")

    val GetBatchesCodec: CBOR.Codec[GetBatches] =
      new CBOR.PartialCodec[GetBatches]("Array") {
        private val longCodec = implicitly[CBOR.Codec[Long]]
        private val epochCodec = implicitly[CBOR.Codec[Epoch]]
        private val limitCodec = implicitly[CBOR.Codec[Option[ScopeID]]]
        private val segCodec = implicitly[CBOR.Codec[SegmentID]]
        private val filtersCodec = implicitly[CBOR.Codec[ByteBuf]]

        override def readArrayStart(length: Long, stream: CBOR.In) = {
          val gb = GetBatches(segCodec.decode(stream),
                              longCodec.decode(stream),
                              epochCodec.decode(stream),
                              limitCodec.decode(stream),
                              Unpooled.EMPTY_BUFFER)
          length match {
            case 4 => gb
            case 5 => gb.copy(filters = filtersCodec.decode(stream))
            case i => throw new IllegalStateException(s"Invalid length $i.")
          }
        }

        def encode(stream: CBOR.Out, gb: GetBatches) = {
          val hasFilters = gb.filters.isReadable
          if (hasFilters) {
            stream.writeArrayStart(5)
          } else {
            stream.writeArrayStart(4)
          }
          segCodec.encode(stream, gb.segment)
          longCodec.encode(stream, gb.pvers)
          epochCodec.encode(stream, gb.after)
          limitCodec.encode(stream, gb.limit)
          if (hasFilters) {
            filtersCodec.encode(stream, gb.filters)
          } else {
            stream
          }
        }
      }

    val AcceptBatchesCodec: CBOR.Codec[AcceptBatches] =
      new CBOR.PartialCodec[AcceptBatches]("Array") {
        private val longCodec = implicitly[CBOR.Codec[Long]]
        private val epochCodec = implicitly[CBOR.Codec[Epoch]]
        private val filtersCodec = implicitly[CBOR.Codec[ByteBuf]]

        override def readArrayStart(length: Long, stream: CBOR.In) = {
          val pvers = longCodec.decode(stream)
          val epoch = epochCodec.decode(stream)
          val filters = length match {
            case 2 => Unpooled.EMPTY_BUFFER
            case 3 => filtersCodec.decode(stream)
            case i => throw new IllegalStateException(s"Invalid length $i.")
          }
          AcceptBatches(pvers, epoch, filters)
        }

        def encode(stream: CBOR.Out, ab: AcceptBatches) = {
          val hasFilters = ab.filters.isReadable
          if (hasFilters) {
            stream.writeArrayStart(3)
          } else {
            stream.writeArrayStart(2)
          }
          longCodec.encode(stream, ab.pvers)
          epochCodec.encode(stream, ab.acc)
          if (hasFilters) {
            filtersCodec.encode(stream, ab.filters)
          } else {
            stream
          }
        }
      }

    implicit val LogMessageCodec = CBOR.SumCodec[LogMessage](
      AcceptBatchesCodec,
      GetBatchesCodec,
      CBOR.DefunctCodec(Unused),
      CBOR.TupleCodec[AddTxn],
      CBOR.TupleCodec[GetLeader],
      CBOR.TupleCodec[LeaderNotification],
      CBOR.TupleCodec[FilterAction])
  }

  sealed trait DataMessage

  object DataMessage {
    // replies from Log nodes for GetBatches
    final case class Reinit(segment: SegmentID, prev: Epoch, last: Epoch) extends DataMessage

    // FIXME: `limit` is unused
    final case class Batches(segment: SegmentID, pvers: Long, latest: Epoch, limit: Option[ScopeID], prev: Epoch, last: Epoch, batches: Vector[Batch]) extends DataMessage
    final case class Read(
      scope: ScopeID,
      read: Array[Byte],
      ts: Timestamp,
      token: Option[Long],
      version: Long)
        extends DataMessage

    //FIXME: None represents some sort of error.
    final case class ReadResult(
      scope: ScopeID,
      read: Array[Byte],
      ts: Timestamp,
      value: Option[Array[Byte]],
      token: Option[Long],
      version: Long)
        extends DataMessage

    implicit val CBORCodec =
      CBOR.SumCodec[DataMessage](
        CBOR.TupleCodec[Batches],
        CBOR.TupleCodec[Reinit],
        CBOR.TupleCodec[Read],
        CBOR.TupleCodec[ReadResult])
  }
}

package object transaction {

  /**
    * Function type for generating a state reset transaction when applying the
    * original transaction W resulted in mismatches. The coordinator will submit
    * a reset transaction in order to repair underlying replicated state. The
    * returned reset transaction must not do any reads within the transaction
    * and instead blindly write state. This will allow it to consistently apply
    * across all replicas even if their states have previously diverged.
    */
  type MismatchResetTxnFactory[W] = (W => Option[W])

  // FIXME: another one of these...
  private[transaction] def show(obj: Any): String =
    obj match {
      case (a, b)       => s"(${show(a)},${show(b)})"
      case (a, b, c)    => s"(${show(a)},${show(b)},${show(c)})"
      case s: Set[_]    => (s.toList map show).mkString("Set(", ",", ")")
      case m: Map[_,_]  => (m.toList map { case (k, v) => s"${show(k)} -> ${show(v)}" }).mkString("Map(", ",", ")")
      case s: Seq[_]    => (s.toList map show).mkString("Seq(", ",", ")")
      case buf: ByteBuf => CBOR.showBuffer(buf)
      case _ => obj.toString
    }
}
