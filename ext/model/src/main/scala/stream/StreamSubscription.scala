package fauna.model.stream

import fauna.ast._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.model.{ DocumentsSet, IndexSet }
import fauna.repo.service.stream.TxnResult

final case class TxnEvents(txnTS: Timestamp, dispatchTS: Timestamp, events: Vector[StreamEvent])

object TxnEvents {

  def apply(txnTS: Timestamp, dispatchTS: Timestamp, event: StreamEvent): TxnEvents =
    new TxnEvents(txnTS, dispatchTS, Vector(event))

  def apply(event: StreamEvent): TxnEvents =
    new TxnEvents(Timestamp.Epoch, Clock.time, Vector(event))
}

object StreamSubscription {

  def apply(ctx: StreamContext, streamable: StreamableL): StreamSubscription = {
    val (stream, isPartitioned) = subscribe(ctx, streamable)
    new StreamSubscription(stream, isPartitioned)
  }

  private def subscribe(
    ctx: StreamContext,
    streamable: StreamableL): (Observable[TxnResult], Boolean) = {

    def subscribeToSet(set: IndexSet) = {
      val stream = ctx.service.forIndex(set.config, set.terms)
      (stream, set.config.isPartitioned)
    }

    streamable match {
      case RefL(scope, id) => (ctx.service.forDocument(scope, id), false)
      case VersionL(v, _)  => (ctx.service.forDocument(v.parentScopeID, v.id), false)
      case SetL(set) =>
        val set0 = set match {
          case s: DocumentsSet => s.set
          case s               => s
        }
        set0 match {
          case s: IndexSet => subscribeToSet(s)
          case o =>
            throw new IllegalArgumentException(s"Non streamable event set $o")
        }
    }
  }
}

final class StreamSubscription(
  stream: Observable[TxnResult],
  isPartitioned: Boolean) {

  @volatile private var started = false

  def events: Observable[TxnEvents] =
    stream map { res =>
      if (!started) {
        started = true
        startEvent(res.txnTS, res.dispatchTS)
      } else {
        txnEvents(res)
      }
    }

  private def startEvent(startTS: Timestamp, dispatchTS: Timestamp): TxnEvents = {
    val event = StreamStart(startTS)
    TxnEvents(startTS, dispatchTS, Vector(event))
  }

  private def txnEvents(result: TxnResult): TxnEvents =
    TxnEvents(
      result.txnTS,
      result.dispatchTS,
      result.writes map {
        StreamEvent(result.txnTS, _, isPartitioned)
      }
    )
}
