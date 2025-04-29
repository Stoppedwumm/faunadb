package fauna.model.stream

import fauna.atoms._
import fauna.auth._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks._
import fauna.lang.syntax._
import fauna.logging._
import fauna.model._
import fauna.repo._
import fauna.repo.query._
import fauna.repo.service.stream.{ Coordinator => StreamCoordinator, _ }
import fauna.snowflake.IDSource
import fauna.stats._
import io.netty.buffer.ByteBuf
import scala.concurrent.{ Future, TimeoutException }
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.Failure

object StreamRenderContext {

  val QueryTimeout = 1.minute
  val LogMetricsInterval = 1.minute
  val EventSeparator = "\r\n".toUTF8Buf.asReadOnly()

  private final class AuthorizationLost(
    val txnTS: Timestamp,
    val dispatchTS: Timestamp)
      extends NoStackTrace

  private lazy val streamTimer = new Timer(tickMillis = 500, ticksPerWheel = 512)
  private lazy val streamClock = new MonotonicClock(Clock)

  def render(
    auth: Auth,
    version: APIVersion,
    fields: Set[EventField],
    ctx: StreamContext,
    stream: Observable[TxnEvents]): (StreamID, Observable[ByteBuf]) = {

    val streamID = ctx.nextID

    val buffs =
      new StreamRenderContext(
        ctx.repo,
        ctx.stats,
        ctx.idGen,
        auth,
        version,
        fields,
        streamID,
        stream,
        ctx.logEvents
      ).render()

    (streamID, buffs)
  }
}

final class StreamRenderContext(
  repo: RepoContext,
  stats: StatsRecorder,
  idSource: IDSource,
  auth: Auth,
  version: APIVersion,
  fields: Set[EventField],
  streamID: StreamID,
  stream: Observable[TxnEvents],
  logEvents: Boolean = false)
    extends StreamLogging
    with ExceptionLogging {
  import StreamRenderContext._

  private implicit val ec = ImmediateExecutionContext
  @volatile private[this] var streamTime: Timestamp = _
  @volatile private[this] var truncateMetrics = true
  @volatile private[this] var started = false

  private[this] lazy val authInfo =
    runQuery(AuthRender.render(auth)) andThen { case Failure(err) =>
      logException(err)
    }

  private[this] lazy val reporter =
    Option.when(logEvents) {
      StatsRequestBuffer.Reporter(StreamMetrics.bufferedReporter) {
        reportMetrics(_)
      }
    }

  private[this] lazy val globalIDPath =
    auth.database.globalIDPath.map(Database.encodeGlobalID(_))

  private def reportMetrics(buffer: StatsRequestBuffer): Unit =
    if (streamTime ne null) {
      authInfo foreach { auth =>
        val now = streamClock.time
        val period = now difference streamTime
        val metrics = StreamMetrics(buffer, period, truncateMetrics)
        streamTime = now - metrics.remainingPeriod
        logEvent(
          idSource.getID,
          streamID,
          auth,
          metrics,
          version = "4",
          globalIDPath = globalIDPath)
      }
    }

  def render(): Observable[ByteBuf] =
    stream flatMapAsync { events =>
      handle(events) map { obs =>
        obs ensure {
          val now = streamClock.time
          // the event dispatch time is computed using unsynchronized clocks
          val dispatchTimeMillis = now.difference(events.dispatchTS).toMillis
          if (dispatchTimeMillis > 0) {
            repo.stats.timing("Streams.Dispatch.Time", dispatchTimeMillis)
          }
        }
      }
    } recoverAsync { err =>
      logException(handleError(err))
    } ensure {
      reporter foreach { reporter =>
        truncateMetrics = false
        reporter.flush()
      }
    }

  private def handle(events: TxnEvents): Future[Observable[ByteBuf]] = {
    if (!started) {
      started = true
      reporter foreach { reporter =>
        streamTime = streamClock.time
        streamTimer.scheduleRepeatedly(LogMetricsInterval, truncateMetrics) {
          reporter.flush()
        }
      }
    }

    renderTxnEvents(events)
  }

  private def handleError(cause: Throwable): Future[Observable[ByteBuf]] = {
    cause match {
      case e: Cancelable.Canceled =>
        Future.successful(Observable.failed(e)) // propagate

      case _: TimeoutException =>
        renderTxnEvents(TxnEvents(StreamError.RequestTimeout), checkAuth = false)

      case StreamMerger.SyncTimeout =>
        renderTxnEvents(
          TxnEvents(
            StreamError.ServiceTimeout("Stream timed out while synchronizing.")),
          checkAuth = false)

      case Publisher.QueueFull(maxSize) =>
        renderTxnEvents(
          TxnEvents(StreamError.ServiceTimeout(
            s"Stream closed due to congestion after accumulating $maxSize items.")),
          checkAuth = false)

      case _: Sink.ClosedBySource =>
        renderTxnEvents(
          TxnEvents(
            StreamError.ServiceTimeout(
              "Stream was unexpectedly closed by its source.")),
          checkAuth = false)

      case _: Sink.SinkClosed =>
        renderTxnEvents(
          TxnEvents(
            StreamError.ServiceTimeout("Subscribing to an already closed stream.")),
          checkAuth = false)

      case _: StreamCoordinator.MaxOpenStreamsExceeded =>
        renderTxnEvents(TxnEvents(StreamError.TooManyRequests), checkAuth = false)

      case e: AuthorizationLost =>
        renderTxnEvents(
          TxnEvents(e.txnTS, e.dispatchTS, StreamError.PermissionDenied),
          checkAuth = false
        )

      case other =>
        renderTxnEvents(
          TxnEvents(StreamError.InternalServerError(other)),
          checkAuth = false
        )
    }
  }

  private def renderTxnEvents(
    events: TxnEvents,
    checkAuth: Boolean = true): Future[Observable[ByteBuf]] = {

    val buffersQ =
      processEvents(events.txnTS, events.dispatchTS, events.events, checkAuth)
    runQuery(buffersQ, events.txnTS) map { Observable.from(_) }
  }

  private def processEvents(
    txnTS: Timestamp,
    dispatchTS: Timestamp,
    events: Vector[StreamEvent],
    checkAuth: Boolean): Query[Iterable[ByteBuf]] = {

    val encoder = EventEncoder(txnTS, fields)

    RenderContext(auth, version, txnTS) flatMap { ctx =>
      if (checkAuth) {
        Auth.revalidate(auth) flatMap { authValid =>
          if (authValid) {
            renderEvents(ctx, encoder, events)
          } else {
            Query.fail(new AuthorizationLost(txnTS, dispatchTS))
          }
        }
      } else {
        renderEvents(ctx, encoder, events)
      }
    }
  }

  private def renderEvents(
    ctx: RenderContext,
    encoder: EventEncoder,
    events: Vector[StreamEvent]): Query[Iterable[ByteBuf]] = {

    val filteredQ =
      Query.stats flatMap { stats =>
        stats.count(StreamMetrics.Events, events.size)
        Query.value(events) selectMT { EventFilter.check(auth, _) }
      }

    filteredQ flatMapT { event =>
      val literal = encoder.encode(event)
      ctx.render(literal) map { buf =>
        buf.writeBytes(EventSeparator.duplicate())
        Iterable.single(buf)
      }
    }
  }

  private def runQuery[A](
    resultQ: Query[A],
    txnTS: Timestamp = streamClock.time,
    deadline: TimeBound = QueryTimeout.bound): Future[A] = {

    stats.timeFuture("Stream.Query.Time") {
      val resF = reporter match {
        case Some(reporter) =>
          reporter.span releaseAfter { span =>
            val multiStats = StatsRecorder.Multi(Seq(stats, span.buffer))
            repo
              .withStats(multiStats)
              .run(
                resultQ,
                txnTS,
                deadline,
                auth.accountID,
                auth.scopeID,
                auth.limiter)
          }
        case _ =>
          repo
            .withStats(stats)
            .run(
              resultQ,
              txnTS,
              deadline,
              auth.accountID,
              auth.scopeID,
              auth.limiter)
      }
      resF map { case (_, value) => value }
    }
  }
}
