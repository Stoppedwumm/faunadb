package fauna.model.runtime.stream

import fauna.atoms.{ AccountID, ScopeID }
import fauna.auth.{
  AdminPermissions,
  Auth,
  AuthRender,
  ServerPermissions,
  ServerReadOnlyPermissions
}
import fauna.codex.json.JSValue
import fauna.exec.{
  Cancelable,
  ImmediateExecutionContext,
  Observable,
  Observer,
  Publisher
}
import fauna.lang.clocks.{ Clock, MonotonicClock }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging._
import fauna.model.{ Collection, Database }
import fauna.model.runtime.fql2.{ IndexSet, SingletonSet }
import fauna.model.stream.{ StreamContext, StreamID, StreamLogging }
import fauna.repo.query.Query
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import fauna.repo.service.stream.{ Sink, TxnResult }
import fauna.repo.values.Value
import fauna.repo.PagedQuery
import fauna.stats.StreamMetrics
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NoStackTrace

object Subscription {
  val StreamQueryTimeout = 10.seconds
  val LogMetricsInterval = 1.minute
  val BatchSize = 64

  private type ReplayFn =
    (
      Value.EventSource.Cursor,
      Timestamp,
      Int) => PagedQuery[Iterable[Event.Processed]]

  private lazy val StreamClock = new MonotonicClock(Clock)
  private case object PermissionLoss extends Exception with NoStackTrace
  private val sharedtranslations = new SharedTranslation[Event.Processed]
}

final class Subscription(
  auth: Auth,
  ctx: StreamContext,
  source: Value.EventSource,
  statusEventInterval: Duration = 5.minutes,
  streamClock: Clock = Subscription.StreamClock
) extends StreamLogging
    with ExceptionLogging {
  import Subscription._

  if (statusEventInterval >= Collection.MVTOffset) {
    throw new IllegalStateException(
      "Status event interval must be less than MVT Offset. " +
        s"statusEventInterval=$statusEventInterval, MVTOffset=${Collection.MVTOffset}")
  }

  val streamID: StreamID = ctx.nextID

  /** Used to track the compute ops accrued for keeping the stream open.
    * This will be reported on status events.
    */
  private val accruedComputeOps = new AtomicLong(0)

  @volatile private var running = false
  @volatile private var streamOpenTime: Timestamp = _
  @volatile private var lastStatsEvent: Timestamp = _
  @volatile private var lastLoggedMetrics: Timestamp = _

  private implicit val ec = ImmediateExecutionContext

  private lazy val authInfo =
    logException(
      ctx.repo
        .run(
          AuthRender.render(auth),
          streamClock.time,
          StreamQueryTimeout.bound,
          AccountID.Root,
          ScopeID.RootID,
          PermissiveOpsLimiter
        )
        .map { _._2 }
    )

  private lazy val globalIDPath =
    auth.database.globalIDPath.map(Database.encodeGlobalID(_))

  private lazy val metricsReporter =
    EventMetricsRecorder { (now, metrics, running) =>
      authInfo map { reportMetrics(_, now, metrics, running) }
    }

  // NB. The amount of time between internal idle notifications.
  @inline private def idlePeriod = LogMetricsInterval.min(statusEventInterval)

  def subscribe(): Observable[(Event, Event.Metrics)] =
    subscribeAndProcessEvents()
      .transform[(Event, Event.Metrics)] { case (obs, processed) =>
        processed.lift match {
          case Some(tuple) =>
            metricsReporter.recordEvent(processed.metrics)
            processed.toEither match {
              case Right(_) => obs.onNext(tuple)
              case Left(_)  => obs.onNext(tuple).unit before Observer.StopF
            }
          case None =>
            metricsReporter.recordFilteredEvent(processed.metrics)
            Observer.ContinueF
        }
      }
      .recoverAsync {
        case _: Sink.SinkClosed | _: Sink.ClosedBySource | Cancelable.Canceled =>
          // Stream is closing or was canceled by the consumer. Don't return any
          // events, instead, close the stream and let the drivers reconnect.
          Future.successful(Observable.empty)
        case err =>
          onError(err)
      }
      .ensure {
        running = false
        metricsReporter.report(streamClock.time, running)
      }

  private def subscribeAndProcessEvents() = {
    val filter = new EventFilter(source.watchedFields)
    val (set, transformer) = EventTransformer.extract(source.set)

    set match {
      case SingletonSet(doc: Value.Doc) =>
        val translated =
          maybeShareEventTranslation(
            EventTranslator.forDocs(auth, filter, transformer),
            ctx.service.forDocument(
              auth.scopeID,
              doc.id,
              idlePeriod
            )
          )

        processStream(translated) { (cursor, startTS, pageSize) =>
          new EventReplayer(auth, filter, transformer)
            .replay(auth.scopeID, doc.id, cursor, Some(startTS), pageSize)
        }

      case set: IndexSet =>
        val translated =
          maybeShareEventTranslation(
            EventTranslator.forSets(
              auth,
              set.terms,
              set.coveredValues,
              filter,
              transformer
            ),
            ctx.service.forIndex(
              set.config,
              set.terms,
              idlePeriod
            )
          )

        processStream(translated) { (cursor, startTS, pageSize) =>
          new EventReplayer(auth, filter, transformer)
            .replay(set, cursor, Some(startTS), pageSize)
        }

      case other =>
        Observable.failed(
          new IllegalArgumentException(s"invalid index source found: $other"))
    }
  }

  private def maybeShareEventTranslation(
    translator: => EventTranslator,
    obs: => Observable[TxnResult]) = {

    lazy val translator0 = translator // cache by-name value
    def translate(txns: Seq[TxnResult]): Future[Iterable[Event.Processed]] =
      runQ(
        txns.map { translator0.translate(_) }.sequence map {
          _.view.flatten
        }
      )

    // XXX: for now, only streams with static permissions are allowed to share event
    // translation since ABAC's dynamic nature may interfer in the result shape of
    // each event.
    auth.permissions match {
      case AdminPermissions | ServerPermissions | ServerReadOnlyPermissions =>
        val key =
          SharedTranslation.Key(
            auth.scopeID,
            source.set,
            source.watchedFields
          )
        sharedtranslations.share(key, obs.batched(BatchSize), translate)
      case _ =>
        obs.batched(BatchSize) flatMapAsync {
          translate(_) map { Observable.from(_) }
        }
    }
  }

  private def processStream(events: Observable[Event.Processed])(replay: ReplayFn) =
    events flatMapAsync { processed =>
      if (!running) {
        val now = streamClock.time
        streamOpenTime = now
        lastLoggedMetrics = now
        lastStatsEvent = now
        running = true

        source.cursor match {
          case None => Observable.singleF(processed)
          case Some(cursor) =>
            processed.event match {
              case Some(Event.Status(txnTS)) =>
                val pageSize = ctx.eventReplayLimit + 1
                val eventsQ = replay(cursor, txnTS, pageSize)
                runQ(processReplayedEvents(eventsQ, pageSize), txnTS)
              case _ =>
                Observable.singleF(processed)
            }
        }
      } else {
        processed.event match {
          case Some(Event.Status(txnTS)) => Future.successful(onIdle(txnTS))
          case _                         => Observable.singleF(processed)
        }
      }
    }

  private def processReplayedEvents(
    eventsQ: PagedQuery[Iterable[Event.Processed]],
    pageSize: Int): Query[Observable[Event.Processed]] =
    eventsQ
      .takeT(pageSize)
      .flattenT
      .map { events =>
        val events0 =
          if (events.sizeIs <= ctx.eventReplayLimit) {
            events
          } else {
            events :+ Event.Processed(
              Event.Error(
                Event.Error.Code.StreamReplayVolumeExceeded,
                "Too many events to replay. Recreate the stream and try again."),
              Event.Metrics.Empty
            )
          }
        Observable.from(events0)
      }

  private def onIdle(txnTS: Timestamp) = {
    val now = streamClock.time

    if (now.difference(lastLoggedMetrics) >= LogMetricsInterval) {
      lastLoggedMetrics = now
      metricsReporter.report(now, running)
    }

    if (now.difference(lastStatsEvent) >= statusEventInterval) {
      lastStatsEvent = now
      val filteredMetrics = metricsReporter.flushFilteredMetrics()
      Observable.single(
        Event.Processed(
          Event.Status(txnTS),
          Event.Metrics(
            readOps = filteredMetrics.totalReadOps,
            storageBytesRead = filteredMetrics.totalStorageBytesRead,
            computeOps =
              filteredMetrics.totalComputeOps + accruedComputeOps.getAndSet(0),
            processingTime = filteredMetrics.totalProcessingTime,
            rateLimitReadHit = filteredMetrics.rateLimitReadHit,
            rateLimitComputeHit = filteredMetrics.rateLimitComputeHit
          )
        ))
    } else {
      Observable.empty
    }
  }

  private def onError(err: Throwable) = {
    val event = err match {
      case Publisher.QueueFull(_) =>
        Event.Error(Event.Error.Code.StreamOverflow, "Too many events to process.")

      case PermissionLoss =>
        Event.Error(
          Event.Error.Code.PermissionLoss,
          "Stream authentication is no longer valid.")

      case ex =>
        logException(ex)
        Event.Error(
          Event.Error.Code.InternalError,
          s"${ex.getMessage}. Please create a ticket at https://support.fauna.com")
    }

    Observable.singleF((event, Event.Metrics.Empty))
  }

  private def reportMetrics(
    authInfo: JSValue,
    now: Timestamp,
    metrics: EventMetricsAggregate,
    running: Boolean) = {

    val period = now difference streamOpenTime

    val streamMetrics =
      StreamMetrics(
        events = metrics.numEvents,
        filteredEvents = metrics.numFilteredEvents,
        byteReadOps = metrics.totalReadOps,
        storageBytesRead = metrics.totalStorageBytesRead,
        computeOps = metrics.totalComputeOps,
        period = period,
        truncated = running
      )

    val addedComputeOps = streamMetrics.computeOps - metrics.totalComputeOps
    accruedComputeOps.addAndGet(addedComputeOps)

    // NB. Account for truncated reprting periods. See
    // StreamMetrics.calculateComputeCost(..).
    streamOpenTime = now - streamMetrics.remainingPeriod

    logEvent(
      ctx.idGen.getID,
      streamID,
      authInfo,
      streamMetrics,
      version = "10",
      globalIDPath = globalIDPath
    )
  }

  private def runQ[A](
    query: Query[A],
    snapTS: Timestamp = streamClock.time): Future[A] = {
    val query0 =
      Auth.revalidate(auth) flatMap {
        if (_) query else Query.fail(PermissionLoss)
      }

    val resF =
      ctx.repo.run(
        query0,
        snapTS,
        StreamQueryTimeout.bound,
        auth.accountID,
        auth.scopeID,
        auth.limiter
      )

    resF map { case (_, result) => result }
  }
}
