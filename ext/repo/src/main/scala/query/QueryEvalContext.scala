package fauna.repo.query

import fauna.atoms.{ DocID, SchemaVersion, ScopeID }
import fauna.codex.cbor.CBOR
import fauna.exec.{ FaunaExecutionContext, ImmediateExecutionContext }
import fauna.flags.{ PerQueryDocsCacheSize, PerQueryIOCacheSize }
import fauna.lang.{ Local, TimeBound, Timestamp, Timing }
import fauna.lang.syntax._
import fauna.net.RateLimiter
import fauna.repo._
import fauna.repo.service.rateLimits._
import fauna.stats.QueryMetrics
import fauna.trace.GlobalTracer
import scala.collection.{ Set => ISet }
import scala.collection.mutable.{ ArraySeq => MArraySeq, Map => MMap, Set => MSet }
import scala.concurrent.{ Future, TimeoutException }
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

trait QueryContext {

  /** A reference to the immutable environment. Shared across query evaluations. */
  val repo: RepoContext

  /** The snapshot read time of the executing query. */
  val snapshotTime: Timestamp

  /** Force query evaluation to be linearizable. */
  def linearize(): Unit

  /** Ops Rate Limiter to enforce throughput limits */
  def limiter: OpsLimiter

  /** Returns the internal query's read cache. */
  private[repo] def readCache: ReadCache

  /** Safely inspect the state of the underlying cached reads and their results. */
  def inspectReadCache[A](fn: ReadCache.View => A): A

  /** Cache to avoid checking the same docID for index consistency errors multiple
    * times in a query.
    */
  def indexConsistencyCheckDocIDs: ISet[DocID]

  /** Add a docID to the check cache */
  def addIndexConsistencyCheckDocID(id: DocID): Boolean

  /** Used by CacheStore to check the running transaction's schema version state. */
  private[repo] def updateSchemaVersionState(
    scope: ScopeID,
    curr: Option[SchemaVersion]): Unit

  /** Fail the QEC with a fatal exception, short-circuiting as much compute as
    * possible. Recover blocks are still ran. No-op if FF is false.
    */
  def setFatalError(t: Throwable): Unit

  /** Executed before each read I/O is scheduled to ensure at least one
    * permit is available unless the query is not rate limited.
    *
    * See also: Query.checkReadOps()
    */
  protected def preCheckRead(state: State): Future[Unit] =
    if (state.unlimited) {
      Future.unit
    } else {
      limiter.tryAcquireReads(1) match {
        case RateLimiter.DelayUntil(_) =>
          repo.stats.incr(QueryMetrics.RateLimitRead)
          val err = OpsLimitExceptionWithMetrics("read", state.metrics)

          setFatalError(err)
          Future.failed(err)

        case RateLimiter.Acquired(burst) =>
          if (burst) {
            repo.stats.incr(QueryMetrics.RateLimitRead)
          }

          Future.unit
      }
    }
}

object QueryEvalContext {

  sealed trait Cont
  final case class FlatMap(f: Any => Query[Any]) extends Cont
  final case class Catch(f: QTry[Any] => Query[Any]) extends Cont
  final case class Finally(thunk: () => Unit) extends Cont

  type ContStack = List[Cont]

  private[QueryEvalContext] final class AccState(
    val cp: State,
    val slots: MArraySeq[(Query[Any], Done)],
    val seed: Any,
    val fold: (Any, Any) => Any,
    val totalWidth: Int,
    val remaining: Iterator[(Query[Any], Done)],
    val conts: ContStack,
    val w: Wait) {
    var buildIdx: Short = 0
    var pending: Short = 0
  }

  private[QueryEvalContext] type AccStack = List[AccState]

  sealed trait EvalStep
  sealed trait YieldStep extends EvalStep
  final case class Done(qt: QTry[Any], state: State) extends YieldStep
  final case class Wait(var thunk: () => EvalStep, locals: Local.Context)
      extends YieldStep
  final case class Next(
    q: Query[Any],
    state: State,
    conts: ContStack,
    accs: AccStack)
      extends EvalStep

  def eval[A](
    query: Query[A],
    repo: RepoContext,
    snapshotTime: Timestamp,
    deadline: TimeBound,
    txnSizeLimitBytes: Int,
    limiter: OpsLimiter,
    scopeID: ScopeID,
    metrics: State.Metrics = new State.Metrics): Future[(QTry[A], State)] = {
    implicit val ec = FaunaExecutionContext.Implicits.global
    repo.hostFlagsFut().flatMap { flags =>
      val ioCacheSize = flags.get(PerQueryIOCacheSize).toInt
      val docsCacheSize = flags.get(PerQueryDocsCacheSize).toInt

      val ctx =
        new QueryEvalContext(
          repo,
          snapshotTime,
          limiter,
          docsCacheSize,
          ioCacheSize,
          scopeID,
          metrics
        )

      ctx.eval(query, deadline, txnSizeLimitBytes)
    }
  }
}

/** QueryEvalContext is an interpreter for the monadic constructs defined by
  * Query. A QEC is one-shot: once it has evaluated a query it should not be
  * used again. The static `eval` method is the preferred way to create and
  * immediately invoke one.
  *
  * The QEC maintains context for a specific query execution which does not need
  * to change with respect to the query execution plan (such as for failure,
  * recovery, concurrent branching, etc.)
  *
  * The core of QEC's evaluation implementation is in stepEval. See doc comments
  * on the below methods for more details.
  *
  * Eval uses scala Futures to schedule compute and trigger async IO (see the
  * Read trait). The uses below of ImmediateExecutionContext vs global
  * FaunaExecutionContext are worth some explanation:
  *
  * - Calls to stepEval as well as evaluating thunks contained in a Wait step
  *   are scheduled on the global EC, since they are the actual computation of a
  *   Query.
  * - IEC is used to schedule tasks such as trivial transformations, or
  *   signaling a semaphore. This reduces the pressure on the global pool
  *   scheduling mechanism and ensures that signals do not get delayed due to
  *   queueing in the global pool.
  */
final class QueryEvalContext(
  val repo: RepoContext,
  val snapshotTime: Timestamp,
  val limiter: OpsLimiter,
  docsCacheSize: Int,
  ioCacheSize: Int,
  /** Used to log the ScopeID when there are wide queries. This helps
    * to identify problematic queries if wide queries are causing
    * issues.
    */
  scopeID: ScopeID,
  metrics: State.Metrics = new State.Metrics)
    extends QueryContext
    with QueryEvalWait {

  import QueryEvalContext._

  private def tracer = GlobalTracer.instance

  // WARNING: Members below are _not_ threadsafe, relying on the fact that
  // evaluation is effectively single-threaded. Breaking this invariant will
  // lead to data races.
  private[this] var linearized = false
  protected var stepCount = 0
  private[this] val schemaVersions = MMap.empty[ScopeID, Option[SchemaVersion]]
  private[this] val _indexConsistencyCheckDocIDs = MSet.empty[DocID]
  private[this] var globalWidth = 0

  // Queries may abort when encountering a fatal error. It's important to let
  // finalizers execute via `QFrame` evaluation so that side effects aren't lost.
  private[this] var fatalErr: Throwable = _
  private def hasFatalError = fatalErr ne null

  private[repo] val readCache = new ReadCache(docsCacheSize, ioCacheSize)

  // QEC state members

  def linearize(): Unit = linearized = true
  def inspectReadCache[A](fn: ReadCache.View => A) = readCache.inspect(fn)
  def indexConsistencyCheckDocIDs: ISet[DocID] = _indexConsistencyCheckDocIDs
  def addIndexConsistencyCheckDocID(id: DocID): Boolean =
    _indexConsistencyCheckDocIDs.add(id)
  def setFatalError(t: Throwable): Unit = {
    fatalErr = t
  }

  private[repo] def updateSchemaVersionState(
    scope: ScopeID,
    curr: Option[SchemaVersion]): Unit = {
    def newSCE() = SchemaContentionException(scope, curr.get)
    // It's possible the cache pulls a schema snapshot time which is after our own
    // query snapshot. Definitely bail in this case.
    if (repo.schemaOCCEnabled && curr.exists { _.ts > snapshotTime }) {
      throw newSCE()
    } else {
      schemaVersions.get(scope) match {
        case None =>
          schemaVersions(scope) = curr
        case Some(prev) =>
          // if curr is None, then schema has not changed even if it TTLed
          if (repo.schemaOCCEnabled && curr.isDefined && prev != curr) {
            throw newSCE()
          }
        // else do nothing. First seen ts is preserved so it can still be turned
        // into an OCC check on linearized queries. See `postEvalState` below.
      }
    }
  }

  // Evaluation

  final def eval[A](
    query: Query[A],
    deadline: TimeBound,
    txnSizeLimitBytes: Int): Future[(QTry[A], State)] = {

    var computeNanos = 0L
    var waitNanos = 0L

    val seed = State(
      None,
      true,
      ReadsWrites.empty,
      CollectionWrites.empty,
      0,
      repo.stats,
      deadline,
      txnSizeLimitBytes,
      0,
      true,
      Set.empty,
      0,
      false,
      metrics)

    def postEvalState(seed: State): State =
      if (repo.schemaOCCEnabled || linearized) {
        // If we're linearized, force the transaction to go through the log.
        var state = if (linearized) seed.addNoopWrite() else seed
        // And force all schema versions to be occ checked as well.
        schemaVersions.foreach { case (scope, sv) =>
          state = state.addSchemaVersionOccCheck(scope, sv)
        }
        state
      } else {
        seed
      }

    /** Outer trampoline. Reduces an EvalStep to a Future of its Normal Form
      * (i.e. value or error) along with the paired State.
      */
    def eval0(w: Wait): Future[(QTry[Any], State)] = {

      // If the deadline is overdue, set hasTimedOut and record a timeout.
      // stepEval checks this flag below and will convert a QDone into a timeout.
      // This results in an "async" exception throw within the Query monad (ala
      // Haskell's concept of async exceptions), meaning that a timeout may be
      // thrown _any time_ an expression crosses back into the Query monad (e.g. a
      // map or flatMap call). Once this flag is set, the timeout will continue
      // firing: recover and ensure callbacks will be called while the Query's
      // stack unwinds, but any successful result returned by a recover block will
      // be reconverted into a timeout exception as eval continues.
      if (!hasFatalError && deadline.isOverdue) {
        fatalErr = new TimeoutException("Query eval has timed out.")
        repo.stats.incr("Query.Eval.Timeouts")
      }

      stepCount = 0
      val timing = Timing.start

      val yieldstep = {
        // Save off environment Local ctx
        val envLocals = Local.save()

        try {
          // stepWaiters will restore the Local ctx for waiters it steps
          stepWaiters()
          if (hasPendingWaiters) {
            null
          } else {
            // restore the local context associated with w's execution thread
            Local.restore(w.locals)
            w.thunk() match {
              case Next(q, s, cs, accs) => stepEval(q, s, cs, accs)
              case ys: YieldStep        => ys
            }
          }
        } finally {
          Local.restore(envLocals)
        }
      }

      val elapsed = timing.elapsed
      computeNanos += elapsed.toNanos
      repo.stats.timing("Query.Eval.Step.Timing", elapsed.toMillis)

      yieldstep match {
        case null =>
          implicit val ec = FaunaExecutionContext.Implicits.global
          val waitTiming = Timing.start
          wakeFut.flatMap { _ =>
            waitNanos += waitTiming.elapsedNanos
            eval0(w)
          }
        case Done(qt, state) => Future.successful((qt, postEvalState(state)))
        case w: Wait         => eval0(w)
      }
    }

    tracer.withSpan("query.eval") {
      try {
        val totalTiming = Timing.start
        val w = Wait(
          () => Next(query.asInstanceOf[Query[Any]], seed, Nil, Nil),
          Local.save())
        implicit val ec = ImmediateExecutionContext
        eval0(w) transform { res =>
          val totalMillis = totalTiming.elapsedMillis
          val computeMillis = computeNanos / 1_000_000
          val waitMillis = waitNanos / 1_000_000
          val overheadMillis = totalMillis - computeMillis - waitMillis

          repo.stats.timing("Temp.Query.Total.Time", totalMillis)
          repo.stats.timing("Temp.Query.Compute.Time", computeMillis)
          repo.stats.timing("Temp.Query.Wait.Time", waitMillis)
          repo.stats.timing("Temp.Query.Overhead.Time", overheadMillis)
          readCache.reportStats(repo.stats)

          res map {
            case (QDone(a), s) => (QDone(a.asInstanceOf[A]), s)
            case (QFail(t), s) => (QFail(t), s)
          }
        }
      } catch {
        case NonFatal(t) => Future.failed(t)
      }
    }
  }

  /** Reduces a Query and a stack of continuations to either a
    * Future of the next intermediate value+state along with pending
    * continuations, or its final NF + State.
    *
    * The core of `stepEval` is a loop which incrementally destructs a query into
    * an intermediate value, and a stack of continuations. Query variants which
    * need input from the outside world (i.e. QRead, or a wrapped Future) will
    * result in `stepEval` returning a pair of the Future, along with the
    * subsequent continuations to be processed once the Future is complete.
    * `eval` and `stepEval` above are responsible for restarting the loop once the
    * future is completed (the call separation is necessary in order for scala to
    * compile `stepEval` as a tail-recursive function.)
    *
    * `stepEval` implements cooperative sharing of execution threads via the
    * `count` counter, which is incremented as `stepEval` iterates. When the
    * counter exceeds the `queryEvalStepsPerYield` config value, we will yield
    * back to the thread.
    */
  @annotation.tailrec
  private def stepEval(
    query: Query[Any],
    state: State,
    conts: ContStack,
    accs: AccStack): YieldStep = {

    // increment the stepCount
    stepCount += 1

    query match {
      case QFlatMap(q, fn) =>
        stepEval(q, state, FlatMap(fn) :: conts, accs)

      // bail out to the outer trampoline once we hit the step limit. This
      // will allow the async work thread to context-switch.
      case t: QTry[_] if stepCount >= repo.queryEvalStepsPerYield =>
        waitFuture(Future.unit)(_ => Next(t, state, conts, accs))

      case d @ QDone(v) =>
        // fail if we have a fatal error.
        if (hasFatalError) {
          stepEval(QFail(fatalErr), state, conts, accs)
        } else {
          conts match {
            case FlatMap(fn) :: tail =>
              stepEval(QFail.guard(fn(v)), state, tail, accs)
            case Catch(fn) :: tail =>
              stepEval(QFail.guard(fn(d)), state.commit(), tail, accs)
            case Finally(thnk) :: tail =>
              thnk()
              stepEval(d, state, tail, accs)

            // process pending accumulation builds
            case Nil =>
              stepAcc(Done(d, state), accs) match {
                case Next(q, s, cs, as) => stepEval(q, s, cs, as)
                case res: YieldStep     => res
              }
          }
        }

      case f @ QFail(_) =>
        val conts0 = conts.dropWhile {
          // Catch intercepts failure
          case _: Catch => false

          // FlatMap and Finally do not (though Finally runs its thunk as a side
          // effect).
          case _: FlatMap => true
          case Finally(thnk) =>
            thnk()
            true
        }

        conts0 match {
          case Catch(fn) :: tail =>
            stepEval(fn(f), state.commit(omitWrites = true), tail, accs)
          case Nil =>
            stepAcc(Done(f, state), accs) match {
              case Next(q, s, cs, as) => stepEval(q, s, cs, as)
              case res: YieldStep     => res
            }
          case _ =>
            throw new IllegalStateException("unreachable case branch in QFail eval")
        }

      case QFrame(q, fn) =>
        // state.checkpoint paired with state.commit() when consuming the Catch,
        // above
        stepEval(q, state.checkpoint, Catch(fn) :: conts, accs)

      case QState(fn) =>
        val (q, s) =
          try {
            val (v, s) = fn(state)
            (QDone(v), s)
          } catch {
            case NonFatal(t) => (QFail(t), state)
          }
        stepEval(q, s, conts, accs)

      case QContext =>
        stepEval(QDone(this), state, conts, accs)

      case QTrace(name, fn) =>
        tracer.activeSpan match {
          case None =>
            // Short-circuit when no parent span.
            // Presume this query is not traced.
            stepEval(QFail.guard(fn()), state, conts, accs)

          case Some(_) =>
            val span = tracer.buildSpan(name).start()
            val scope = tracer.activate(span)
            def thnk() = scope.foreach { _.close() }
            stepEval(
              QFail.guard(fn()),
              state,
              Finally(thnk _) :: conts,
              accs
            )
        }

      case QAccumulate(qs, seed, fold) =>
        if (qs.isEmpty) {
          stepEval(QDone(seed), state, conts, accs)
        } else {
          val remaining = qs.iterator.map { (_, null: Done) }
          // Accumulate saves Local state to use as the starting state for each
          // branch. This allows the bracketed behavior of tracing to work, but
          // use of Locals expecting Query's "serializable" semantics will fail.
          accInit(state, seed, fold, 0, remaining, conts, Local.save()) match {
            case Left(fail) => stepEval(fail, state, Nil, accs)
            case Right(acc) => stepEval(acc.slots(0)._1, acc.cp, Nil, acc :: accs)
          }
        }

      case QRead(op) =>
        val prefix = state.writesForRowKey(op.rowKey).to(ReadCache.Prefix)
        val relevantWrites = prefix filter { op.isRelevant(_) }

        val skippedOrCached = op.skipIO(relevantWrites) match {
          case Some(res) => Some((Success(res), true))
          case None      => readCache.get(prefix, op).map((_, false))
        }

        skippedOrCached match {
          case Some((Success(res), skipped)) =>
            // we add this here as this won't be hitting the cache due to the
            // io skipping, but we still want to fill the cache with the
            // results
            if (skipped) {
              readCache.maybeFillDocsCache(prefix, op, res)
            }

            val s0 = state.recordRead(
              op.columnFamily,
              op.rowKey,
              res.lastModifiedTS,
              // we don't want to record an OCC enabled read if we skipped io
              // that would end up recording an OCC read with the incorrect
              // timestamp.
              occCheckEnabled = !skipped
            )

            stepEval(QDone(res), s0, conts, accs)

          case Some((Failure(ex), _)) =>
            stepEval(QFail(ex), state, conts, accs)

          case None =>
            def fut = tracer.withSpan("query.read") {
              val started = state.startRead()
              var miss = false

              implicit val ec = ImmediateExecutionContext
              val f = readCache.getOrLoad(prefix, op) {
                miss = true
                preCheckRead(state) flatMap { _ =>
                  repo.keyspace.read(
                    repo.priorityGroup,
                    op,
                    relevantWrites,
                    started.deadline min repo.newReadTimeout.bound)
                }
              }

              f.map { res =>
                tracer.activeSpan foreach { span =>
                  span.addAttribute("op", op.toString())
                  span.addAttribute("cached", !miss)
                  span.addAttribute("row_key", CBOR.showBuffer(op.rowKey))
                }

                var s0 = started
                if (miss) s0 = s0.recordCacheMiss(res.bytesRead)
                s0 = s0.recordRead(op.columnFamily, op.rowKey, res.lastModifiedTS)
                (res, s0)
              }
            }

            val w = waitFuture(fut) {
              case Success((res, state)) =>
                Next(QDone(res), state.stopRead(), conts, Nil)
              case Failure(ex) =>
                Next(QFail(ex), state, conts, Nil)
            }

            // this branch of the query has to wait on the future, so we park it
            // and process any other branches that are part of an accumulate.
            stepAcc(w, accs) match {
              case Next(q, s, cs, as) => stepEval(q, s, cs, as)
              case res: YieldStep     => res
            }
        }

      case QScan(op) =>
        val w = waitFuture {
          tracer.withSpan("query.scan") {

            implicit val ec = ImmediateExecutionContext

            // NB: Scans don't benefit from caching, so there's no caching.
            repo.keyspace.scan(
              repo.priorityGroup,
              op,
              state.deadline min repo.newReadTimeout.bound
            ) map { res =>
              tracer.activeSpan foreach { span =>
                span.addAttribute("op", op.toString())
              }

              res
            }
          }
        } {
          case Success(res) =>
            Next(QDone(res), state, conts, Nil)
          case Failure(ex) =>
            Next(QFail(ex), state, conts, Nil)
        }

        // this branch of the query has to wait on the future, so we park it and
        // process any other branches that are part of an accumulate.
        stepAcc(w, accs) match {
          case Next(q, s, cs, as) => stepEval(q, s, cs, as)
          case res: YieldStep     => res
        }

      case QSubTxn(fut) =>
        val w = waitFuture(fut(this, state)) {
          case Success(res) =>
            Next(QDone(res), state, conts, Nil)
          case Failure(ex) =>
            Next(QFail(ex), state, conts, Nil)
        }

        // this branch of the query has to wait on the future, so we park it and
        // process any other branches that are part of an accumulate.
        stepAcc(w, accs) match {
          case Next(q, s, cs, as) => stepEval(q, s, cs, as)
          case res: YieldStep     => res
        }
    }
  }

  // Accumulate machinery

  @annotation.tailrec
  private def stepAcc(res: YieldStep, accs: AccStack): EvalStep =
    accs match {
      case Nil => res
      case acc :: tail =>
        accSave(acc, acc.buildIdx, res)
        acc.buildIdx = (acc.buildIdx + 1).toShort

        // Restore root Local ctx before initializing the next branch or reducing.
        Local.restore(acc.w.locals)

        if (acc.buildIdx < acc.slots.size) {
          // We're still in the process of building up this accumulation's
          // state. Move to the next branch.
          Next(acc.slots(acc.buildIdx)._1, acc.cp, Nil, accs)
        } else if (acc.pending == 0) {
          // all branches have been built. If there is no outstanding async
          // work, we can eagerly roll up the accumulation.
          accReduceSlots(acc) match {
            case Next(q, s, cs, as) => Next(q, s, cs, as ++ tail)
            case res: YieldStep     => stepAcc(res, tail)
          }
        } else {
          // We're done building, but there are pending branches, so we will set
          // up the waiter and pass it on.
          acc.w.thunk = () => accReduceSlots(acc)
          stepAcc(acc.w, tail)
        }
    }

  private def accInit(
    state: State,
    seed: Any,
    fold: (Any, Any) => Any,
    prevWidth: Int,
    remaining: Iterator[(Query[Any], Done)],
    conts: ContStack,
    locals: Local.Context) = {
    val cp = state.checkpoint
    val b = MArraySeq.newBuilder[(Query[Any], Done)]
    var i = repo.queryAccumulatePageWidth.toInt

    while (i > 0 && remaining.hasNext) {
      i -= 1
      b += remaining.next()
    }

    val slots = b.result()

    val localWidth = prevWidth + slots.size
    // paired with decr in accReduceSlots
    globalWidth += slots.size

    val localLogThreshold = repo.queryWidthLogRatio * repo.queryMaxWidth
    val globalLogThreshold = repo.queryWidthLogRatio * repo.queryMaxGlobalWidth

    if (localWidth > localLogThreshold) {
      getLogger().info(s"Large query width: scopeID=$scopeID width=$localWidth")
    }

    if (globalWidth > globalLogThreshold) {
      getLogger().info(
        s"Large global query width: scopeID=$scopeID width=$globalWidth")
    }

    if (localWidth > repo.queryMaxWidth) {
      repo.stats.incr("Query.Eval.Accumulate.Rejected")
      val err = MaxQueryWidthExceeded(repo.queryMaxWidth, localWidth)
      setFatalError(err)
      Left(QFail(err))
    } else if (globalWidth > repo.queryMaxGlobalWidth) {
      repo.stats.incr("Query.Eval.GlobalWidth.Rejected")
      val err = MaxQueryWidthExceeded(repo.queryMaxGlobalWidth, globalWidth)
      // reset global width so we don't trigger again, which is unnecessary
      globalWidth = 0
      setFatalError(err)
      Left(QFail(err))
    } else {
      val w = Wait(null, locals)
      Right(
        new AccState(
          cp,
          slots,
          seed,
          fold,
          prevWidth + slots.size,
          remaining,
          conts,
          w))
    }
  }

  private def accSave(acc: AccState, i: Int, step: YieldStep): Unit =
    step match {
      case d @ Done(_, _) =>
        acc.slots(i) = ((acc.slots(i)._1, d))
      case w: Wait =>
        acc.pending = (acc.pending + 1).toShort
        mapWait(w) { es =>
          acc.pending = (acc.pending - 1).toShort
          val next = es match {
            case Next(q, st, cs, accs) => stepEval(q, st, cs, accs)
            case res: YieldStep        => res
          }
          accSave(acc, i, next)

          // if no more branches are pending and the waiter has been set above
          // in stepAcc, we need trigger it.
          if ((acc.w.thunk ne null) && acc.pending == 0) {
            waitImmediate(acc.w)
          }

          // waiter result for acc branches is unused but we have to return
          // something. `null` will at least cause things to blow up later on.
          // FIXME: separate result transformation and callback use-cases?
          null
        }
    }

  // TODO: Accumulates could by optimized somewhat by checking for read
  // invalidations as waiters come back, rather than only at the end. It seems
  // like that implementation would be quite a bit more complex, and I don't
  // think there is much benefit.
  private def accReduceSlots(acc: AccState): EvalStep = {
    val iter = acc.slots.iterator

    @annotation.tailrec
    def reduce0(state: State, m: Any): EvalStep =
      if (iter.isEmpty) {
        if (acc.remaining.isEmpty) {
          repo.stats.timing("Query.Eval.Accumulate.Width", acc.totalWidth)
          Next(QDone(m), state.commit(), acc.conts, Nil)
        } else {
          val s0 = state.commit()
          accInit(
            s0,
            m,
            acc.fold,
            acc.totalWidth,
            acc.remaining,
            acc.conts,
            acc.w.locals) match {
            case Left(fail)  => Next(fail, s0, acc.conts, Nil)
            case Right(acc0) => Next(acc0.slots(0)._1, acc.cp, Nil, List(acc0))
          }
        }
      } else {
        val (q, Done(qt, s)) = iter.next()

        // paired with incr in accInit
        globalWidth -= 1

        if (state.invalidatesReadsOf(s)) {
          repo.stats.incr("Query.Eval.Reads.Invalidated")
          // The state was invalidated here but we still want to include the
          // diagnostics because that code path
          // was still executed.
          val s0 = state.checkpoint.addDiagnostics(s.diagnostics)
          // we dump out to outer Future-based trampoline here in order to avoid
          // a stack buildup in calls between reduce0 & reduce1.
          waitFuture(Future.unit)(_ => reduce1(Next(q, s0, Nil, Nil), m))
        } else {
          val s1 =
            try {
              state.merge(s)
            } catch {
              // throw in merge is fatal
              case t: Throwable =>
                setFatalError(t)
                state
            }
          qt match {
            case QDone(v) => reduce0(s1, acc.fold(m, v))
            case QFail(_) => Next(qt, s1.commit(omitWrites = true), acc.conts, Nil)
          }
        }
      }

    def reduce1(step: EvalStep, m: Any): EvalStep = {
      step match {
        case Next(q, s, cs, accs) =>
          reduce1(stepEval(q, s, cs, accs), m)
        case Done(qt, s0) =>
          val s1 = s0.commit()
          qt match {
            case QDone(v) => reduce0(s1, acc.fold(m, v))
            case QFail(_) => Next(qt, s1.commit(omitWrites = true), acc.conts, Nil)
          }
        case w: Wait =>
          mapWait(w)(reduce1(_, m))
          w
      }
    }

    reduce0(acc.cp, acc.seed)
  }
}
