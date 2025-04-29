package fauna.repo.query

import fauna.atoms.AccountID
import fauna.atoms.ScopeID
import fauna.exec.{ FaunaExecutionContext, ImmediateExecutionContext, Timer }
import fauna.lang.{ MonadException, Page, TimeBound, Timestamp, Timing }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.security.{
  JWK,
  JWKNotConfigured,
  JWKProvider,
  JWKProviderNotConfigured
}
import fauna.net.RateLimiter
import fauna.repo._
import fauna.repo.service.rateLimits._
import fauna.scheduler._
import fauna.stats.{ QueryMetrics, StatsRecorder }
import fauna.storage.api.{ Read, Scan }
import fauna.storage.ops.Write
import fauna.trace.{ traceMsg, GlobalTracer }
import fauna.tx.transaction.Coordinator
import io.netty.util.AsciiString
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{ Failure, Random, Success, Try }

object Query extends ExceptionLogging {
  private val timer = Timer.Global

  private val toUnit: Any => Query[Unit] = { _ => Query.unit }

  final def apply[A](a: => A): Query[A] = QFail.guard(QDone(a))

  /** Fail a query with a Throwable. Regular failed queries can be recovered with
    * `recover` and `recoverWith`
    */
  @inline final def fail(t: Throwable): Query[Nothing] = QFail(t)

  /** Fail a query unrecoverably with a Throwable. The eval context is permanently
    * failed and any pending compute is short-circuited.
    */
  @inline final def fatal(t: Throwable): Query[Nothing] =
    QContext.flatMap { ctx =>
      ctx.setFatalError(t)
      QFail(t)
    }

  @inline final def value[A](a: A): Query[A] = QDone(a)

  @inline final def write(op: Write): Query[Unit] =
    updateState { _.addWrite(op) } join

  @inline final def read[A <: Read.Result](op: Read[A]): Query[A] =
    QRead(op)

  @inline final def scan[A <: Scan.Result](op: Scan[A]): Query[A] =
    QScan(op)

  @inline final def future[A](f: => Future[A]): Query[A] = QSubTxn((_, _) => f)

  @inline final def fromTry[A](t: Try[A]): Query[A] =
    t match {
      case Success(v) => Query.value(v)
      case Failure(e) => Query.fail(e)
    }

  @inline final def transaction[A](p: Option[PriorityGroup])(
    tx: Query[A]): Query[RepoContext.Result[A]] = {
    QSubTxn { (ctx, _) =>
      val repo = p match {
        case Some(p) => ctx.repo.copy(priorityGroup = p)
        case None    => ctx.repo
      }
      repo.result(tx, ctx.snapshotTime)
    }
  }

  @inline final def transaction[A](tx: Query[A]): Query[RepoContext.Result[A]] =
    transaction(None)(tx)

  @inline final def deferred[A](a: => A): Query[A] = defer(value(a))

  @inline final def defer[A](a: => Query[A]): Query[A] =
    QFlatMap[Unit, A](unit, _ => a)

  /** Attribute for traced spans which contains the number of reads
    * per query evaluation.
    */
  val AttrReadsSize = new AsciiString("query.reads.size")

  /** Attribute for traced spans which contains the number of writes
    * per commit attempt.
    */
  val AttrWritesSize = new AsciiString("query.writes.size")

  val True: Query[Boolean] = QDone(true)

  val False: Query[Boolean] = QDone(false)

  val unit: Query[Unit] = QDone(())

  val none: Query[Option[Nothing]] = QDone(None)

  @inline def some[T](value: T): Query[Option[T]] = QDone(Some(value))

  @inline def context: Query[QueryContext] = QContext

  val repo: Query[RepoContext] = context map { _.repo }

  val snapshotTime: Query[Timestamp] = context map { _.snapshotTime }

  val state: Query[State] = QState(state => (state, state))

  def valueWithStateUpdate[T](t: T)(f: State => State): Query[T] =
    QState(s0 => (t, f(s0)))

  def updateState(f: State => State): Query[State] =
    QState { s0 =>
      val next = f(s0)
      (next, next)
    }

  val stats: Query[StatsRecorder] = repo map { _.stats }

  val nextID: Query[Long] = repo map { _.nextID() }

  val internalJWK: Query[(JWK, String)] = repo flatMap {
    _.internalJWK match {
      case Some(jwk) => QDone(jwk)
      case None      => QFail(new JWKNotConfigured)
    }
  }

  val jwkProvider: Query[JWKProvider] = repo flatMap {
    _.jwkProvider match {
      case Some(jwkProvider) => QDone(jwkProvider)
      case None              => QFail(new JWKProviderNotConfigured)
    }
  }

  private def readOpsPerByte(partitions: Int, bytes: Int): Int = {
    require(bytes > 0, s"bytes read must be > 0, got $bytes.")
    val net = (bytes + QueryMetrics.BytesPerReadOp - 1) / QueryMetrics.BytesPerReadOp

    // all read ops read at least one partition; add any additional
    // margin
    net + partitions - 1
  }

  private def metrics(f: State.Metrics => State.Metrics): Query[Unit] =
    updateState { state => state.copy(metrics = f(state.metrics)) } join

  private def checkComputeOps(n: Int): Query[Unit] = {
    def checkComputeOps0(n: Int) =
      if (n < 1) {
        getLogger().debug(s"checkComputeOps saw < 1: $n")
        Query.unit
      } else {
        Query.context flatMap { ctx =>
          ctx.limiter.tryAcquireCompute(n) match {
            case RateLimiter.DelayUntil(_) =>
              ctx.repo.stats.incr(QueryMetrics.RateLimitCompute)
              Query.state flatMap { state =>
                Query.fatal(OpsLimitExceptionWithMetrics("compute", state.metrics))
              }
            case RateLimiter.Acquired(true) =>
              ctx.repo.stats.incr(QueryMetrics.RateLimitCompute)
              Query.unit
            case RateLimiter.Acquired(false) =>
              Query.unit
          }
        }
      }

    limitCheck(checkComputeOps0(n))
  }

  // Runs `q` if rate limits apply, else no-ops.
  private def limitCheck(q: Query[Unit]): Query[Unit] =
    Query.state flatMap { s =>
      if (s.unlimited) {
        Query.unit
      } else q
    }

  private def checkReadOps(n: Int): Query[Unit] = {
    def checkReadOps0(n: Int): Query[Unit] = {
      require(n >= 1, s"At least one read op must have occurred, but received $n.")

      if (n == 1) {
        // A permit was already reserved for this op if it needed one.
        Query.unit
      } else {
        // -1 to account for the initial permit before the read, see
        // QueryEvalContext.
        acquireReads(n - 1)
      }
    }

    limitCheck(checkReadOps0(n))
  }

  private def acquireReads(n: Int) = {
    require(n > 0, s"cannot acquire $n <= 0 permits")

    Query.context flatMap { ctx =>
      ctx.limiter.tryAcquireReads(n) match {
        case RateLimiter.DelayUntil(_) =>
          ctx.repo.stats.incr(QueryMetrics.RateLimitRead)
          Query.state flatMap { state =>
            Query.fatal(OpsLimitExceptionWithMetrics("read", state.metrics))
          }
        case RateLimiter.Acquired(true) =>
          ctx.repo.stats.incr(QueryMetrics.RateLimitRead)
          Query.unit
        case RateLimiter.Acquired(false) =>
          Query.unit
      }
    }
  }

  def incrCompute(): Query[Unit] =
    addCompute(1)

  def addCompute(n: Int): Query[Unit] =
    Query.context.flatMap { _ =>
      metrics { _.addCompute(n) }
    } recoverWith { case e: TxnTooManyComputeOpsException =>
      Query.fatal(e)
    } flatMap { _ =>
      checkComputeOps(n)
    }

  def incrDocuments(bytes: Int): Query[Unit] = {
    val ops = readOpsPerByte(1, bytes)
    metrics { _.incrDocuments(ops) } flatMap { _ =>
      checkReadOps(ops)
    }
  }

  def addSets(n: Int, partitions: Int, bytes: Int): Query[Unit] = {
    val ops = readOpsPerByte(partitions, bytes)
    metrics { _.addSets(n, ops) } flatMap { _ =>
      checkReadOps(ops)
    }
  }

  def incrCreates(): Query[Unit] =
    metrics { _.incrCreates() }

  def incrUpdates(): Query[Unit] =
    metrics { _.incrUpdates() }

  def incrDeletes(): Query[Unit] =
    metrics { _.incrDeletes() }

  def incrInserts(): Query[Unit] =
    metrics { _.incrInserts() }

  def incrRemoves(): Query[Unit] =
    metrics { _.incrRemoves() }

  def incrDatabaseCreates(): Query[Unit] =
    metrics { _.incrDatabaseCreates() }

  def incrDatabaseUpdates(): Query[Unit] =
    metrics { _.incrDatabaseUpdates() }

  def incrDatabaseDeletes(): Query[Unit] =
    metrics { _.incrDatabaseDeletes() }

  def incrCollectionCreates(): Query[Unit] =
    metrics { _.incrCollectionCreates() }

  def incrCollectionUpdates(): Query[Unit] =
    metrics { _.incrCollectionUpdates() }

  def incrCollectionDeletes(): Query[Unit] =
    metrics { _.incrCollectionDeletes() }

  def incrRoleCreates(): Query[Unit] =
    metrics { _.incrRoleCreates() }

  def incrRoleUpdates(): Query[Unit] =
    metrics { _.incrRoleUpdates() }

  def incrRoleDeletes(): Query[Unit] =
    metrics { _.incrRoleDeletes() }

  def incrKeyCreates(): Query[Unit] =
    metrics { _.incrKeyCreates() }

  def incrKeyUpdates(): Query[Unit] =
    metrics { _.incrKeyUpdates() }

  def incrKeyDeletes(): Query[Unit] =
    metrics { _.incrKeyDeletes() }

  def incrFunctionCreates(): Query[Unit] =
    metrics { _.incrFunctionCreates() }

  def incrFunctionUpdates(): Query[Unit] =
    metrics { _.incrFunctionUpdates() }

  def incrFunctionDeletes(): Query[Unit] =
    metrics { _.incrFunctionDeletes() }

  /** This method will return the result of the query q and
    * execute the provided incr method with the number of bytes
    * that query q read.  The typical use case here is to use this
    * method to increase the read metrics on a query
    * Ex: (setReadQ is the query that returns the results)
    *      Query.readAndIncrStats(Query.addSets(1, config.partitions.toInt, _)) {
    *        setReadQ
    *      }
    */
  def readAndIncrStats[A](incr: Int => Query[Unit])(q: => Query[A]): Query[A] =
    Query.withBytesReadDelta(q) flatMap {
      case (res, Some(n)) => incr(n) map { _ => res }
      case (res, _)       => Query.value(res)
    }

  def withBytesReadDelta[A](query: => Query[A]): Query[(A, Option[Int])] =
    state flatMap { before =>
      query flatMap { res =>
        state map { after =>
          if (after.cacheMissesRolledUp > before.cacheMissesRolledUp) {
            val bytesRead = after.metrics.bytesRead - before.metrics.bytesRead
            if (bytesRead < 1) {
              getLogger().warn(
                s"withByteReadDelta calculated less than 1 byte read. (before: ${before.metrics.bytesRead}, after: ${after.metrics.bytesRead})")
              (res, Some(1))
            } else {
              (res, Some(bytesRead))
            }
          } else {
            (res, None)
          }
        }
      }
    }

  /** This will provide the query result along with the State.Metrics for the
    * query. In order to get query metrics as they are tracked and reported
    * to users and in logs you will need to commit the state metrics to a
    * StatsRequestBuffer and use that with the QueryMetrics class.
    * QueryMetrics will perform calculations on the raw State.Metrics
    * provided here to obtain the values users will see.
    *
    * Example:
    * val stats = new StatsRequestBuffer()
    * // use a user account id if want to track metrics
    * // against their account.
    * stateMetrics.commitAll(stats, AccountID.Root)
    * val metrics = QueryMetrics([query time in ms], stats)
    * metrics.byteReadOps
    * metrics.computeOps
    * ...
    * etc
    */
  def withMetrics[A](query: => Query[A]): Query[(A, State.Metrics)] = {
    query flatMap { res =>
      state map { st =>
        (res, st.metrics)
      }
    }
  }

  def withSpan[A](name: String)(fn: => Query[A]): Query[A] =
    QTrace(name, () => fn)

  def timing[T](key: String)(f: => Query[T]): Query[T] = {
    withSpan("query.timing") {
      GlobalTracer.instance.activeSpan foreach {
        _.setOperation(s"query.timing.$key")
      }
      stats flatMap { stats =>
        val t = Timing.start
        f ensure {
          stats.timing(key, t.elapsedMillis)
          Query.unit
        }
      }
    }
  }

  // Force a read-only query to run through the transaction pipeline,
  // guaranteeing linearizability/strict serializability. Note, this
  // does NOT override whether or not index reads are included in OCC
  // checks.
  def linearized[T](q: Query[T]): Query[T] =
    context flatMap { qec =>
      qec.linearize()
      q
    }

  /** Disables OCCs for the given query.
    *
    * NOTE: This method does not disable OCCs for `Page` continuations. Use
    * `disableConcurrencyChecksForPage` instead.
    */
  def disableConcurrencyChecks[A](query: Query[A]): Query[A] =
    inStateContext[Boolean, A](query)(
      _.enabledConcurrencyChecks,
      _ => false,
      (c, s) => s.copy(enabledConcurrencyChecks = c)
    )

  /** Disables OCCs for the given `Page` and all its next pages. */
  def disableConcurrencyChecksForPage[A](page: PagedQuery[A]): PagedQuery[A] =
    disableConcurrencyChecks(page) map { page =>
      Page(
        page.value,
        page.next.map { next => () =>
          disableConcurrencyChecksForPage(next())
        }
      )
    }

  // Runs `query` but rate limits (read, write, and compute) do not apply.
  def unlimited[A](query: Query[A]): Query[A] =
    inStateContext[Boolean, A](query)(
      _.unlimited,
      _ => true,
      (c, s) => s.copy(unlimited = c)
    )

  private[this] def inStateContext[P, T](
    q: Query[T])(curr: State => P, next: P => P, set: (P, State) => State) =
    QState(state => {
      val c = curr(state)
      (c, set(next(c), state))
    }) flatMap { prev =>
      q ensure {
        updateState { state => set(prev, state) } join
      }
    }

  def accumulate[A, B](ms: Iterable[Query[A]], seed: B)(f: (B, A) => B): Query[B] =
    QAccumulate(ms, seed, f)

  def serialJoin[A](qs: Iterable[Query[A]]): Query[Unit] =
    if (qs.isEmpty) unit else qs.head flatMap { _ => serialJoin(qs.tail) }

  // Type classes

  implicit object MonadInstance extends MonadException[Query] {
    def pure[A](a: A) = Query(a)
    def map[A, B](m: Query[A])(f: A => B) = m map f
    def flatMap[A, B](m: Query[A])(f: A => Query[B]) = m flatMap f
    def accumulate[A, B](ms: Iterable[Query[A]], seed: B)(f: (B, A) => B) =
      Query.accumulate(ms, seed)(f)

    def fail[A](e: Throwable): Query[A] = QFail(e)

    def recover[A, A1 >: A](
      m: Query[A],
      pf: PartialFunction[Throwable, A1]): Query[A1] = m.recover(pf)

    def recoverWith[A, A1 >: A](
      m: Query[A],
      pf: PartialFunction[Throwable, Query[A1]]): Query[A1] = m.recoverWith(pf)
  }

  // Running queries

  // Evaluate a query and commit write effects
  def execute[A](
    repo: RepoContext,
    query: Query[A],
    minSnapTime: Timestamp,
    deadline: TimeBound,
    accountID: AccountID,
    scopeID: ScopeID,
    limiter: OpsLimiter): Future[(Timestamp, A)] = {

    if (!deadline.hasTimeLeft) {
      return Future.failed(
        new TimeoutException(s"Timed out before deadline $deadline"))
    }

    // Use a 0-200ms additional delay between attempts.
    // fauna.tx.transaction.Coordinator.trySuccess already delays even the
    // contention result until it catches up with the read clock, so it
    // already has a sufficient delay; this delay is on top of that.
    val maxDelayMillis = 200
    val metrics = new State.Metrics

    def tryExecute(
      times: Int,
      contention: List[ContentionException],
      minSnapTime: Timestamp): Future[(Timestamp, A)] = {
      val snapshotTime = repo.clock.time max minSnapTime
      val evaluated =
        QueryEvalContext.eval(
          query,
          repo,
          snapshotTime,
          deadline,
          repo.txnSizeLimitBytes,
          limiter,
          scopeID,
          metrics)

      implicit val _ec = FaunaExecutionContext.Implicits.global
      evaluated transformWith {
        case Success((qtry, state)) =>
          val commitF = qtry match {
            case QDone(a) =>
              // if successful, proceed to commit
              commitTransaction(repo, limiter, snapshotTime, state, deadline) map {
                (_, a)
              }
            case QFail(ex) => Future.failed(ex)
          }
          commitF andThen { _ =>
            state.metrics.commitAll(repo.stats, accountID)
          }
        case Failure(ex) =>
          ex match {
            /** TxnTooManyComputeOpsException can occur here due to a state commit + merge in the query eval.
              * This isn't a case that we need in our exception log.
              */
            case _: TxnTooManyComputeOpsException => ()
            case _                                => logException(ex)
          }
          Future.failed(ex)
      } recoverWith {
        case e: ContentionException =>
          repo.stats.incr(QueryMetrics.TransactionsContended)

          val retryDelay = e match {
            case SchemaContentionException(scope, ver) =>
              repo.stats.incr("Storage.Transaction.Schema.Contention")
              repo.cacheContext.schema2
                .invalidateScopeBefore(scope, Some(ver))
              // immediately retry on schema contention
              0.millis

            case _ => Random.nextInt(maxDelayMillis).millis
          }

          if (
            repo.retryOnContention && times < repo.maxAttempts && deadline.hasTimeLeft
          ) {
            repo.stats.timing("Storage.Transaction.Delay", retryDelay.toMillis)
            timer.delay(retryDelay)(tryExecute(times + 1, e +: contention, e.newTS))
          } else {
            if (times >= repo.maxAttempts) {
              if (e.isInstanceOf[SchemaContentionException]) {
                repo.stats.incr("Storage.Transaction.Schema.Contention.Abort")
              } else {
                repo.stats.incr("Storage.Transaction.Contention.Abort")
              }
            }

            throw ContentionException.aggregate(e +: contention)
          }

        case e: TxnTooLargeException =>
          repo.stats.incr(QueryMetrics.TransactionsTooLarge)
          throw e

        case _: TimeoutException if contention.nonEmpty =>
          throw ContentionException.aggregate(contention)

        case Coordinator.ShutDownException =>
          // Shutdown has begun, hang on...
          Future.never

        case e =>
          traceMsg("  ERROR: QUERY ABORT")
          throw e
      }
    }

    if (!deadline.hasTimeLeft) {
      Future.failed(new TimeoutException(s"Timed out before deadline $deadline"))
    } else {
      tryExecute(1, Nil, minSnapTime)
    }
  }

  private def commitTransaction(
    repo: RepoContext,
    limiter: OpsLimiter,
    snapshotTime: Timestamp,
    state: State,
    deadline: TimeBound): Future[Timestamp] = {
    val rw = state.readsWrites
    def writesSize = rw.writesSize
    val readsSize = state.readsWrites.reads.size

    val tracer = GlobalTracer.instance

    tracer.activeSpan foreach { span =>
      span.addAttribute(AttrReadsSize, readsSize)
      span.addAttribute(AttrWritesSize, writesSize)
    }
    repo.stats.timing("Storage.Transaction.Keys.Read", readsSize)
    repo.stats.timing("Storage.Transaction.Keys.Written", writesSize)

    state.flushLocalKeyspace foreach {
      _.getColumnFamilyStores.forEach { _.forceBlockingFlush() }
    }

    if (repo.keyspace eq null) {
      traceMsg(s"  EXECUTE BATCH ($writesSize) DROPPED: No keyspace")
      return Future.successful(snapshotTime)
    }

    if (!state.readsWrites.hasWrites) {
      return Future.successful(snapshotTime)
    }

    // FIXME: use the ReadsPerTransaction feature here.
    val readsLimit = repo.txnMaxOCCReads.toLong

    if (readsSize > readsLimit) {
      return Future.failed(TxnTooLargeException(readsSize, readsLimit, "OCC reads"))
    }

    val writes = rw.allWrites.toVector
    val writeStats = Write.getStats(writes, rw.unlimitedKeys)

    val byteWriteOps = writeStats.ops
    val limitedByteWriteOps = writeStats.limitedOps

    implicit val _ec = ImmediateExecutionContext

    val limitCheckFut = if (byteWriteOps <= 0) {
      getLogger().debug(s"checkWriteOps saw < 1: $byteWriteOps")
      Future.unit
    } else if (limitedByteWriteOps == 0) {
      Future.unit
    } else {
      limiter.tryAcquireWrites(limitedByteWriteOps) match {
        case RateLimiter.DelayUntil(_) =>
          repo.stats.incr(QueryMetrics.RateLimitWrite)
          Future.failed(OpsLimitExceptionWithMetrics("write", state.metrics))
        case RateLimiter.Acquired(true) =>
          repo.stats.incr(QueryMetrics.RateLimitWrite)
          Future.unit
        case RateLimiter.Acquired(false) =>
          Future.unit
      }
    }

    limitCheckFut flatMap { _ =>
      repo.stats.count(QueryMetrics.BytesWrite, writeStats.totalBytes)
      repo.stats.count(QueryMetrics.ByteWriteOps, byteWriteOps)

      tracer.withSpan("query.commit") {
        traceMsg(s"  EXECUTE BATCH ($writesSize) ${"-" * 70}")

        repo.stats.incr("Storage.Transactions")
        repo.stats.timeFuture("Storage.Ops.Write.Time") {
          val write = repo.keyspace.execute(
            rw.reads,
            writes,
            writeStats,
            repo.priorityGroup,
            repo.writeTimeout.bound min deadline)

          write map {
            case None => snapshotTime
            case Some((txTime, elapsed)) =>
              repo.stats.timing(QueryMetrics.TransactionLogTime, elapsed.toMillis)
              txTime
          }
        }
      }
    }
  }
}

/** A Query is a plan for a stateful computation inside Fauna.
  * It has three main results when executed:
  * 1. The output of the computation (an A for a Query[A]).
  * 2. The set of read operations in the computation.
  * 3. The set of mutations in the computation.
  * Executing a query requires an evaluation context, which
  * contains (among other things) information about the repository
  * where reads and writes are done. See Query.execute.
  *
  * As plans of execution, queries are monadic (combine as computations)
  * in a straightforward way: plan to wait for the output of the first
  * query and use its result in the second query. See QFlatMap.
  *
  * As stateful computations, an executing query is provided a
  * state, which parts of the plan may update in the functional
  * style by returning a new state based on the original one.
  * See QState and QueryEvalContext.eval.
  */
sealed trait Query[+A] extends Any {
  final def flatMap[B](f: A => Query[B]): Query[B] = QFlatMap(this, f)

  final def map[B](f: A => B): Query[B] = flatMap { a => QDone(f(a)) }

  final def foreach(f: A => Unit) = this map f

  final def join: Query[Unit] = this flatMap Query.toUnit

  final def recoverWith[A1 >: A](
    f: PartialFunction[Throwable, Query[A1]]): Query[A1] =
    this match {
      case QDone(_)  => this
      case QFail(ex) => QFail.guard(f.applyOrElse(ex, Function.const(this)))
      case q =>
        QFrame[A, A1](
          q,
          {
            case done @ QDone(_) => done
            case fail @ QFail(ex) =>
              QFail.guard(f.applyOrElse(ex, Function.const(fail)))
          })
    }

  final def recover[A1 >: A](f: PartialFunction[Throwable, A1]): Query[A1] =
    recoverWith(f.andThen(QDone.apply(_)))

  final def ensure(f: => Query[Unit]): Query[A] =
    this match {
      case QDone(_) | QFail(_) => QFail.guard(f) flatMap { _ => this }
      case _ =>
        QFrame[A, A](
          this,
          { r =>
            QFail.guard(f) flatMap { _ => r }
          })
    }

  final def andThen(f: Query[Unit]): Query[A] = {
    flatMap(a => f.map(_ => a))
  }
}

// Concrete subtypes.

sealed trait QTry[+A] extends Any with Query[A] {
  def isFailure: Boolean
}

/** QDone is a plan for a no-op successful query: the result is available.
  * It's used to implement monadic "return", which injects a value into
  * the Query monad.
  */
final case class QDone[+A](a: A) extends AnyVal with QTry[A] {
  def isFailure = false
}

object QFail {
  final def guard[A](f: => Query[A]) = try f
  catch { case e: Throwable => QFail(e) }
}

/** QFail is a plan for a no-op failed query.
  */
final case class QFail(err: Throwable) extends AnyVal with QTry[Nothing] {
  def isFailure = true
}

/** QFlatMap sequences a second plan that depends on the results of a first plan.
  * It's used to implement monadic "bind", which is the fundamental way
  * Query computations can be combined.
  */
final case class QFlatMap[A, +B](q: Query[A], fn: A => Query[B]) extends Query[B]

/** QFrame sequences plans like QFlatMap but in the case where the first plan
  * may fail.
  *
  * If the first query's execution results in an error, writes added to the
  * state do not get committed. If the first query succeeds, execution
  * continues and mutations are added to the execution's state.
  */
final case class QFrame[I, +A](op: Query[I], cont: QTry[I] => Query[A])
    extends Query[A]

/** QState is a plan that consumes execution state and returns a new state.
  * It emulates a mutable environment during execution.
  */
final case class QState[+A](op: State => (A, State)) extends AnyVal with Query[A]

/** QContext injects a reference to the current evaluation context into the
  * plan. The eval context is used for retrieving information about the
  * environment during execution.
  */
case object QContext extends Query[QueryContext]

/** Executes the fut computation in a sub-environment. Returns a new
  * monadic value and restores the original State after fut has
  * completed.
  */
final case class QSubTxn[+A](fut: (QueryContext, State) => Future[A])
    extends AnyVal
    with Query[A]

/** QRead is a plan to perform a read operation from storage. It
  * behaves like QState in that, during execution, it also produces an
  * updated state.
  */
final case class QRead[A <: Read.Result](op: Read[A]) extends AnyVal with Query[A]

/** A scan is like a read except:
  * * it does not traverse the network
  * * it's not recorded in the query state
  * * it's meant for internal background process use ONLY
  */
final case class QScan[A <: Scan.Result](op: Scan[A]) extends AnyVal with Query[A]

/** QAccumulate is an optimized plan for folding together several plans.
  * As with all monadic operations, it can be implemented with flatMap, but
  * QAccumulate is more efficient because it directs that the sub-queries be
  * executed in parallel.
  */
final case class QAccumulate[A, B](qs: Iterable[Query[A]], seed: B, acc: (B, A) => B)
    extends Query[B]

/** QTrace causes its sub-query to be executed inside a trace span called `name`.
  */
final case class QTrace[A](name: String, fn: () => Query[A]) extends Query[A]
