package fauna.repo.test

import fauna.atoms._
import fauna.lang.{ Page, TimeBound }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query._
import fauna.repo.service.rateLimits._
import fauna.storage.{ Add, Create, Unresolved }
import fauna.storage.api.version.DocSnapshot
import fauna.storage.doc.{ Data, Field }
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.ops.{ NoopWrite, RemoveAllWrite, SetAdd, VersionAdd, Write }
import io.netty.buffer.Unpooled
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Random

class QuerySpec extends Spec {

  implicit val ctx = CassandraHelper.context("repo")

  def eval[A](
    q: Query[A],
    ctx: RepoContext = ctx,
    timeout: Duration = Duration.Inf,
    sizeLimitBytes: Int = 16 * 1024 * 1024) =
    Await.result(
      QueryEvalContext
        .eval(
          q,
          ctx,
          Clock.time,
          TimeBound.Max,
          sizeLimitBytes,
          PermissiveOpsLimiter,
          ScopeID.RootID
        ),
      timeout
    )

  def result[A](q: Query[A]): A =
    eval(q) match {
      case (QDone(a), _) => a
      case (QFail(e), _) => throw e
    }

  def error[A](q: Query[A]): Option[Throwable] =
    eval(q) match {
      case (QDone(_), _) => None
      case (QFail(e), _) => Some(e)
    }

  def writes[A](q: Query[A]): Iterable[Write] =
    eval(q) match {
      case (QDone(_), s) => s.readsWrites.allWrites.toSeq
      case _             => Nil
    }

  def write(label: String): Write = RemoveAllWrite(label, Unpooled.EMPTY_BUFFER)

  "Query" - {
    "runs" in {
      result(Query(2)) should equal(2)
    }

    "limits compute" in {
      val limit = State.computeOpsLimit
      val over = limit + 1
      error(Query.addCompute(limit)) should equal(None)
      error(Query.addCompute(over)) should equal(
        Some(TxnTooManyComputeOpsException(over, limit)))
    }

    "sequences with map and flatMap" in {
      result(Query(2) map { _ + 2 }) should equal(4)
      result(Query(2) flatMap { i => Query(i + 2) }) should equal(4)
    }

    "cluster health" in {
      CassandraHelper.markUnhealthy()
      ctx.isClusterHealthy should equal(false)
      ctx.isLocalHealthy should equal(false)
      ctx.isStorageHealthy should equal(false)

      CassandraHelper.markHealthy()
      ctx.isClusterHealthy should equal(true)
      ctx.isLocalHealthy should equal(true)
      ctx.isStorageHealthy should equal(true)
    }

    "captures throwables" in {
      val oops = new Exception("oops")

      error(Query(throw oops)) should equal(Some(oops))
      error(Query(1) map { _ => throw oops }) should equal(Some(oops))
      error(Query(1) flatMap { _ => throw oops }) should equal(Some(oops))
      error(Query deferred { throw oops }) should equal(Some(oops))
    }

    "recovers errors" in {
      val oops = new Exception("oops")

      val q = Query.deferred { if (true) throw oops else "foo" }

      error(q) should equal(Some(oops))
      result(q recover { case _ => "bar" }) should equal("bar")
      result(q recoverWith { case _ => Query("bar") }) should equal("bar")
      result(q recover { case _ => throw oops } recover { case _ =>
        "bar"
      }) should equal("bar")
      result(q recoverWith { case _ => throw oops } recover { case _ =>
        "bar"
      }) should equal("bar")
      result(q flatMap { _ => Query("foo") } recoverWith { case _ =>
        throw oops
      } recover { case _ => "bar" }) should equal("bar")
    }

    "transaction size limits" - {
      val scope = ScopeID(result(Query.nextID))
      val doc = DocID(SubID(result(Query.nextID)), CollectionID(1024))

      "component too large" in {
        val term =
          BytesV(Unpooled.wrappedBuffer(new Array[Byte](Short.MaxValue - 16)))
        val key = IndexKey(scope, IndexID(1024), Vector(IndexTerm(term)))
        val value = IndexValue(scope, doc, Clock.time, Add)

        val write = SetAdd(key, value)

        write.canWrite should be(false)
        // NB. The write will be dropped and the query will
        // succeed. This exception is not recoverable.
        result(Query.write(write)) should be(())
      }

      "key too large" in {
        val term =
          BytesV(Unpooled.wrappedBuffer(new Array[Byte](Short.MaxValue - 17)))
        val values =
          BytesV(Unpooled.wrappedBuffer(new Array[Byte](Short.MaxValue - 1578)))

        val key = IndexKey(scope, IndexID(1024), Vector(IndexTerm(term)))
        val tuple = IndexTuple(scope, doc, Vector(IndexTerm(values)))
        val value = IndexValue(tuple, Clock.time, Add)

        val write = SetAdd(key, value)
        write.canWrite should be(true)

        a[TransactionKeyTooLargeException] shouldBe thrownBy {
          result(Query.write(write))
        }

        val rec = Query.write(write) recover {
          case _: TransactionKeyTooLargeException => ()
        }

        noException shouldBe thrownBy {
          result(rec)
        }
      }

      "transaction too large" in {
        val term =
          BytesV(Unpooled.wrappedBuffer(new Array[Byte](Short.MaxValue - 17)))
        val values =
          BytesV(Unpooled.wrappedBuffer(new Array[Byte](Short.MaxValue - 1579)))

        val key = IndexKey(scope, IndexID(1024), Vector(IndexTerm(term)))
        val tuple = IndexTuple(scope, doc, Vector(IndexTerm(values)))
        val value = IndexValue(tuple, Clock.time, Add)

        val write = SetAdd(key, value)
        write.canWrite should be(true)

        val q = Query.write(write)

        eval(q, sizeLimitBytes = write.numBytes - 1) should matchPattern {
          case (QFail(_: TxnTooLargeException), _) =>
        }

        val rec = q recoverWith { case _: TxnTooLargeException =>
          // Test that recovering with a smaller write can succeed.
          Query.write(SetAdd(key, IndexValue(scope, doc, Clock.time, Add)))
        }

        eval(rec, sizeLimitBytes = write.numBytes - 1) should matchPattern {
          case (QDone(_), _) =>
        }
      }
    }

    "errors cancel future actions" in {
      val oops = new Exception("oops")
      var didit = false

      val q = Query.deferred { if (true) throw oops else "foo" } map { _ =>
        didit = true; ()
      }

      error(q) should equal(Some(oops))
      didit should equal(false)
    }

    def futureQ[A](a: A) = {
      implicit val ec = ExecutionContext.Implicits.global
      Query.future(Future {
        Thread.sleep(10 + Random.nextInt(200))
        a
      })
    }

    "sequences futures" in {
      val q1 = futureQ(1)
      result(q1) shouldEqual 1

      val q2 = futureQ(1).flatMap(_ => futureQ(2))
      result(q2) shouldEqual 2
    }

    "accumulates futures" in {
      val q1 = (1 to 10).map(i => Query.value(i)).sequence
      result(q1) shouldEqual (1 to 10)

      val q2 = (1 to 10).map(i => futureQ(i)).sequence
      result(q2) shouldEqual (1 to 10)
    }

    "has an accessible snapshot time" in {
      val getQ =
        Store.getLatestNoTTLUnmigrated(
          ScopeID(1),
          DocID(SubID(1), CollectionID(1024)))
      val ctxQ = getQ flatMap { _ => Query.snapshotTime }
      val now = Clock.time
      val res = result(ctxQ)
      res should be >= now
    }

    "does not overflow" in {
      val max = 100000
      val q = (1L to max foldLeft Query.future(Future.unit)) { case (q, _) =>
        q flatMap { _ => Query.unit }
      }

      result(q)
      noException should be thrownBy result(q)
    }

    "does not stack overflow" in {
      val qs = Seq.empty[Query[Unit]]
      def loop(lim: Long): Query[Unit] = Query.defer(
        if (lim == 0) Query.value(())
        else qs.sequence flatMap { _ => loop(lim - 1) })
      eval(loop(8192), ctx, 10.seconds)
    }

    "does not abort parallel branches on normal error" in {
      val counters = MMap.empty[Int, Int]
      def incr(branch: Int): Int = counters.synchronized {
        counters.updateWith(branch)(i => Some(i.getOrElse(0) + 1)).get
      }

      def loop(branch: Int): Query[Unit] =
        Query.defer {
          Thread.sleep(10)
          if (branch == 0) {
            Query.fail(new Exception("stop it!"))
          } else if (incr(branch) == 100) {
            Query.unit
          } else {
            loop(branch)
          }
        }

      val q = (0 until 10).map(loop).join

      the[Exception] thrownBy { ctx ! q } should have message "stop it!"

      // the query failed, but the state of counters demonstrates that other
      // branches ran to completion.
      counters.toMap shouldEqual (1 until 10).map((_, 100)).toMap
    }

    "aborts parallel branches on fatal error" in {
      def loop(branch: Int, depth: Int): Query[Unit] =
        Query.defer {
          // Just one branch will fail. Others run until interrupted.
          if (branch == 0 && depth == 10) {
            // If this is changed to `Query.fail`, the query will either loop
            // infinitely or overflow the stack.
            Query.fatal(new Exception("stop it!"))
          } else {
            Seq(loop(branch, depth + 1), loop(branch + 1, depth + 1)).join
          }
        }

      the[Exception] thrownBy { ctx ! loop(0, 0) } should have message "stop it!"
    }

    "parallel branches preserve sequential semantics" in {
      val docID = DocID(SubID(1), CollectionID(1024))
      val field = Field[Int]("i")
      val snapTS = Clock.time

      def write(i: Int) = {
        val data = Data(field -> i)
        val w = VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          data,
          None)
        Query.write(w) map { _ => i }
      }

      def read() = {
        val r = DocSnapshot(ScopeID.RootID, docID, snapTS)
        Query.read(r) map { res =>
          // Note that .get will fail in braches that don't see an early write. The
          // query should then rerun this branch causing the error to be discarded in
          // favor or a correct execution where early writes were observed.
          Version.decode(res.version.get).data(field)
        }
      }

      def loop(branch: Int, depth: Int): Query[Int] =
        Query.defer {
          if (depth == 3) {
            if (branch == 0) {
              write(1) // Only the first branch writes the initial value.
            } else {
              // Every other branch must run sequentially to observe early writes.
              read() flatMap { i => write(i + 1) }
            }
          } else {
            Seq(
              loop(branch, depth + 1),
              loop(branch + 1, depth + 1)
            ).sequence map { _.last }
          }
        }

      (ctx ! loop(0, 0)) shouldBe math.pow(2, 3)
    }

    "linearized" - {
      "adds a NoopWrite" in {
        val q = Query.linearized(Query.repo)

        writes(q) should equal(Seq(NoopWrite))
      }

      "doesn't add NoopWrite if unnecessary" in {
        val w = write("write")
        val q = Query.linearized(Query.write(w))
        writes(q) should equal(Seq(w))
      }

      "no-IO transactions still complete" in {
        val q = Query.linearized(Query.deferred("yes!"))

        (ctx ! q) should equal("yes!")
      }
    }

    "framing and sequencing" - {
      val oops = new Exception("oops")
      val w1 = write("write 1")
      val w2 = write("write 2")

      "recover 1" in {
        val q = Query.write(w1) flatMap { _ =>
          Query.write(w2) map { _ =>
            throw oops
          }
        } recover { case _ =>
          ()
        }

        writes(q) should not(contain(w1))
        writes(q) should not(contain(w2))
      }

      "recover 2" in {
        val q = Query.write(w1) flatMap { _ =>
          Query.write(w2) map { _ =>
            throw oops
          } recover { case _ =>
            ()
          }
        }

        writes(q) should contain(w1)
        writes(q) should not(contain(w2))
      }

      "recover 3" in {
        val q = Query.write(w1) flatMap { _ =>
          Query.write(w2) flatMap { _ =>
            Query(throw oops) recover { case _ =>
              ()
            }
          }
        }

        writes(q) should contain(w1)
        writes(q) should contain(w2)
      }

      "merged 1" in {
        val q = Seq(Query.write(w1), Query.write(w2)).join

        writes(q) should contain(w1)
        writes(q) should contain(w2)
      }

      "merged 2" in {
        val q = Seq(Query.write(w1), Query.write(w2) map { _ => throw oops }).join

        writes(q) should not(contain(w1))
        writes(q) should not(contain(w2))
      }

      "cannot recover from fatal exception" in {
        val fatal = new Exception("fatal")

        val q0 = Query.fatal(fatal) recover { _ => () }
        the[Exception] thrownBy (ctx ! q0) shouldBe fatal
      }

      "run side effects on fatal exceptions" in {
        val fatal = new Exception("fatal")

        @volatile var v0 = false
        val q0 = Query.fatal(fatal) recover { _ => v0 = true }

        the[Exception] thrownBy (ctx ! q0) shouldBe fatal
        assert(v0, "didn't run side effect")

        @volatile var v1 = false
        val q1 = Query.fatal(fatal) ensure {
          v1 = true
          Query.unit
        }

        the[Exception] thrownBy (ctx ! q1) shouldBe fatal
        assert(v1, "didn't run side effect")
      }
    }

    "limits" - {
      "max width of a flat query" in {
        // max width must be GTE acc page size in order to make sense
        val limited = ctx.copy(queryMaxWidth = 1000, queryAccumulatePageWidth = 1000)

        val q0 = Seq.fill(1000) { Query.value(42) }.join
        noException should be thrownBy { eval(q0, limited) }

        val q1 = Seq.fill(1001) { Query.value(42) }.join

        val (QFail(ex), _) = eval(q1, limited)
        ex should have message "Query exceeds max width. Max: 1000, current: 1001."
      }

      "max width of nested queries queries" in {
        // max width must be GTE acc page size in order to make sense
        val limited = ctx.copy(queryMaxWidth = 1000, queryAccumulatePageWidth = 1000)

        val q = Seq(
          Seq.fill(1000) { Query.value(42) }.join,
          Seq.fill(1001) { Query.value(42) }.join).join

        val (QFail(ex), _) = eval(q, limited)
        ex should have message "Query exceeds max width. Max: 1000, current: 1001."
      }

      "max global width" in {
        val limited = ctx.copy(queryMaxWidth = 1000, queryMaxGlobalWidth = 1000)

        val q =
          Seq.fill(100)(Seq.fill(100)(Query.future(Future.successful(42))).join).join

        val (QFail(ex), _) = eval(q, limited)
        ex should have message "Query exceeds max width. Max: 1000, current: 1100."
      }

      "respects concurrency limit" in {
        var count = 0

        implicit val ec = ExecutionContext.Implicits.global
        val q = Seq
          .fill(128) {
            Query.future(Future {
              // look, ma, no thread safety
              val c = count + 1
              Thread.sleep(1)
              count = c
            })
          }
          .join
        val limited = ctx
          .copy(maxPerQueryParallelism = 1, queryMaxConcurrentReads = 1)
        eval(q, limited, 5.seconds)

        count shouldEqual (128)
      }

      "no deadlock on limited parallelism" in {
        val q = Seq
          .fill(64) { Seq.fill(64) { Query.future(Future.successful(42)) }.join }
          .join
        val limited = ctx
          .copy(maxPerQueryParallelism = 32, queryMaxConcurrentReads = 32)
        noException should be thrownBy { eval(q, limited, 5.seconds) }
      }

      def evalAndCheckFatalState(q: Query[Unit]) = {
        var recovered = false
        var fatal = true
        var ex = Option.empty[Throwable]

        try {
          ctx ! (q recoverWith { _ =>
            // fatal errors still result in recovery blocks running
            recovered = true
            // but mapped compute does not
            Query.unit map { _ => fatal = false; () }
          })
        } catch { case e: Throwable => ex = Some(e) }

        assert(ex.isDefined, "did not throw")
        assert(recovered)
        assert(fatal)

        ex.get
      }

      "exceeding compute limits is fatal (single branch)" in {
        def loop(branch: Int): Query[Unit] =
          Query.defer {
            if (branch == 5) {
              // One branch blows the limit while the rest do no compute.
              Query.addCompute(State.computeOpsLimit + 1)
            } else {
              loop(branch + 1)
            }
          }

        val ex = evalAndCheckFatalState(loop(0))

        assert(ex.isInstanceOf[TxnTooManyComputeOpsException])
      }

      "exceeding compute limits is fatal (cross-branch)" in {
        val q = (0 to 4)
          .map(_ => Query.addCompute(State.computeOpsLimit / 5 + 1))
          .join

        val ex = evalAndCheckFatalState(q)

        assert(ex.isInstanceOf[TxnTooManyComputeOpsException])
      }
    }

    "OCC" - {
      "disables OCCs for query" in {
        val occEnabledQ =
          Query.disableConcurrencyChecks(
            Query.state map { _.enabledConcurrencyChecks }
          )
        assert(!result(occEnabledQ))
      }

      "disables OCCs for pages" in {
        val pageQ =
          Query.disableConcurrencyChecksForPage(
            Page.unfold[Query, Int, (Int, Boolean)](0) { n =>
              Query.state map { state =>
                ((n, state.enabledConcurrencyChecks), Some(n + 1))
              }
            }
          )

        val (_, occEnabled) = result(pageQ).value
        assert(!occEnabled) // disables OCC for first page

        val (_, occEnabled0) = result(pageQ.flatMap { _.next.get() }).value
        assert(!occEnabled0) // disables OCC for second page
      }
    }
  }

  "unlimited mode" - {
    def evalWithLimiter[T](q: Query[T], limiter: OpsLimiter) =
      Await.result(
        QueryEvalContext
          .eval(
            q,
            ctx,
            Clock.time,
            TimeBound.Max,
            16 * 1024 * 1024,
            limiter,
            ScopeID.RootID
          ),
        Duration.Inf
      )

    "doesn't rate limit compute" in {
      val q = Query.addCompute(1)
      evalWithLimiter(q, FixedComputeOpsLimiter(0)) should matchPattern {
        case (QFail(_: OpsLimitExceptionWithMetrics), _) => // OK.
      }
      evalWithLimiter(
        Query.unlimited(q),
        FixedComputeOpsLimiter(0)) should matchPattern { case (QDone(_), _) => // OK.
      }
    }

    "doesn't rate limit reads" in {
      val q = Query.snapshotTime.flatMap { ts =>
        Query.read(DocSnapshot(ScopeID.RootID, DocID.MinValue, ts))
      }
      evalWithLimiter(q, FixedReadOpsLimiter(0)) should matchPattern {
        case (QFail(_: OpsLimitExceptionWithMetrics), _) => // OK.
      }
      evalWithLimiter(
        Query.unlimited(q),
        FixedReadOpsLimiter(0)) should matchPattern { case (QDone(_), _) => // OK.
      }
    }

    "doesn't rate limit writes" in {
      val q = Query.write(
        VersionAdd(
          ScopeID.RootID,
          DocID.MinValue,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data.empty,
          None))

      // Write rate limit checking is done when committing the transaction post-eval,
      // so we check the limited ops number from the state to mimic what happens
      // then.
      def getOps(state: State) = {
        val rw = state.readsWrites
        val stats = Write.getStats(rw.allWrites.toVector, rw.unlimitedKeys)
        (stats.ops, stats.limitedOps)
      }

      val (_, state) = evalWithLimiter(q, FixedWriteOpsLimiter(0))
      getOps(state) shouldBe ((1, 1))

      val (_, stateU) = evalWithLimiter(Query.unlimited(q), FixedWriteOpsLimiter(0))
      getOps(stateU) shouldBe ((1, 0))
    }
  }
}
