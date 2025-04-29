package fauna.repo.test

import fauna.atoms.{ CollectionID, DocID, SchemaVersion, ScopeID, SubID }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.prop.Prop
import fauna.repo.doc.Version
import fauna.repo.query._
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import fauna.repo.Store
import fauna.stats.StatsRecorder
import fauna.storage.doc.{ Data, Field }
import fauna.storage.ops.{ VersionAdd, Write }
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NonFatal

object RYOWSpec {
  val MaxDocs = 4
  val MaxDepth = 8
  val MaxWidth = 12
  val MaxTxnSize = 16 * 1024 * 1024
}

class RYOWSpec extends PropSpec {
  import RYOWSpec._

  val ctx = CassandraHelper.context("repo")
  val scopeID = ScopeID(ctx.nextID())
  val collID = CollectionID(ctx.nextID().toShort) // CollectionIDs are 2 bytes max.)
  val iField = Field[Int]("data", "i")

  val schemaVersions = MMap.empty[ScopeID, Option[SchemaVersion]]

  val docIDP =
    Prop.int(1 to MaxDocs) map { id =>
      DocID(SubID(id), collID)
    }

  val readP =
    docIDP map {
      Store.getUnmigrated(scopeID, _)
    }

  val upsertP =
    docIDP map { docID =>
      Store.getUnmigrated(scopeID, docID) map {
        case None     => (Data(iField -> 0), true)
        case Some(vs) => (Data(iField -> (vs.data(iField) + 1)), false)
      } flatMap { case (data, isCreate) =>
        Store.insertUnmigrated(scopeID, docID, data, isCreate)
      }
    }

  val deleteP =
    docIDP map { docID =>
      Store.getUnmigrated(scopeID, docID) flatMap {
        case Some(_) => Store.removeUnmigrated(scopeID, docID)
        case None    => Query.unit
      }
    }

  def accP(depth: Int) =
    Prop.int(1 to MaxWidth) flatMap { width =>
      Seq.fill(width)(queryP(depth + 1)).sequence map {
        _.sequence
      }
    }

  def queryP(depth: Int = 0): Prop[Query[_]] =
    depth match {
      case `MaxDepth` => Prop.const(Query.unit)
      case 0          => accP(depth) // prefer large queries
      case _ =>
        Prop.int(4) flatMap {
          case 0 => readP
          case 1 => upsertP
          case 2 => deleteP
          case 3 => accP(depth)
        }
    }

  def evalDefault(q: Query[_]): State = {
    val (_, state) = await(
      QueryEvalContext.eval(
        q,
        ctx,
        Clock.time,
        DefaultTimeout.bound,
        MaxTxnSize,
        PermissiveOpsLimiter,
        ScopeID.RootID
      ))
    state
  }

  def evalSequential(q: Query[_]): State = {
    schemaVersions.clear()
    val snapshotTS = Clock.time

    val qCtx = new QueryContext {
      val repo = ctx
      val snapshotTime = snapshotTS
      def linearize() = sys.error("unsupported")
      def limiter = PermissiveOpsLimiter
      def useNewReadCache = false
      def readCache = sys.error("unsupported")
      def inspectReadCache[A](fn: ReadCache.View => A) = sys.error("unsupported")
      def indexConsistencyCheckDocIDs = sys.error("unsupported")
      def addIndexConsistencyCheckDocID(id: DocID) = sys.error("unsupported")
      def setFatalError(t: Throwable): Unit = sys.error("unsupported")
      def updateSchemaVersionState(s: ScopeID, cur: Option[SchemaVersion]) =
        schemaVersions.get(s) match {
          case None =>
            schemaVersions(s) = cur
          // Assume no contention because the schema doesn't change.
          case _ => ()
        }
    }

    def postEvalState(seed: State): State = {
      var state = seed
      schemaVersions.foreach { case (scope, ts) =>
        state = state.addSchemaVersionOccCheck(scope, ts)
      }
      state
    }

    def eval0(q: Query[_], state: State): (Any, State) =
      q match {
        case QDone(v) => (v, state)
        case QContext => eval0(QDone(qCtx), state)

        case QRead(op) =>
          var ioSkipped = false
          val writes =
            state
              .writesForRowKey(op.rowKey)
              .filter { op.isRelevant(_) }
              .toSeq

          val res = await(
            op.skipIO(writes)
              .map { res =>
                ioSkipped = true
                Future.successful(res)
              }
              .getOrElse(
                qCtx.repo.keyspace.read(
                  qCtx.repo.priorityGroup,
                  op,
                  writes,
                  state.deadline
                )
              ))
          val state0 =
            state.recordRead(
              op.columnFamily,
              op.rowKey,
              res.lastModifiedTS,
              occCheckEnabled = !ioSkipped
            )
          (res, state0)

        case QScan(op) =>
          val res = await(
            qCtx.repo.keyspace.scan(qCtx.repo.priorityGroup, op, state.deadline))

          (res, state)

        case QFlatMap(q, fn) =>
          val (v, st) = eval0(q, state)
          eval0(fn(v), st)

        case QAccumulate(qs, seed, acc) =>
          qs.foldLeft((seed, state)) { case ((prev, state), q) =>
            val (v, nextSt) = eval0(q, state)
            (acc(prev, v), nextSt)
          }

        case QState(op) =>
          val (v, st) = op(state)
          eval0(QDone(v), st)

        case QFail(ex) => throw ex

        case QTrace(_, fn) => eval0(QFail.guard(fn()), state)

        case QSubTxn(fut) =>
          try {
            val v = await(fut(qCtx, state))
            eval0(QDone(v), state)
          } catch {
            case NonFatal(ex) => eval0(QFail(ex), state)
          }

        case QFrame(q, fn) =>
          try {
            val (r, s) = eval0(q, state.checkpoint)
            eval0(fn(QDone(r)), s.commit())
          } catch {
            case NonFatal(ex) =>
              eval0(QFail(ex), state.commit(omitWrites = true))
          }
      }

    val seed =
      State(
        parent = None,
        enabledConcurrencyChecks = true,
        readsWrites = ReadsWrites.empty,
        collectionWrites = CollectionWrites.empty,
        cacheMisses = 0,
        stats = StatsRecorder.Null,
        deadline = 5.minutes.bound,
        txnSizeLimitBytes = MaxTxnSize,
        txnSizeBytes = 0,
        readOnlyTxn = true,
        flushLocalKeyspace = Set.empty,
        serialReads = 0,
        unlimited = false
      )

    val (_, state) = eval0(q, seed)
    postEvalState(state)
  }

  prop("Query evaluation follows sequential semantics") {
    for {
      // Persist a write effect per iteration to ensure diffs are carried forward.
      seedQ <- upsertP
      _ = ctx ! seedQ
      // Run an arbitrary query with the default QEC and the sequential one to
      // compare their read and write sets -- no write effects are persisted.
      testQ <- queryP()
      default = evalDefault(testQ).readsWrites
      sequential = evalSequential(testQ).readsWrites
    } yield {
      default.reads should contain theSameElementsInOrderAs sequential.reads
      default.mergeableWrites should contain theSameElementsAs sequential.mergeableWrites
      default.nonMergableWrites should contain theSameElementsAs sequential.nonMergableWrites
      assertWriteEffects(default.allWrites)
    }
  }

  def assertWriteEffects(writes: Iterator[Write]): Unit = {
    val inDisk = {
      val seedsQ =
        (1 to MaxDocs).map { id =>
          val docID = DocID(SubID(id), collID)
          Store.getUnmigrated(scopeID, docID) mapT {
            docID -> _.withUnresolvedTS // drop txn time for equality checks
          }
        }.sequence
      (ctx ! seedsQ).flatten.toMap
    }

    writes foreach {
      case w: VersionAdd =>
        val writeVersion =
          if (w.action.isCreate) {
            Version.Live(
              w.scope,
              w.id,
              w.writeTS,
              w.action,
              w.schemaVersion,
              w.data,
              w.diff
            )
          } else {
            Version.Deleted(
              w.scope,
              w.id,
              w.writeTS,
              w.schemaVersion,
              w.diff
            )
          }
        writeVersion.prevVersion shouldBe inDisk.get(w.id)
      case w =>
        sys.error(s"unexpected write: $w")
    }
  }
}
