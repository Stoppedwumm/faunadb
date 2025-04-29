package fauna.model.gc.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.flags.EnableMVTReadHints
import fauna.lang._
import fauna.lang.clocks._
import fauna.model.gc._
import fauna.model.test._
import fauna.model.Database
import fauna.repo.{ MVTProvider => RepoMVTProvider, _ }
import fauna.repo.cassandra.CassandraService
import fauna.repo.query._
import fauna.repo.test.CassandraHelper
import fauna.storage._
import fauna.storage.api._
import fauna.storage.api.set._
import fauna.storage.cassandra.{ CollectionFilter, _ }
import org.apache.cassandra.db.compaction.{ CompactionManager, LeveledManifest }
import org.apache.cassandra.utils.FBUtilities
import org.scalactic.source.Position
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{ Failure, Random, Success }
import scala.util.control.NonFatal

/** Base class for test scenarios affected by inline and/or non-transactional GC.
  *
  * Inheriting classes MUST wrap asserts with the `qassertAt` helper so that they are
  * executed both before and after the GC aware compaction. Be aware that write
  * effects computed before GC are discarded so that the query can be repeated after
  * GC and produce the same result.
  *
  * Considering that asserts run compaction at the given snapshot time, test cases
  * MUST be written with assertions in increasing snapshot order so that an early
  * compaction don't remove data that is required for a following assertion.
  */
abstract class InlineGCSpec(historyDays: Int = 1) extends Spec {
  import SocialHelpers._

  // Provide a version of the MVT provider that also records MVT hints.
  private object FakeMVTProvider extends RepoMVTProvider {

    def get(scopeID: ScopeID, collID: CollectionID) =
      MVTProvider.get(scopeID, collID) map { mvt =>
        feedMVT(scopeID, collID, mvt)
        mvt
      }

    def get(cfg: IndexConfig, terms: Vector[Term]) =
      MVTProvider.get(cfg, terms) map { mvts =>
        mvts match {
          case MVTMap.Default => sys.error("unexpected mvt map")
          case MVTMap.Mapping(map) =>
            map foreach { case (collID, mvt) =>
              feedMVT(cfg.scopeID, collID, mvt)
            }
        }
        mvts
      }

    private def feedMVT(scopeID: ScopeID, collID: CollectionID, mvt: Timestamp) = {
      // NB. Coordinators' clocks are unsynchronized, therefore, the MVT hits jitter
      // around the average clock skew. We mimic this behaviour here when hinting to
      // ensure compactions ran in the spec observe the same behaviour. Moreover, we
      // counter the effect of GCGrace by adding it to the reported MVT.
      val mvt0 = (mvt - Random.between(-100, 100).millis) max Timestamp.Epoch
      CollectionStrategy.hints.put(scopeID, collID, mvt0 + CollectionHints.GCGrace)
    }
  }

  val ctx = CassandraHelper.context("model", mvtProvider = FakeMVTProvider)

  var createTS = Clock.time - 10.days
  var update0TS = createTS + 1.day
  var update1TS = update0TS + 1.day
  var deleteTS = update1TS + 1.day
  var recreateTS = deleteTS + 1.day

  var auth: Auth = _
  var scope: ScopeID = _

  private def service = CassandraService.instance
  private def keyspace = service.storage.keyspace

  private val columnFamilies =
    Set(
      Tables.Versions.CFName,
      Tables.SortedIndex.CFName,
      Tables.HistoricalIndex.CFName
    )

  override def beforeAll() = {
    columnFamilies foreach { cfName =>
      val store = keyspace.getColumnFamilyStore(cfName)
      require(
        store.getCompactionStrategyClass() == classOf[CollectionStrategy].getName(),
        s"collection compaction must be enabled for column family: $cfName"
      )
    }

    // NB. Disable hints at the storage API level so that the spec can control them.
    CassandraHelper.ffService.setHost(EnableMVTReadHints, false)
  }

  override def afterAll() = CassandraHelper.ffService.reset()

  implicit def ts2Json(ts: Timestamp) =
    JSLong(ts.micros)

  before {
    columnFamilies foreach { cfName =>
      val store = keyspace.getColumnFamilyStore(cfName)
      // Flush first to sync up with the Storage-Heartbeat thread,
      // otherwise we'll race and C* gets stuck.
      store.forceBlockingFlush()
      store.clearUnsafe()
    }

    // Make the scope before anything else.
    scope = writeAt(
      createTS - 5.days,
      CreateDatabase(MkObject("name" -> Random.nextInt().toString)),
      RootAuth
    ) match {
      case Right(VersionL(v, _)) => v.data(Database.ScopeField)
      case v                     => fail(s"expected version, got $v")
    }
    auth = Auth.forScope(scope)

    writeAt(
      createTS - 3.days,
      CreateCollection(
        MkObject(
          "name" -> "users",
          "history_days" -> historyDays
        )))

    writeAt(
      createTS - 2.days,
      CreateIndex(
        MkObject(
          "name" -> "users_by_name",
          "source" -> ClassRef("users"),
          "active" -> true,
          "terms" -> Seq(
            MkObject("field" -> Seq("data", "name"))
          )
        ))
    )

    // Create a second index for complex set operations.
    writeAt(
      createTS - 2.days,
      CreateIndex(
        MkObject(
          "name" -> "users_by_job",
          "source" -> ClassRef("users"),
          "active" -> true,
          "terms" -> Seq(
            MkObject("field" -> Seq("data", "job"))
          )
        ))
    )

    CassandraHelper.invalidateCaches(ctx) // ensure cache reload catches idx creation
  }

  def withSimpleHistory[A](test: JSValue => A) = {
    val userID = ctx ! Query.nextID
    val userRef = RefV(userID, ClsRefV("users"))
    writeAt(createTS, CreateF(userRef, MkData("name" -> "Bob")))
    writeAt(update0TS, Update(userRef, MkData("age" -> 42)))
    writeAt(update1TS, Update(userRef, MkData("name" -> "Bob L")))
    writeAt(deleteTS, DeleteF(userRef))
    writeAt(recreateTS, CreateF(userRef, MkData("name" -> "Bob L", "age" -> 42)))
    test(userRef)
  }

  def withSimpleTTLedHistory[A](test: JSValue => A) = {
    val userTTLID = ctx ! Query.nextID
    val userTTLRef = RefV(userTTLID, ClsRefV("users"))

    writeAt(
      createTS,
      CreateF(
        userTTLRef,
        MkObject(
          "data" -> MkObject("name" -> "Alice"),
          "ttl" -> TS(update0TS.toString)
        )))

    test(userTTLRef)
  }

  def withComplexSetHistory[A](test: => A) = {
    val cookSarahID = ctx ! Query.nextID
    val cookSarahRef = RefV(cookSarahID, ClsRefV("users"))
    writeAt(
      createTS,
      CreateF(cookSarahRef, MkData("name" -> "Sarah", "job" -> "cook"))
    )

    val chefSarahID = ctx ! Query.nextID
    val chefSarahRef = RefV(chefSarahID, ClsRefV("users"))
    writeAt(
      createTS,
      CreateF(chefSarahRef, MkData("name" -> "Sarah", "job" -> "chef"))
    )

    val cookTomID = ctx ! Query.nextID
    val cookTomRef = RefV(cookTomID, ClsRefV("users"))
    writeAt(createTS, CreateF(cookTomRef, MkData("name" -> "Tom", "job" -> "cook")))
    writeAt(deleteTS, DeleteF(cookTomRef))

    val chefTomID = ctx ! Query.nextID
    val chefTomRef = RefV(chefTomID, ClsRefV("users"))
    writeAt(createTS, CreateF(chefTomRef, MkData("name" -> "Tom", "job" -> "chef")))

    test
  }

  after {
    CassandraHelper.setCacheContext(ctx)
    CassandraHelper.invalidateCaches(ctx)
  }

  def MkData(fields: (String, JSValue)*) =
    MkObject("data" -> MkObject(fields: _*))

  def writeAt(txnTS: Timestamp, body: JSObject, auth: Auth = auth)(
    implicit pos: org.scalactic.source.Position) = {
    val query = evalQuery(auth, txnTS, body)
    getWrites(ctx, query, txnTS) match {
      case (Success(v), writes) =>
        applyWritesAt(txnTS, writes)
        v
      case (Failure(err), _) => fail(err.getMessage, err)
    }
  }

  def qassertAt(snapTS: Timestamp)(query: JSObject)(implicit pos: Position) =
    withClue(s"snapTS=$snapTS, query=$query") {
      try {
        // Discard writes so that this query can be repeated after compaction.
        runQueryAt(snapTS, discardWrites = true)(query).value shouldBe TrueL
      } catch {
        case NonFatal(err) =>
          info(s"Query at line ${pos.lineNumber} failed ** BEFORE ** compaction !!!")
          throw err
      }

      runCompactions()

      try {
        runQueryAt(snapTS)(query).value shouldBe TrueL
      } catch {
        case NonFatal(err) =>
          info(s"Query at line ${pos.lineNumber} failed ** AFTER ** compaction !!!")
          throw err
      }

      info(s"Assert at line ${pos.lineNumber} ran before and after compaction.")
    }

  def runQueryAt(snapTS: Timestamp, discardWrites: Boolean = false)(
    query: JSObject) = {
    // Invalidate cache to recalculate collection's MVT at the given snapshot time.
    val ctx0 = ctx.withStaticSnapshotTime(snapTS)
    CassandraHelper.setCacheContext(ctx0)
    CassandraHelper.invalidateCaches(ctx0)

    if (discardWrites) {
      getWrites(ctx0, evalQuery(auth, snapTS, query), snapTS) match {
        case (Success(Right(res)), _) => RepoContext.Result(snapTS, res)
        case (Success(Left(errs)), _) => fail(errs.mkString("\n"))
        case (Failure(err), _)        => fail(err.getMessage, err)
      }
    } else {
      ctx0 !! runQuery(auth, snapTS, query)
    }
  }

  private def runCompactions() = {
    def compact(cfName: String) = {
      val store = keyspace.getColumnFamilyStore(cfName)
      store.forceBlockingFlush()

      val sstables = store.getSSTables()
      val maxSSTableBytes = store.getCompactionStrategy.getMaxSSTableBytes

      // NB. Recompact all sstables into a single file at L0. We're not concerned
      // with the leveling structure as much as we're concerned with compacting all
      // available cells for a given column family so that expired cells can be
      // removed in a single compaction.
      val candidate =
        new LeveledManifest.CompactionCandidate(
          sstables,
          0 /* level */,
          maxSSTableBytes
        )

      val filter: CollectionFilter.Builder =
        cfName match {
          case Tables.Versions.CFName        => VersionsFilter.apply
          case Tables.SortedIndex.CFName     => SortedIndexFilter.apply
          case Tables.HistoricalIndex.CFName => HistoricalIndexFilter.apply
          case _                             => fail(s"no filter for cf: $cfName")
        }

      val task =
        new CollectionTask(
          store,
          candidate,
          Int.MaxValue, // gcBefore
          CollectionStrategy.hints,
          filter
        )

      store.getDataTracker.markCompacting(sstables) shouldBe true
      noException shouldBe thrownBy {
        FBUtilities.waitOnFuture(CompactionManager.instance.submitTask(task))
      }
    }

    columnFamilies foreach { cfName =>
      eventually(timeout(1.minute)) {
        compact(cfName)
      }
    }
  }

  def userEqual(ref: JSValue, data: JSObject) =
    Let(
      "user" -> Get(ref),
      "data" -> Select("data", Var("user"))
    ) {
      Equals(Var("data"), data)
    }

  def userHistory(ref: JSValue, data: JSArray) =
    Let(
      "page" -> Paginate(Events(ref)),
      "history" -> MapF(
        Lambda(
          "event" ->
            JSArray(
              Select("action", Var("event")),
              Select("data", Var("event"))
            )),
        Var("page"))
    ) {
      Equals(Select("data", Var("history")), data)
    }

  def usersByName(name: String, data: JSArray) = {
    def paginate(set: JSValue) =
      Let(
        "users" -> MapF(Lambda("ref" -> Get(Var("ref"))), Paginate(set)),
        "data" -> MapF(Lambda("user" -> Select("data", Var("user"))), Var("users"))
      ) {
        Select("data", Var("data"))
      }

    val set = Match("users_by_name", name)

    And(
      Equals(paginate(set), data),
      Equals(paginate(Reverse(set)), Reverse(data))
    )
  }

  def pairwiseHistory(name: String, data: JSArray) =
    Let(
      "page" -> MapF(
        Lambda(
          "event" ->
            Let(
              "ts" -> Select("ts", Var("event")),
              "ref" -> Select("document", Var("event")),
              "user" -> If(
                Exists(Var("ref"), Var("ts")),
                Get(Var("ref"), Var("ts")),
                JSNull
              )
            ) {
              JSArray(
                Select("action", Var("event")),
                Select("data", Var("user"), JSNull)
              )
            }),
        Paginate(Events(Match("users_by_name", name)))
      )
    ) {
      Equals(Select("data", Var("page")), data)
    }
}
