package fauna.storage.api

import fauna.atoms._
import fauna.exec.ImmediateExecutionContext
import fauna.lang._
import fauna.lang.syntax._
import fauna.prop._
import fauna.scheduler.ConstantPriorityProvider
import fauna.stats.StatsRecorder
import fauna.storage.{ Storage => CStorage, _ }
import fauna.storage.api.scan.{ DocScan, ElementScan, KeyScan }
import fauna.storage.api.set.{
  Element,
  SetHistory,
  SetSnapshot,
  SetSortedValues,
  SparseSetSnapshot
}
import fauna.storage.api.version.{ DocHistory, DocSnapshot, StorageVersion }
import fauna.storage.doc.{ Data, Diff, Field }
import fauna.storage.ops._
import fauna.storage.test.{ Spec => StorageSpec }
import org.scalactic.source.Position
import org.scalatest._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random

package test {

  // We used to have fauna.storage.api.version.Version.{ Live, Deleted },
  // but they weren't really used outside of storage API tests. This is
  // a quick way to remove them from the main codebase without too much
  // work redoing tests.
  sealed trait TestVersion {
    def scopeID: ScopeID
    def docID: DocID
    def validTS: Option[Timestamp]

    def action = this match {
      case v: TestVersion.Live =>
        if (v.diff.isEmpty) Create else Update
      case _: TestVersion.Deleted =>
        Delete
    }
  }

  object TestVersion {
    final case class Live(
      scopeID: ScopeID,
      docID: DocID,
      validTS: Option[Timestamp],
      ttl: Option[Timestamp],
      data: Data,
      diff: Option[Diff],
      isChange: Boolean = false
    ) extends TestVersion

    final case class Deleted(
      scopeID: ScopeID,
      docID: DocID,
      validTS: Option[Timestamp],
      diff: Option[Diff],
      isChange: Boolean = false
    ) extends TestVersion

    // Decode the data and diff, producing a fullyfledged version.
    implicit def fromVersion(v: StorageVersion): TestVersion = {
      v.action match {
        case Delete =>
          TestVersion.Deleted(
            v.scopeID,
            v.docID,
            v.ts.validTSOpt,
            v.diff,
            v.isChange)
        case Create | Update =>
          TestVersion.Live(
            v.scopeID,
            v.docID,
            v.ts.validTSOpt,
            v.ttl,
            v.data,
            v.diff,
            v.isChange)
      }
    }
  }

  // Not all tests need a Cassandra setup, but most do, and it's fast, so for
  // now every test gets a Cassandra setup.
  abstract class Spec(override val keyspaceName: String)
      extends StorageSpec(keyspaceName)
      with OptionValues
      with Inside {

    val Timeout = 5.seconds
    val HostFlags = () => FutureNone

    override def beforeEach() = {
      super.beforeEach()
      CStorage(
        keyspaceName,
        Seq(
          Tables.RowTimestamps.Schema,
          Tables.Versions.Schema,
          Tables.HistoricalIndex.Schema,
          Tables.SortedIndex.Schema,
          Tables.HealthChecks.Schema,
          Tables.SchemaVersions.Schema
        )
      )
        // Overrides off allows each test to be in its own keyspace,
        // so scans don't see other tests' data.
        .init(overrideIDs = false)
    }

    // Reused in a few places in subclass tests.
    val createData = Data(Field[String]("version") -> "create")
    val updateData = Data(Field[String]("version") -> "update")
    val recreateData = Data(Field[String]("version") -> "re-create")

    def storageProp(name: String, testTags: Tag*)(f: StorageEngine => Prop[Unit])(
      implicit conf: PropConfig): Unit =
      test(name, testTags: _*) {
        withStorageEngine(StatsRecorder.Null) {
          f(_).test(conf)
        }
      }

    def deadline: TimeBound = Timeout.bound

    def await[A](f: Future[A]): A =
      Await.result(f, Timeout)

    def reverseIfDescending[R](order: Order, vs: Seq[R]) = order match {
      case Order.Ascending  => vs
      case Order.Descending => vs.reverse
    }

    def applyWrites(
      engine: StorageEngine,
      txnTS: Timestamp,
      writes: Write*): Unit = {

      val reads = Map.empty[TxnRead, Timestamp]
      val res = await(engine.evalTxnApply(txnTS, reads, (reads, writes.toVector)))
      res shouldBe StorageEngine.TxnSuccess
    }

    def runRead[R <: Read.Result](
      engine: StorageEngine,
      op: Read[R],
      writes: Iterable[Write] = Iterable.empty
    )(implicit pos: Position): R = {
      // Moves the last applied TS beyond the snapshot time to avoid LAT waits.
      val lastAppliedTS =
        op.snapshotTS.max(engine.appliedTimestamp) +
          Random.between(0, 500).millis

      engine.updateMaxAppliedTimestamp(lastAppliedTS)

      implicit val ec = ImmediateExecutionContext
      val ctx = new Storage.Context(ConstantPriorityProvider, engine, HostFlags)
      val readF = Storage(ctx).read(HostID.NullID, op.scopeID, op, writes, deadline)
      val res = await(readF)
      res.lastAppliedTS shouldBe >=(lastAppliedTS)
      res
    }

    def runDocSnapshot(
      engine: StorageEngine,
      op: DocSnapshot,
      writes: Iterable[Write] = Iterable.empty
    )(implicit pos: Position): Option[TestVersion.Live] =
      runRead(engine, op, writes).version flatMap { rv =>
        TestVersion.fromVersion(rv) match {
          case v: TestVersion.Live => Some(v)
          case _                   => None
        }
      }

    def runDocHistory(
      engine: StorageEngine,
      op: DocHistory,
      writes: Iterable[Write] = Iterable.empty
    )(implicit pos: Position): (Vector[TestVersion], Option[DocHistory]) = {
      val DocHistory.Result(versions, next, _, _, _) =
        runRead(engine, op, writes)
      (versions map { TestVersion.fromVersion(_) }, next)
    }

    def runSetSortedValues(
      engine: StorageEngine,
      op: SetSortedValues,
      writes: Iterable[Write] = Iterable.empty
    ): (Vector[Element], Option[SetSortedValues]) = {
      val SetSortedValues.Result(elems, next, _, _, _) = runRead(engine, op, writes)
      (elems, next)
    }

    def runSetSnapshot(
      engine: StorageEngine,
      op: SetSnapshot,
      writes: Iterable[Write] = Iterable.empty
    ): (Vector[Element.Live], Option[SetSnapshot]) = {
      val SetSnapshot.Result(elems, next, _, _, _) = runRead(engine, op, writes)
      (elems, next)
    }

    def runSparseSetSnapshot(
      engine: StorageEngine,
      op: SparseSetSnapshot,
      writes: Iterable[Write] = Iterable.empty
    ): Vector[Element.Live] = {
      val SparseSetSnapshot.Result(elems, _, _, _) = runRead(engine, op, writes)
      elems
    }

    def runSetHistory(
      engine: StorageEngine,
      op: SetHistory,
      writes: Iterable[Write] = Iterable.empty)
      : (Vector[Element], Option[SetHistory]) = {
      val SetHistory.Result(elems, next, _, _, _) = runRead(engine, op, writes)
      (elems, next)
    }

    def runHealthChecks(
      engine: StorageEngine,
      op: HealthChecks): Vector[(HostID, Timestamp)] = {
      val HealthChecks.Result(values, _, _, _) = runRead(engine, op, Iterable.empty)
      values
    }

    def runSchemaVersion(
      engine: StorageEngine,
      op: SchemaVersionSnapshot): Timestamp = {
      val SchemaVersionSnapshot.Result(ts, _, _) = runRead(engine, op, Vector.empty)
      ts
    }

    def runScan[R](
      engine: StorageEngine,
      op: Scan[R]
    ): R = {
      // Moves the last applied TS beyond the snapshot time to avoid LAT waits.
      val lastAppliedTS =
        op.snapshotTS.max(engine.appliedTimestamp) +
          Random.between(0, 500).millis

      engine.updateMaxAppliedTimestamp(lastAppliedTS)

      implicit val ec = ImmediateExecutionContext
      val ctx = new Storage.Context(ConstantPriorityProvider, engine, HostFlags)
      val res = await(Storage(ctx).scan(ScopeID.RootID, op, deadline))
      res
    }

    def runDocScan(
      engine: StorageEngine,
      op: DocScan): (Vector[TestVersion], Option[ScanSlice]) = {
      val DocScan.Result(values, next) = runScan(engine, op)
      (values map { TestVersion.fromVersion(_) }, next)
    }

    def runKeyScan(
      engine: StorageEngine,
      op: KeyScan): (Vector[KeyScan.Entry], Option[ScanSlice]) = {
      val KeyScan.Result(values, next) = runScan(engine, op)
      (values, next)
    }

    def runElementScan(
      engine: StorageEngine,
      op: ElementScan): (Vector[ElementScan.Entry], Option[ElementScan.Cursor]) = {
      val ElementScan.Result(values, next) = runScan(engine, op)
      (values, next)
    }

    // Some closures that save a bit of typing.
    def docSnapshotFactory(scopeID: ScopeID, docID: DocID)(
      start: Timestamp,
      limit: Timestamp) = {
      val diff = limit.difference(start).toMillis
      val snapshotTS = start + Random.between(0, diff).millis
      DocSnapshot(scopeID, docID, snapshotTS)
    }

    def docHistoryFactory(
      scopeID: ScopeID,
      docID: DocID,
      order: Order = Order.Descending)(max: Timestamp, min: Timestamp) =
      DocHistory(scopeID, docID, max, min, max, order = order)

    def writeFactory(engine: StorageEngine, scopeID: ScopeID, docID: DocID) = (
      validTS: Timestamp,
      txnTS: Timestamp,
      action: DocAction,
      data: Data,
      diff: Option[Diff]) =>
      applyWrites(
        engine,
        validTS,
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = Resolved(validTS, txnTS),
          action = action,
          schemaVersion = SchemaVersion.Min,
          data = data,
          diff = diff
        ))

    def tombstoneFactory(engine: StorageEngine, scopeID: ScopeID, docID: DocID) =
      (validTS: Timestamp, txnTS: Timestamp, action: DocAction) =>
        applyWrites(
          engine,
          validTS,
          VersionRemove(
            scope = scopeID,
            id = docID,
            writeTS = Resolved(validTS, txnTS),
            action = action
          ))

    def idsP =
      for {
        scope <- Prop.long
        sub   <- Prop.long
        coll  <- Prop.long(1 << 16) // CollectionIDs are 2 bytes max.
      } yield (ScopeID(scope), DocID(SubID(sub), CollectionID(coll)))

    def indexP = Prop.long map { IndexID(_) }

    case class BasicHistory(
      create: TestVersion.Live,
      update: TestVersion.Live,
      delete: TestVersion.Deleted,
      recreate: TestVersion.Live
    )

    // A clumsy helper to set up a basic document history used in multiple tests.
    // There are five regions of history:
    //
    // -> creation
    // creation -> update
    // update -> deletion
    // deletion -> recreation
    // recreation ->
    //
    // In each region where a document exists, the data for the document consists
    // of a single field "version" with value describing the change, e.g. between
    // creation and update the document is `{ "version" : "create" }`.
    def createBasicHistory(
      engine: StorageEngine,
      scopeID: ScopeID,
      docID: DocID,
      createTS: Timestamp,
      updateTS: Timestamp,
      deleteTS: Timestamp,
      recreateTS: Timestamp) = {

      def va(ts: Timestamp, action: DocAction, data: Data, diff: Option[Diff]) =
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = Resolved(ts),
          action = action,
          schemaVersion = SchemaVersion.Min,
          data = data,
          diff = diff)

      // Create.
      applyWrites(engine, createTS, va(createTS, Create, createData, None))
      val create =
        TestVersion.Live(scopeID, docID, Some(createTS), None, createData, None)

      // Update.
      val uDiff = updateData.diffTo(createData)
      applyWrites(engine, updateTS, va(updateTS, Update, updateData, Some(uDiff)))
      val update =
        TestVersion.Live(
          scopeID,
          docID,
          Some(updateTS),
          None,
          updateData,
          Some(uDiff))

      // Delete.
      val dDiff = Diff(updateData.fields)
      applyWrites(engine, deleteTS, va(deleteTS, Delete, Data.empty, Some(dDiff)))
      val delete = TestVersion.Deleted(scopeID, docID, Some(deleteTS), Some(dDiff))

      // Recreate.
      applyWrites(engine, recreateTS, va(recreateTS, Create, recreateData, None))
      val recreate =
        TestVersion.Live(scopeID, docID, Some(recreateTS), None, recreateData, None)

      BasicHistory(create, update, delete, recreate)
    }
  }
}
