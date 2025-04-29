package fauna.model.test

import fauna.ast._
import fauna.atoms.{ DocID, SchemaVersion }
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.CollectionConfig
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.repo.store.VersionStore
import fauna.repo.test.CassandraHelper
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ir._
import org.scalatest.PrivateMethodTester._
import scala.util.Random

class DocumentRepairSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val exec = TaskExecutor(ctx)
  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  def readCells(schema: CollectionSchema, id: DocID) =
    ctx ! Store
      .versions(schema, id, VersionID.MaxValue, VersionID.MinValue, 10000, false)
      .flattenT

  def checkVersions(versions: Seq[Version]) = {
    val root = versions reduceLeft { (hi, lo) =>
      hi.diff match {
        case Some(diff) => hi.fields.patch(diff) should equal(lo.fields)
        case None       => lo.isDeleted should equal(true)
      }
      lo
    }

    root.diff should equal(None)
  }

  def processAllTasks(ctx: RepoContext, exec: Executor): Unit = {
    val tasks =
      Task.getRunnableByHost(CassandraService.instance.localID.get).flattenT

    while ((ctx ! tasks).nonEmpty) {
      exec.step()
    }
  }

  "DocumentRepair" - {

    "repairs the indexes for a series of versions" in {
      val cls =
        ctx ! mkCollection(auth, MkObject("name" -> "broken", "history_days" -> 30))

      val id = ID(1, cls)

      val indexer = ctx ! Index.getIndexer(scope, cls)

      val addVersion = PrivateMethod[Query[Unit]](Symbol("addVersion"))

      def idxRow(ts: Long, live: Boolean) = {
        val vers = if (live) {
          Version.Live(
            scope,
            id,
            Resolved(TS(ts), TS(ts)),
            Create,
            SchemaVersion.Min,
            Data.empty)
        } else {
          Version.Deleted(
            scope,
            id,
            Resolved(TS(ts), TS(ts)),
            SchemaVersion.Min,
            Some(Diff.empty))
        }

        val rowsQ = Store.build(vers, indexer)
        val versQ = VersionStore invokePrivate addVersion(vers)
        ctx !! Seq(rowsQ, versQ).join
      }

      idxRow(6, false)
      idxRow(5, true)
      idxRow(4, false)
      idxRow(3, false)
      idxRow(2, true)
      idxRow(1, true)

      val eventsQ = events(auth, Documents(ClassRef("broken")))

      (ctx ! eventsQ).elems should equal(
        Seq(
          SetEventL(SetEvent(AtValid(TS(1)), scope, id, Add)),
          SetEventL(SetEvent(AtValid(TS(2)), scope, id, Add)),
          SetEventL(SetEvent(AtValid(TS(3)), scope, id, Remove)),
          SetEventL(SetEvent(AtValid(TS(4)), scope, id, Remove)),
          SetEventL(SetEvent(AtValid(TS(5)), scope, id, Add)),
          SetEventL(SetEvent(AtValid(TS(6)), scope, id, Remove))
        ))

      ctx ! DocumentRepair.Root.create()
      processAllTasks(ctx, exec)

      val repaired = ctx ! ModelStore.versions(scope, id).flattenT

      val evs = repaired map { v =>
        SetEvent(v.ts, scope, id, v.action, Vector.empty)
      }

      val expected =
        evs.reverse.foldLeft((None: Option[SetEvent], Seq.empty[SetEventL])) {
          case ((None, evs), ev) =>
            (Some(ev), evs :+ SetEventL(ev.copy(ts = AtValid(ev.ts.validTS))))
          case ((Some(prev), evs), ev) if prev.action == ev.action => (Some(ev), evs)
          case ((_, evs), ev) =>
            (Some(ev), evs :+ SetEventL(ev.copy(ts = AtValid(ev.ts.validTS))))
        }

      (ctx ! eventsQ).elems should equal(expected._2)

      checkVersions(repaired)
      readCells(CollectionSchema.empty(scope, id.collID), id).size should equal(
        repaired.size)
    }

    "repair does not modify version data" in {
      val cls = ctx ! mkCollection(
        auth,
        MkObject("name" -> "immutable", "history_days" -> 30))
      val id = ID(1, cls)
      val field = Field[String]("foo")

      val ops = for (ts <- 1 to 100) yield {
        if (Random.nextBoolean()) {
          getWrites(
            ctx,
            Store.insertCreateUnmigrated(
              scope,
              id,
              TS(ts),
              Data(field -> (Random.alphanumeric take 10).mkString)))
        } else {
          getWrites(ctx, Store.insertDeleteUnmigrated(scope, id, TS(ts)))
        }
      }

      val versions = ops map { case (version, _) => version.fold(throw _, identity) }

      ctx ! (Random.shuffle(ops) flatMap { _._2 })

      ctx ! DocumentRepair.Root.create()
      processAllTasks(ctx, exec)

      val repaired =
        ctx ! ModelStore.versions(scope, versions.head.docID).flattenT

      // repair compresses redundant deletes; do so here as well
      val (_, expected) =
        versions.foldLeft((None: Option[Version], Seq.empty[Version])) {
          case ((None, acc), v) => (Some(v), acc :+ v)
          case ((Some(prev), acc), v) if prev.isDeleted && v.isDeleted =>
            (Some(v), acc)
          case ((_, acc), v) => (Some(v), acc :+ v)
        }

      repaired.zip(expected.reverse) foreach { case (a, b) =>
        a should equalVersion(b)
      }

      checkVersions(repaired)
      readCells(CollectionSchema.empty(scope, id.collID), id).size should equal(
        repaired.size)
    }

    "repair does not produce redundant work" in {
      val cls = ctx ! mkCollection(
        auth,
        MkObject("name" -> "redundant", "history_days" -> 30))
      val id = ID(1, cls)
      val field = Field[String]("foo")

      for (ts <- 1 to 100) yield {
        val data = Data(field -> (Random.alphanumeric take 10).mkString)
        ctx ! Store.insertCreateUnmigrated(scope, id, TS(ts), data)
      }

      val emptySchema = CollectionSchema.empty(scope, id.collID)
      val versions = (ctx ! Store.versions(emptySchema, id).flattenT).toList

      ctx ! DocumentRepair.Root.create()
      processAllTasks(ctx, exec)

      val repaired = (ctx ! Store.versions(emptySchema, id).flattenT).toList

      versions should equal(repaired)
      readCells(emptySchema, id).size should equal(repaired.size)
    }

    "repairs diffs for concurrent updates" in {
      val cls = ctx ! mkCollection(
        auth,
        MkObject("name" -> "concurrent", "history_days" -> 30))
      val id = ID(1, cls)
      val terms = Seq("foo", "bar", "baz", "qux")
      val field = Field[String]("data", "idx")

      ctx ! mkIndex(auth, "concurrent", "concurrent", List(field.path))

      ctx ! writesWithSchemaOCC(scope) {
        val schema = ctx ! CollectionConfig(scope, cls).map { _.get.Schema }
        val ops = for {
          ts   <- 1 to 10
          term <- terms
        } yield {
          val q = Store.insertCreate(schema, id, TS(ts), Data(field -> term))
          getWrites(ctx, q)
        }
        ops.flatMap { _._2 }
      }

      terms foreach { term =>
        val es = ctx ! events(auth, Match(IndexRef("concurrent"), term))
        es.elems.size should equal(10)
      }

      val emptySchema = CollectionSchema.empty(scope, id.collID)
      (ctx ! Store.versions(emptySchema, id).flattenT) foreach { version =>
        version.diff.isEmpty should be(true)
      }

      ctx ! DocumentRepair.Root.create()
      processAllTasks(ctx, exec)

      val repaired =
        ctx ! Store.versions(emptySchema, id).flattenT

      repaired.size should equal(10)

      // not part of the diff to the previous version
      val mask = Diff(MapV("ref" -> NullV, "class" -> NullV))
      repaired.tail.reverse.foldLeft(repaired.last) { (prev, version) =>
        version.prevData() foreach { _ should equal(prev.fields.patch(mask)) }
        version
      }

      checkVersions(repaired)
      readCells(emptySchema, id).size should equal(repaired.size)
    }
  }
}
