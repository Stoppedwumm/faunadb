package fauna.repo.test

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.prop._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.repo.store._
import fauna.repo.Store
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.storage.ops.VersionAdd

class VersionsSpec extends PropSpec {

  val ctx = CassandraHelper.context("repo")

  def newScope = Prop.const(ScopeID(ctx.nextID()))

  val faunaClass = CollectionID(1024)

  def ID(id: Long) = DocID(SubID(id), faunaClass)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  once("get resolves conflicts") {
    for {
      scope <- newScope
    } {
      val winner =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(1), TS(2)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          None)

      val loser =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(1), TS(1)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          None)

      ctx ! Query.write(loser)
      ctx ! Query.write(winner)

      val v = ctx ! Store.getUnmigrated(scope, ID(1), TS(3))
      v.nonEmpty should be(true)
      v.get.ts should be(Resolved(TS(1), TS(2)))

      // cleanup
      ctx ! VersionStore.clear(scope, ID(1))
    }
  }

  once("repairs missing diffs") {
    for {
      scope <- newScope
      emptySchema = CollectionSchema.empty(scope, faunaClass)
    } {
      val origin = VersionAdd(
        scope,
        ID(1),
        Resolved(TS(1), TS(1)),
        Create,
        SchemaVersion.Min,
        Data(MapV("field" -> StringV("a"))),
        None)

      val update =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(2), TS(2)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          None)

      ctx ! Query.write(origin)
      ctx ! Query.write(update)

      val versions = ctx ! Store.versions(emptySchema, ID(1)).flattenT

      versions.size should be(2)
      versions foreach { ch =>
        ch.diff.isEmpty should be(true)
      }

      val conflicts = Version.resolveConflicts(versions)

      conflicts.size should be(2) // no write conflict

      ctx ! VersionStore.repair(conflicts.head, conflicts.last)
      ctx ! VersionStore.repair(
        conflicts.last,
        Conflict(
          Version.Deleted(
            scope,
            ID(1),
            Resolved(Timestamp.Epoch, Timestamp.Epoch),
            SchemaVersion.Min),
          Nil))

      val repaired = ctx ! Store.versions(emptySchema, ID(1)).flattenT
      repaired.size should be(2)
      repaired.head.diff.get should be(
        Diff(MapV("ts" -> LongV(1), "field" -> StringV("a"))))
      repaired.last.diff.isEmpty should be(true)
    }
  }

  once("repairs deleted diffs") {
    for {
      scope <- newScope
      emptySchema = CollectionSchema.empty(scope, faunaClass)
    } {
      val delete =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(1), TS(1)),
          Delete,
          SchemaVersion.Min,
          Data.empty,
          None)

      val update = VersionAdd(
        scope,
        ID(1),
        Resolved(TS(2), TS(2)),
        Create,
        SchemaVersion.Min,
        Data.empty,
        Some(Diff(MapV("ts" -> LongV(1), "field" -> StringV("a")))))

      ctx ! Query.write(delete)
      ctx ! Query.write(update)

      val versions = ctx ! Store.versions(emptySchema, ID(1)).flattenT

      versions.size should be(2)
      versions.head.diff.isEmpty should be(false)

      val conflicts = Version.resolveConflicts(versions)

      conflicts.size should be(2) // no write conflict

      ctx ! VersionStore.repair(conflicts.head, conflicts.last)
      ctx ! VersionStore.repair(
        conflicts.last,
        Conflict(
          Version.Deleted(
            scope,
            ID(1),
            Resolved(Timestamp.Epoch, Timestamp.Epoch),
            SchemaVersion.Min),
          Nil))

      val repaired = ctx ! Store.versions(emptySchema, ID(1)).flattenT
      repaired.size should be(2)
      repaired foreach { c =>
        c.diff.isEmpty should be(true)
      }
    }
  }

  once("repairs incorrect diffs") {
    for {
      scope <- newScope
      emptySchema = CollectionSchema.empty(scope, faunaClass)
    } {
      val create = VersionAdd(
        scope,
        ID(1),
        Resolved(TS(1), TS(1)),
        Create,
        SchemaVersion.Min,
        Data(MapV("field" -> StringV("a"))),
        None)

      val update = VersionAdd(
        scope,
        ID(1),
        Resolved(TS(2), TS(2)),
        Create,
        SchemaVersion.Min,
        Data.empty,
        Some(Diff(MapV("field" -> StringV("b")))))

      ctx ! Query.write(create)
      ctx ! Query.write(update)

      val versions = ctx ! Store.versions(emptySchema, ID(1)).flattenT

      versions.size should be(2)

      val conflicts = Version.resolveConflicts(versions)

      conflicts.size should be(2) // no write conflict

      ctx ! VersionStore.repair(conflicts.head, conflicts.last)
      ctx ! VersionStore.repair(
        conflicts.last,
        Conflict(
          Version.Deleted(
            scope,
            ID(1),
            Resolved(Timestamp.Epoch, Timestamp.Epoch),
            SchemaVersion.Min),
          Nil))

      val repaired = ctx ! Store.versions(emptySchema, ID(1)).flattenT
      repaired.size should be(2)
      repaired.head.diff.get should be(
        Diff(MapV("ts" -> LongV(1), "field" -> StringV("a"))))
      repaired.last.diff.isEmpty should be(true)
    }
  }

  once("repairs meaningless deletes") {
    for {
      scope <- newScope
      emptySchema = CollectionSchema.empty(scope, faunaClass)
    } {
      val origin = Conflict(
        Version.Deleted(
          scope,
          ID(1),
          Resolved(Timestamp.Epoch, Timestamp.Epoch),
          SchemaVersion.Min),
        Nil)
      val once =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(1), TS(1)),
          Delete,
          SchemaVersion.Min,
          Data.empty,
          None)

      val twice =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(2), TS(2)),
          Delete,
          SchemaVersion.Min,
          Data.empty,
          None)

      ctx ! Query.write(once)
      ctx ! Query.write(twice)

      val versions = ctx ! Store.versions(emptySchema, ID(1)).flattenT

      versions.size should be(2)

      val conflicts = Version.resolveConflicts(versions)

      conflicts.size should be(2) // no write conflict

      ctx ! VersionStore.repair(conflicts.head, conflicts.last)
      ctx ! VersionStore.repair(conflicts.last, origin)

      val repaired = ctx ! Store.versions(emptySchema, ID(1)).flattenT
      repaired.size should be(1)
      repaired.head.ts should equal(Resolved(TS(1), TS(1)))
    }
  }

  once("ttl on read") {
    for {
      scope <- newScope
    } {
      val data = MapV("data" -> MapV("name" -> "sam"), "ttl" -> TimeV(TS(15))).toData

      val add =
        VersionAdd(
          scope,
          ID(1),
          Resolved(TS(10), TS(10)),
          Create,
          SchemaVersion.Min,
          data,
          None)

      ctx ! Query.write(add)

      (ctx ! Store.getUnmigrated(scope, ID(1), TS(1))) shouldBe None
      (ctx ! Store.getUnmigrated(scope, ID(1), TS(10))) shouldBe None
      (ctx ! Store.getUnmigrated(scope, ID(1), TS(15))) shouldBe None
      (ctx ! Store.getUnmigrated(scope, ID(1), TS(20))) shouldBe None
    }
  }
}
