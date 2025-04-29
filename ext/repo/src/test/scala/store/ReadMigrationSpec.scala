package fauna.repo.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, ScalarType }
import fauna.repo.schema.migration.{ Migration, MigrationList }
import fauna.repo.schema.DataMode
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType
import fauna.repo.Store
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.storage.ops.VersionAdd

class ReadMigrationSpec extends Spec {

  val ctx = CassandraHelper.context("repo")
  var scope: ScopeID = _
  val coll = CollectionID(1024)
  before {
    scope = ScopeID(ctx.nextID())
  }

  def ID(id: Long) = DocID(SubID(id), coll)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  def create(
    id: DocID,
    ts: Timestamp,
    data: MapV,
    diff: Option[MapV] = None,
    schemaVersion: SchemaVersion = SchemaVersion.Min) =
    VersionAdd(
      scope,
      id,
      Resolved(ts, ts),
      Create,
      schemaVersion,
      data.toData,
      diff.map(_.toDiff))
  def delete(
    id: DocID,
    ts: Timestamp,
    diff: Option[MapV] = None,
    schemaVersion: SchemaVersion = SchemaVersion.Min) =
    VersionAdd(
      scope,
      id,
      Resolved(ts, ts),
      Delete,
      schemaVersion,
      Data.empty,
      diff.map(_.toDiff))

  "migrate on read" in {
    val migrations = MigrationList(
      SchemaVersion(1) -> Migration.SplitField(
        ConcretePath("data", "foo"),
        ConcretePath("data", "bar"),
        ScalarType.Int,
        5,
        NullV)
    )
    val schema = CollectionSchema
      .empty(scope, coll)
      .copy(migrations = migrations)

    // Before the migration, history looked like
    // 1. Create data -> {}
    // 2. Update data -> { data: { foo: 6 } }  diff -> { data: { foo: null } }
    // 3. Delete                               diff -> { data: { foo: 6 } }
    //
    // After the migration, history should look like
    // 1. Create data -> { data: { foo: 5 } }
    // 2. Update data -> { data: { foo: 6 } }  diff -> { data: { foo: 5 } }
    // 3. Delete                               diff -> { data: { foo: 6 } }
    val add = create(ID(1), TS(10), MapV.empty, None)
    val update = create(
      ID(1),
      TS(15),
      MapV("data" -> MapV("foo" -> 6)),
      Some(MapV("data" -> MapV("foo" -> NullV))))
    val remove = delete(ID(1), TS(20), Some(MapV("data" -> MapV("foo" -> 6))))

    ctx ! Query.write(add)
    ctx ! Query.write(update)
    ctx ! Query.write(remove)

    val versions = (ctx ! Store.versions(schema, ID(1)).flattenT).reverse

    val v0 = versions(0)
    val v1 = versions(1)
    val v2 = versions(2)

    v0.data.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
    v0.diff shouldBe None

    v1.data.fields.get(List("data", "foo")) shouldBe Some(LongV(6))
    v1.diff.get.fields.get(List("data", "foo")) shouldBe Some(LongV(5))

    v2.data.fields.get(List("data", "foo")) shouldBe None
    v2.diff.get.fields.get(List("data", "foo")) shouldBe Some(LongV(6))
  }

  "migrate diffs for deletes" in {
    val migrations = MigrationList(
      SchemaVersion(1) -> Migration.AddField(ConcretePath("data", "foo"), Int, 5)
    )
    val schema = CollectionSchema
      .empty(scope, coll)
      .copy(migrations = migrations)

    // Before the migration, history looked like
    // 1. Create data -> {}
    // 2. Update data -> {}  diff -> {}
    // 3. Delete diff -> {}
    //
    // After the migration, history should look like
    // 1. Create data -> { data: { foo: 5 } }
    // 2. Create data -> { data: { foo: 5 } }  diff -> {}
    // 3. Delete diff -> { data: { foo: 5 } }
    val add = create(ID(1), TS(10), MapV.empty, None)
    val update = create(ID(1), TS(15), MapV.empty, Some(MapV.empty))
    val remove = delete(ID(1), TS(20), Some(MapV.empty))

    ctx ! Query.write(add)
    ctx ! Query.write(update)
    ctx ! Query.write(remove)

    val versions = (ctx ! Store.versions(schema, ID(1)).flattenT).reverse

    val v0 = versions(0)
    val v1 = versions(1)
    val v2 = versions(2)

    v0.data.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
    v0.diff shouldBe None

    v1.data.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
    v1.diff.get.fields.get(List("data", "foo")) shouldBe None

    v2.data.fields.get(List("data", "foo")) shouldBe None
    v2.diff.get.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
  }

  "migrations ignore versions with a higher schema version" in {
    val migrations = MigrationList(
      SchemaVersion(1) -> Migration.AddField(ConcretePath("data", "foo"), Int, 5)
    )
    val schema = CollectionSchema
      .empty(scope, coll)
      .copy(migrations = migrations)

    val add = create(ID(1), TS(10), MapV.empty, None)

    // `AddField` will blow up it if attempts to migrate a version containing this
    // field, so this test not blowing up means this migration is definitely being
    // skipped for this version.
    val update = create(
      ID(1),
      TS(15),
      MapV("data" -> MapV("foo" -> 10)),
      // this is the diff to the _migrated_ previous version
      Some(MapV("data" -> MapV("foo" -> 5))),
      schemaVersion = SchemaVersion(1)
    )

    // As part of writing the above update, this version will be rewritten and
    // migrated in the process.
    val remove = delete(
      ID(1),
      TS(20),
      Some(MapV("data" -> MapV("foo" -> 10))),
      schemaVersion = SchemaVersion(1))

    ctx ! Query.write(add)
    ctx ! Query.write(update)
    ctx ! Query.write(remove)

    val versions = (ctx ! Store.versions(schema, ID(1)).flattenT).reverse

    val v0 = versions(0)
    val v1 = versions(1)
    val v2 = versions(2)

    // This got migrated
    v0.data.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
    v0.diff shouldBe None

    // This remains unchanged
    v1.data.fields.get(List("data", "foo")) shouldBe Some(LongV(10))
    v1.diff.get.fields.get(List("data", "foo")) shouldBe Some(LongV(5))

    // This remains unchanged
    v2.data.fields.get(List("data", "foo")) shouldBe None
    v2.diff.get.fields.get(List("data", "foo")) shouldBe Some(LongV(10))
  }

  "writing migrates the previous version to determine the diff" in {
    val migrations = MigrationList(
      SchemaVersion(1) -> Migration.AddField(ConcretePath("data", "foo"), Int, 5)
    )
    val schema = CollectionSchema
      .empty(scope, coll)
      .copy(
        struct = SchemaType.StructSchema(Map("data" -> ScalarType.Any)),
        migrations = migrations)

    val add = create(ID(1), TS(10), MapV.empty, None)

    ctx ! Query.write(add)
    // `v0`, above, has empty data. So this call will migrate v0 to the latest schema
    // version, and the compute the diff.
    ctx ! Store.externalUpdate(
      schema,
      ID(1),
      DataMode.PlainData,
      MapV("foo" -> 10).toDiff)

    val versions = (ctx ! Store.versions(schema, ID(1)).flattenT).reverse

    val v0 = versions(0)
    val v1 = versions(1)

    // This got migrated
    v0.data.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
    v0.diff shouldBe None

    // This remains unchanged, as the `Store.externalUpdate` should write the latest
    // schema version.
    v1.data.fields.get(List("data", "foo")) shouldBe Some(LongV(10))
    v1.diff.get.fields.get(List("data", "foo")) shouldBe Some(LongV(5))
  }
}
