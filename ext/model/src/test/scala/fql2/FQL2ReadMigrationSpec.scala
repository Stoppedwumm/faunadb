package fauna.model.test

import fauna.atoms._
import fauna.auth.{ AdminPermissions, Auth }
import fauna.lang.clocks.Clock
import fauna.model.{ Collection, Index }
import fauna.model.schema.SchemaCollection
import fauna.model.tasks.TaskExecutor
import fauna.repo.schema.migration.{ Migration, MigrationList }
import fauna.repo.schema.ScalarType
import fauna.repo.store.CacheStore
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.{ Add, Remove }
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fql.ast.Span

class FQL2ReadMigrationSpec extends FQL2Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "migrations" - {
    "can be read from internal collections" in {
      val doc = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]

      val migrations = MigrationList(
        SchemaVersion(1) -> Migration.AddField(
          ConcretePath("data", "foo"),
          ScalarType.Int,
          5),
        SchemaVersion(2) -> Migration.DropField(ConcretePath("data", "bar")),
        SchemaVersion(4) -> Migration.MoveField(
          ConcretePath("data", "foo"),
          ConcretePath("data", "bar")),
        SchemaVersion(4) -> Migration.SplitField(
          ConcretePath("data", "bar"),
          ConcretePath("data", "baz"),
          ScalarType.Str,
          "replace_str",
          100)
      )

      val cfg = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(
        cfg.Schema,
        doc,
        Diff(MapV("name" -> "Foo", "internal_migrations" -> migrations.encode)))

      val read = ctx ! Store.get(cfg.Schema, doc)
      val migrationsRead = read.get.data.fields.get(List("internal_migrations")).get
      MigrationList.decode(migrationsRead) shouldBe migrations
    }

    "doesn't show up when reading in v10" in {
      val doc = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]

      val migrations = MigrationList(
        SchemaVersion(2) -> Migration.DropField(ConcretePath("data", "bar")))

      val cfg = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(
        cfg.Schema,
        doc,
        Diff(MapV("internal_migrations" -> migrations.encode)))

      evalOk(
        auth,
        "Foo.definition.internal_migrations",
        typecheck = false).isNull shouldBe true
    }
  }

  "reads" - {
    "migrate version data" in {
      val collDoc = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]
      val collID = CollectionID(collDoc.subID.toLong)

      // The doc will be created with this version, so adding a migration for this
      // version won't affect it.
      val v1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).get.active.get.config.internalMigrations.latestVersion

      val doc = evalOk(auth, "Foo.create({ id: 1 })").as[DocID]
      evalOk(auth, "Foo.byId(1)!.update({ bar: 0 })")

      // Applying an update to bump the cached version.
      evalOk(auth, "Collection.byName('Foo')!.update({})")
      val newVers = (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get
      newVers should be > v1

      // Build a migrations list by hand using that new version. Internally, we use a
      // `TransactionTime` for the new version, but this is a little easier for
      // testing.
      val migrations = MigrationList(
        newVers -> Migration.AddField(ConcretePath("data", "foo"), ScalarType.Int, 5)
      )

      val cfg0 = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(
        cfg0.Schema,
        collDoc,
        Diff(MapV("internal_migrations" -> migrations.encode)))

      // Skip the cache. It's too flaky in these tests.
      val collRead = ctx ! Collection.getUncached(auth.scopeID, collID).map(_.get)
      val schema = collRead.active.get.config.Schema
      schema.migrations shouldBe migrations

      val get = ctx ! Store.get(schema, doc)
      get.get.data.fields.get(List("data", "foo")).get shouldBe LongV(5)

      val vs = ctx ! Store.versions(schema, doc).flattenT
      vs.size shouldBe 2
      vs.head.data.fields.get(List("data", "foo")).get shouldBe LongV(5)
      vs.tail.head.data.fields.get(List("data", "foo")).get shouldBe LongV(5)
    }

    "migrate version diffs" in {
      val collDoc = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]
      val collID = CollectionID(collDoc.subID.toLong)

      // The doc will be created with this version, so adding a migration for this
      // version won't affect it.
      val v1 =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          collID)).get.active.get.config.internalMigrations.latestVersion

      val doc = evalOk(auth, "Foo.create({ id: 1 })").as[DocID]
      evalOk(auth, "Foo.byId(1)!.update({})")
      evalOk(auth, "Foo.byId(1)!.update({ foo: 10 })")

      // Apply an update to bump the schema version.
      evalOk(auth, "Collection.byName('Foo')!.update({})")
      val newVers = (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get
      newVers should be > v1

      // This replaces any non-ints with `5`. This is the equivalent of migrating
      // from `Int | Null` to `Int` with a default of `5`.
      val migrations = MigrationList(
        newVers -> Migration.SplitField(
          ConcretePath("data", "foo"),
          ConcretePath("data", "ignore"),
          ScalarType.Int,
          5,
          0
        )
      )

      val cfg0 = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(
        cfg0.Schema,
        collDoc,
        Diff(MapV("internal_migrations" -> migrations.encode)))

      val get = ctx ! ModelStore.get(auth.scopeID, doc)
      get.get.data.fields.get(List("data", "foo")).get shouldBe LongV(10)

      // The diff should act like the previous version always had the field set to 5.
      get.get.diff.get.fields.get(List("data", "foo")).get shouldBe LongV(5)

      val vs = ctx ! ModelStore.versions(auth.scopeID, doc).flattenT
      vs.size shouldBe 3
      vs.head.diff.get.fields.get(List("data", "foo")).get shouldBe LongV(5)
      vs.tail.head.diff.get.fields.contains(List("data", "foo")) shouldBe false
      vs.tail.tail.head.diff shouldBe empty
    }
  }

  "diffs" - {
    "diffs are updated correctly on update" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    foo: {
           |      signature: "Int",
           |      default: "5"
           |    },
           |  }
           |})""".stripMargin
      )

      val doc = evalOk(auth, "Foo.create({ id: 0 })").as[DocID]

      val read0 = (ctx ! ModelStore.get(auth.scopeID, doc)).get
      read0.data.fields.get(List("data", "foo")).get shouldBe LongV(5)
      read0.data.fields.get(List("data", "bar")) shouldBe None

      evalOk(
        auth,
        """|Collection.byName('Foo')!.update({
           |  fields: {
           |    bar: {
           |      signature: "String",
           |      default: "'hi'"
           |    }
           |  },
           |  migrations: [
           |    { add: { field: ".bar" } }
           |  ]
           |})""".stripMargin
      )

      // This will migrate the stored document, and migrate the previous version, and
      // then re-compute the diff. So the diff for `bar` should be empty.
      evalOk(auth, "Foo.byId(0)!.update({ foo: 10 })")

      val read1 = (ctx ! ModelStore.get(auth.scopeID, doc)).get
      read1.data.fields.get(List("data", "foo")).get shouldBe LongV(10)
      read1.data.fields.get(List("data", "bar")).get shouldBe StringV("hi")

      read1.diff.get.fields.get(List("data", "foo")).get shouldBe LongV(5)
      read1.diff.get.fields.get(List("data", "bar")) shouldBe None
    }
  }

  "index builds" - {
    "read migrated versions in synchronous index builds" in {
      val collDoc = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]
      val collID = CollectionID(collDoc.subID.toLong)

      evalOk(auth, "Foo.create({ id: 1 })")
      evalOk(auth, "Foo.byId(1)!.update({ bar: 0 })")
      evalOk(auth, "Foo.byId(1)!.update({ foo: 10 })")

      evalOk(auth, "Collection.byName('Foo')!.update({})")
      val newVers = (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get

      val migrations = MigrationList(
        newVers -> Migration.SplitField(
          ConcretePath("data", "foo"),
          ConcretePath("data", "ignore"),
          ScalarType.Int,
          5,
          0
        )
      )

      val cfg0 = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(
        cfg0.Schema,
        collDoc,
        Diff(MapV("internal_migrations" -> migrations.encode)))

      // Add the index later, so the migrations are in place at build time.
      evalOk(
        auth,
        """|Collection.byName('Foo')!.update({
           |  indexes: {
           |    byFoo: { terms: [{ field: ".foo" }] }
           |  }
           |})""".stripMargin
      )

      val idx =
        (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)).head
      ctx ! Store.collection(idx, Vector(5), Clock.time).isEmptyT shouldBe true
      ctx ! Store.collection(idx, Vector(10), Clock.time).countT shouldBe 1

      // The history of the foo field post-migration (time incr) is 5 -> 5 -> 10.
      ctx ! Store
        .historicalIndex(idx, Vector(IndexTerm(5)))
        .mapValuesT(_.action)
        .flattenT shouldBe Vector(Remove, Add)
      ctx ! Store
        .historicalIndex(idx, Vector(IndexTerm(10)))
        .mapValuesT(_.action)
        .flattenT shouldBe Vector(Add)
    }

    val ctxScan = ctx.withReindexDocsCancelLimit(Index.BuildSyncSize)
    for ((name, ctx) <- Seq("reindex" -> ctx, "scan" -> ctxScan)) {
      s"read migrated versions in asynchronous $name builds" in {
        val collDoc = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]
        val collID = CollectionID(collDoc.subID.toLong)

        evalOk(auth, "Foo.create({ id: 1 })")
        evalOk(auth, "Foo.byId(1)!.update({ bar: 0 })")
        evalOk(auth, "Foo.byId(1)!.update({ foo: 10 })")

        // Add enough junk to trigger asynchronous builds.
        val qs = (1 to 2 * Index.BuildSyncSize) map { _ => "Foo.create({})" }
        evalOk(auth, qs.mkString(";"))

        evalOk(auth, "Collection.byName('Foo')!.update({})")
        val newVers = (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get

        val migrations = MigrationList(
          newVers -> Migration.SplitField(
            ConcretePath("data", "foo"),
            ConcretePath("data", "ignore"),
            ScalarType.Int,
            5,
            0
          )
        )

        val cfg0 = ctx ! SchemaCollection.Collection(auth.scopeID)
        ctx ! Store.internalUpdate(
          cfg0.Schema,
          collDoc,
          Diff(MapV("internal_migrations" -> migrations.encode)))

        evalOk(
          auth,
          """|Collection.byName('Foo')!.update({
            |  indexes: {
            |    byFoo: { terms: [{ field: ".foo" }] }
            |  }
            |})""".stripMargin
        )
        TaskHelpers.processAllTasks(ctx, TaskExecutor(ctx))

        val idx =
          (ctx ! Index.getUserDefinedBySourceUncached(auth.scopeID, collID)).head
        ctx ! Store
          .collection(idx, Vector(5), Clock.time)
          .countT shouldBe 2 * Index.BuildSyncSize
        ctx ! Store.collection(idx, Vector(10), Clock.time).countT shouldBe 1

        val es = ctx ! Store
          .historicalIndex(idx, Vector(IndexTerm(5)))
          .mapValuesT(_.action)
          .flattenT
        es should have size 2 * Index.BuildSyncSize + 2
        es.count(_ == Add) shouldBe 2 * Index.BuildSyncSize + 1
        ctx ! Store
          .historicalIndex(idx, Vector(IndexTerm(10)))
          .mapValuesT(_.action)
          .flattenT shouldBe Vector(Add)
      }
    }
  }

  "deriving" - {
    "migrations are derived from updates" in {
      val doc = evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    _no_wildcard: { signature: "Null" }
           |  }
           |})""".stripMargin
      ).as[DocID]
      // Make a doc to actually apply migrations.
      evalOk(auth, "Foo.create({})")

      val v1 = (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get

      evalOk(
        auth,
        """|Collection.byName('Foo')!.update({
           |  fields: {
           |    _no_wildcard: { signature: "Null" },
           |    foo: {
           |      signature: "Int",
           |      default: "5"
           |    },
           |  },
           |  migrations: [{ add: { field: ".foo" } }]
           |})""".stripMargin
      )

      val v2 = (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get
      v2 should be > v1

      val read = ctx ! ModelStore.get(auth.scopeID, doc)
      val migrationsRead = MigrationList.decode(
        read.get.data.fields.get(List("internal_migrations")).get)

      // Note that this contains the schema version after the update.
      migrationsRead shouldBe MigrationList(
        v2 -> Migration.AddField(ConcretePath("data", "foo"), ScalarType.Int, 5))
    }

    "empty collections get no migrations" in {
      val doc = evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    _no_wildcard: { signature: "Null" }
           |  }
           |})""".stripMargin
      ).as[DocID]

      evalOk(
        auth,
        """|Collection.byName('Foo')!.update({
           |  fields: {
           |    _no_wildcard: { signature: "Null" },
           |    foo: {
           |      signature: "Int",
           |      default: "5"
           |    },
           |  },
           |  migrations: [{ add: { field: ".foo" } }]
           |})""".stripMargin
      )

      val read = ctx ! ModelStore.get(auth.scopeID, doc)
      read.get.data.fields.get(List("internal_migrations")) shouldBe None
    }
  }

  "put it all together" - {
    "migrations are visible through reads" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    _no_wildcard: { signature: "Null" }
           |  }
           |})""".stripMargin
      )
      evalOk(auth, "Foo.create({ id: 0 })")

      evalOk(auth, "let doc: Any = Foo.byId(0)!; doc.foo") shouldBe Value.Null(
        Span.Null)

      evalOk(
        auth,
        """|Collection.byName("Foo")!.update({
           |  fields: {
           |    _no_wildcard: { signature: "Null" },
           |    foo: {
           |      signature: "Int",
           |      default: "5"
           |    },
           |  },
           |  migrations: [{ add: { field: ".foo" } }]
           |})""".stripMargin
      )

      evalOk(auth, "Foo.byId(0)!.foo") shouldBe Value.Int(5)
    }
  }

  "updating documents should work" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
        """.stripMargin
    )

    val id = evalOk(auth, "User.create({ name: 'Alice' })").as[DocID]

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  migrations {
           |    drop .name
           |    add .name
           |    backfill .name = "Bob"
           |  }
           |}
        """.stripMargin
    )

    val original = ctx ! ModelStore.get(auth.scopeID, id).map(_.get.schemaVersion)
    evalOk(auth, s"User.byId(${id.subID.toLong})!.update({ name: 'Carol' })")
    val updated = ctx ! ModelStore.get(auth.scopeID, id).map(_.get.schemaVersion)

    // The doc was written before migrations, but the synchronous migration should
    // have updated the doc and its schema version to the latest.
    original shouldBe (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get
    // The schema version used to write the document comes from the `MigrationList`
    // on the collection, but in this case it should be the same as the cached value.
    updated shouldBe (ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)).get

    evalOk(auth, "User.all().map(.name).toArray()") shouldBe Value.Array(
      Value.Str("Carol"))
  }

  "implicitly add parents of nested fields" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ id: 0, name: 'Alice' })")

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  address: {
           |    street: String
           |  }
           |
           |  migrations {
           |    add .address.street
           |    backfill .address.street = "10th"
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User(0)!.address.street") shouldBe Value.Str("10th")
  }
}
