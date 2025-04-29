package fauna.model.test

import fauna.ast.VersionL
import fauna.atoms.{ CollectionID, DocID, SchemaVersion, SubID }
import fauna.auth.{ AdminPermissions, Auth }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Collection, Database, Index, SchemaNames }
import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure, Result }
import fauna.model.schema.{
  CollectionConfig,
  NativeCollectionID,
  NativeIndex,
  SchemaStatus
}
import fauna.repo.query.Query
import fauna.repo.schema.ConstraintFailure
import fauna.repo.store.CacheStore
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.api.set.Scalar
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.index.NativeIndexID
import fauna.storage.ir.{ MapV, TrueV }
import fql.ast.{ Span, Src }
import fql.parser.Parser
import org.scalactic.source.Position
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration._

class FQL2CollectionSpec extends FQL2WithV4Spec {
  import ConstraintFailure._

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "FQL2CollectionSpec" - {
    "stores and updates schema versions" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    _no_wildcard: { signature: "Null" }
           |  },
           |  document_ttls: true
           |})""".stripMargin
      )
      val colID = (ctx ! Collection.idByNameActive(auth.scopeID, "Foo")).get

      val ttlS = "3000-01-01T12:00:00.000Z"
      evalOk(auth, s"Foo.create({ id: 0, ttl: Time('$ttlS') })")
      val doc0 =
        (ctx ! Store.getUnmigrated(auth.scopeID, DocID(SubID(0), colID))).get
      // There are no migrations, so the schema version is the minimum
      doc0.schemaVersion shouldEqual SchemaVersion.Min
      doc0.ttl shouldEqual Some(Timestamp.parse(ttlS))

      evalOk(
        auth,
        """|Foo.definition.update({
           |  fields: {
           |    _no_wildcard: { signature: "Null" },
           |    a: { signature: "Number | Null" }
           |  },
           |  migrations: [{ add: { field: ".a" } }]
           |})""".stripMargin
      )
      // Adding migrations should bump the schema version.
      val svMiddle =
        (ctx ! Collection.getUncached(
          auth.scopeID,
          colID)).get.active.get.config.internalMigrations.latestVersion
      svMiddle should be > SchemaVersion.Min

      evalOk(auth, "Foo.create({ id: 1, a: 0 })")
      val doc1 =
        (ctx ! Store.getUnmigrated(auth.scopeID, DocID(SubID(1), colID))).get
      // Now that there are migrations, the schema version should be bumped.
      doc1.schemaVersion shouldEqual svMiddle

      // And updates.
      evalOk(auth, "Foo.byId(0)!.update({ a: 1 })")
      val docUpdated =
        (ctx ! Store.getUnmigrated(auth.scopeID, DocID(SubID(0), colID))).get
      docUpdated.schemaVersion shouldEqual svMiddle

      // And legacy writes.
      val legacyID = evalV4Ok(
        auth,
        CreateF(Ref("collections/Foo"), MkObject("a" -> 2))
      ).asInstanceOf[VersionL].version.id
      val doc2 = (ctx ! Store.getUnmigrated(auth.scopeID, legacyID)).get
      doc2.schemaVersion shouldEqual svMiddle

      // Legacy collection updates should bump the cached schema version, but not
      // affect migrations.
      evalV4Ok(auth, Update(ClassRef("Foo"), MkObject("history_days" -> 10)))
      (ctx ! Collection.getUncached(
        auth.scopeID,
        colID)).get.active.get.config.internalMigrations.latestVersion shouldBe svMiddle
      (
        ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID)
      ).get should be > svMiddle
    }

    "rejects non-persistable values" in {
      // typechecking disallows this, so test runtime behavior
      evalErr(
        auth,
        "Collection.create({ name: Collection })",
        typecheck = false) shouldBe QueryRuntimeFailure(
        "invalid_type",
        "Value at `name` has type Collection, which cannot be persisted.",
        FQLInterpreter.StackTrace(Seq(Span(17, 39, Src.Query("")))))
    }

    "rejects writes to customer tenant root db" in {
      val tenantRoot = newCustomerTenantRoot(auth)

      val err = evalErr(
        tenantRoot,
        "Collection.create({ name: 'rootColl' })"
      )

      err.code shouldBe "constraint_failure"
      err.failureMessage shouldBe "Failed to create Collection."
      err.asInstanceOf[QueryRuntimeFailure].constraintFailures shouldBe Seq(
        TenantRootWriteFailure("Collection")
      )
    }

    "allows writes to internal tenant root db" in {
      val tenantRoot = newInternalTenantRoot(auth)

      evalOk(
        tenantRoot,
        "Collection.create({ name: 'rootColl' })"
      )
    }

    "allows staged deletes" in {
      evalOk(auth, "Collection.create({ name: 'Foo' })")

      ctx ! SchemaStatus.pinActiveSchema(auth.scopeID, SchemaVersion.Min)

      evalOk(auth, "Collection.byName('Foo')!.delete()")
    }

    "validate foreign references" - {
      "roles" - {
        def withRoleFK(isDelete: Boolean, query: => String) = {
          evalOk(
            auth,
            """|Collection.create({
               |  name: "aColl",
               |})
               |
               |Role.create({
               |  name: "aRole0",
               |  privileges: {
               |    resource: "aColl",
               |    actions: { read: true }
               |  }
               |})
               |
               |Role.create({
               |  name: "aRole1",
               |  membership: {
               |    resource: "aColl"
               |  },
               |  privileges: {
               |    resource: "Collection",
               |    actions: { read: true }
               |  }
               |})""".stripMargin
          )
          if (isDelete) {
            renderErr(auth, query) shouldBe (
              """|error: Invalid database schema update.
                 |    error: Resource `aColl` does not exist
                 |    at main.fsl:5:14
                 |      |
                 |    5 |   privileges aColl {
                 |      |              ^^^^^
                 |      |
                 |    error: Resource `aColl` does not exist
                 |    at main.fsl:14:14
                 |       |
                 |    14 |   membership aColl
                 |       |              ^^^^^
                 |       |
                 |at *query*:1:35
                 |  |
                 |1 | Collection.byName('aColl')!.delete()
                 |  |                                   ^^
                 |  |""".stripMargin
            )
          } else {
            evalOk(auth, query)
            evalOk(
              auth,
              """|
               |[Role.byName("aRole0")!.privileges.resource, Role.byName("aRole1")!.membership.resource]
                 |""".stripMargin
            ) match {
              case Value.Array(elems) =>
                elems shouldBe ArraySeq(
                  Value.Str("anotherName"),
                  Value.Str("anotherName"))
              case v => fail(s"unexpected type returned from fqlx query $v")
            }
          }
        }
        "rename" in withRoleFK(
          isDelete = false,
          "Collection.byName('aColl')!.update({ name: 'anotherName'})"
        )
        "delete" in withRoleFK(
          isDelete = true,
          "Collection.byName('aColl')!.delete()"
        )
      }
    }
    "fql4" - {

      def noMVT(data: Data): Data =
        data.remove(Collection.MinValidTimeFloorField)

      "updates preserve fqlx fields" in {
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "TestCol",
              |  alias: "TestColAlias",
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  },
              |  constraints: [ { unique: ["name"] } ]
              |})""".stripMargin
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(Ref("collections/TestCol"))
        ).asInstanceOf[VersionL].version

        evalV4Ok(
          auth,
          Update(Ref("collections/TestCol"), MkObject())
        )

        val legacyReadTwo = evalV4Ok(
          auth,
          Get(Ref("collections/TestCol"))
        ).asInstanceOf[VersionL].version

        // Updates change the MVT, so compare without it.
        noMVT(legacyReadTwo.data) shouldEqual noMVT(legacyRead.data)
      }
      "updates can't modify fqlx fields" in {
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "TestCol",
              |  alias: "TestColAlias",
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  },
              |  constraints: [ { unique: ["name"] } ]
              |})""".stripMargin
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(Ref("collections/TestCol"))
        ).asInstanceOf[VersionL].version

        evalV4Ok(
          auth,
          Update(
            Ref("collections/TestCol"),
            MkObject(
              "indexes" -> "hello",
              "backingIndexes" -> "foo",
              "constraints" -> "bar"
            ))
        )

        val legacyReadTwo = evalV4Ok(
          auth,
          Get(
            Ref("collections/TestCol")
          )
        ).asInstanceOf[VersionL].version

        noMVT(legacyReadTwo.data) shouldEqual noMVT(legacyRead.data)
      }
      "replace doesn't remove fqlx fields" in {
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "TestCol",
              |  alias: "TestColAlias",
              |  indexes: {
              |    byName: {
              |      terms: [{ field: "name" }]
              |    }
              |  },
              |  constraints: [ { unique: ["name"] }]
              |})""".stripMargin
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(
            Ref("collections/TestCol")
          )
        ).asInstanceOf[VersionL].version

        evalV4Ok(
          auth,
          Replace(
            Ref("collections/TestCol"),
            MkObject(
              "name" -> "hello"
            ))
        )

        val legacyReadTwo = evalV4Ok(
          auth,
          Get(Ref("collections/hello"))
        ).asInstanceOf[VersionL].version

        noMVT(legacyReadTwo.data).remove(SchemaNames.NameField) shouldEqual noMVT(
          legacyRead.data).remove(SchemaNames.NameField)
      }
    }
    // this is a legacy field that can be present on old collections
    "allows has_class_index" in {
      val doc = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "TestCol"
            |})""".stripMargin
      ) match {
        case Value.Doc(doc, _, _, _, _) => doc
        case v => fail(s"unexpected query return type, expected doc, got $v")
      }

      val res = (ctx ! Store.getUnmigrated(auth.scopeID, doc)).get
      val ins = ctx ! CollectionConfig(auth.scopeID, doc.collID).flatMap { cfg =>
        Store.insert(
          cfg.get.Schema,
          res.docID,
          res.data.patch(
            Diff(
              MapV(
                "has_class_index" -> true
              )
            )))
      }
      ins.data.fields.get(List("has_class_index")) shouldBe Some(TrueV)

      evalOk(
        auth,
        s"""|TestCol.definition.update({
            |  history_days: 10
            |})""".stripMargin
      )
    }
    "doesn't allow setting has_class_index" in {
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "TestCol"
            |})""".stripMargin
      )
      val res = evalErr(
        auth,
        s"""|TestCol.definition.update({
            |  has_class_index: true
            |})""".stripMargin)

      inside(res) { case QueryRuntimeFailure(code, _, _, _, Seq(cf), _) =>
        code shouldEqual "constraint_failure"
        cf.label shouldEqual "has_class_index"
      }
    }

    "updates advance MVT field" - {
      "in FQL X" in {
        val c =
          evalOk(auth, "Collection.create({ name: 'Foo' })").asInstanceOf[Value.Doc]
        val before = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        evalOk(auth, "Collection.byName('Foo')!.update({})")
        val after = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        evalV4Ok(auth, Update(Ref("collections/Foo")))
        val afterLegacy = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        val bMVT = before.data(Collection.MinValidTimeFloorField)
        val aMVT = after.data(Collection.MinValidTimeFloorField)
        bMVT shouldBe <(aMVT)
        val alMVT = afterLegacy.data(Collection.MinValidTimeFloorField)
        aMVT shouldBe <(alMVT)
      }

      "in FQL 4" in {
        val c =
          evalV4Ok(auth, CreateCollection(MkObject("name" -> "Foo")))
            .asInstanceOf[VersionL]
            .version
        val before = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        evalV4Ok(auth, Update(Ref("collections/Foo")))
        val after = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        val bMVT = before.data(Collection.MinValidTimeFloorField)
        val aMVT = after.data(Collection.MinValidTimeFloorField)
        bMVT shouldBe <(aMVT)
      }
    }

    "pin prevents updates from advancing MVT" - {
      "in FQL X" in {
        val c =
          evalOk(auth, "Collection.create({ name: 'Foo' })").asInstanceOf[Value.Doc]
        evalOk(auth, "Foo.create({})")

        def checkMVTIsStatic(pinnedMVT: Timestamp) = {
          // V10 update and check.
          evalOk(auth, "Collection.byName('Foo')!.update({})")
          val after = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
          after.data(Collection.MinValidTimeFloorField) shouldBe pinnedMVT

          // V4 update and check.
          evalV4Ok(auth, Update(Ref("collections/Foo")))
          val afterLegacy = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
          afterLegacy.data(Collection.MinValidTimeFloorField) shouldBe pinnedMVT
        }

        // Pin. History days is zero, so MVT pre-offset should pause at the earliest
        // pin TS.
        val pinnedTS = ctx ! (Query.snapshotTime flatMap { ts =>
          Database.addMVTPin(auth.scopeID, ts) map { _ => ts }
        })
        checkMVTIsStatic(pinnedTS)

        // Another pin.
        ctx ! (Query.snapshotTime flatMap { ts =>
          Database.addMVTPin(auth.scopeID, ts)
        })

        // Second pin is later, so the pin TS remains the same.
        checkMVTIsStatic(pinnedTS)

        // Remove a pin.
        ctx ! Database.removeMVTPin(auth.scopeID)
        checkMVTIsStatic(pinnedTS)

        // Remove another pin, so MVT is unpinned.
        ctx ! Database.removeMVTPin(auth.scopeID)

        // MVT should advance.
        evalOk(auth, "Collection.byName('Foo')!.update({})")
        val after = (ctx ! ModelStore.get(auth.scopeID, c.id)).get

        evalV4Ok(auth, Update(Ref("collections/Foo")))
        val afterLegacy = (ctx ! ModelStore.get(auth.scopeID, c.id)).get

        val aMVT = after.data(Collection.MinValidTimeFloorField)
        pinnedTS shouldBe <(aMVT)
        val alMVT = afterLegacy.data(Collection.MinValidTimeFloorField)
        aMVT shouldBe <(alMVT)

        // Pinning before the MVT offset should be an error.
        assertThrows[IllegalStateException] {
          ctx ! (Query.snapshotTime flatMap { ts =>
            Database.addMVTPin(auth.scopeID, ts - Collection.MVTOffset - 1.second)
          })
        }
      }

      "in FQL 4" in {
        val c =
          evalV4Ok(auth, CreateCollection(MkObject("name" -> "Foo")))
            .asInstanceOf[VersionL]
            .version

        // Pin.
        val pinnedTS = ctx ! (Query.snapshotTime flatMap { ts =>
          Database.addMVTPin(auth.scopeID, ts) map { _ => ts }
        })

        evalV4Ok(auth, Update(Ref("collections/Foo")))
        val afterPinned = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        val apMVT = afterPinned.data(Collection.MinValidTimeFloorField)
        apMVT shouldBe pinnedTS

        // Unpin.
        ctx ! Database.removeMVTPin(auth.scopeID)

        evalV4Ok(auth, Update(Ref("collections/Foo")))
        val afterUnpinned = (ctx ! ModelStore.get(auth.scopeID, c.id)).get
        val aupMVT = afterUnpinned.data(Collection.MinValidTimeFloorField)
        pinnedTS shouldBe <(aupMVT)
      }
    }

    "sets default history days" in {
      evalOk(auth, "Collection.create({ name: 'Foo' })")
      evalOk(auth, "Foo.definition.history_days") shouldEqual Value.Long(0)
    }

    "use ttl_days as a default document ttl" in {
      // No TTL
      val coll =
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Col"
             |})
             |""".stripMargin
        ).as[DocID]

      val docsIdxCfg =
        (ctx ! Index.getConfig(
          auth.scopeID,
          NativeIndexID.DocumentsByCollection
        )).get

      val terms = Vector(Scalar(coll))

      def assertTTL(res: Value, defaultTTL: Duration)(implicit pos: Position) = {
        val now = (res / "now").to[Value.Time].value
        val ttl = now + defaultTTL
        (res / "ttl") shouldBe Value.Time(ttl)

        val doc = (res / "doc").to[Value.Doc].id
        val idxValue =
          ctx ! Query.snapshotTime.flatMap {
            Store.collection(docsIdxCfg, terms, _) findValueT {
              _.docID == doc
            }
          }

        val tuple = idxValue.value.tuple
        tuple.ttl.value shouldBe ttl
      }

      // No TTL on create
      evalOk(
        auth,
        """|Col.create({ foo: 'bar' }).ttl
           |""".stripMargin
      ) should matchPattern { case _: Value.Null => }

      // Update coll with 1 day ttl
      evalOk(
        auth,
        """|Col.definition.update({
           |  ttl_days: 1
           |})
           |""".stripMargin
      )

      // TTL on update
      assertTTL(
        evalOk(
          auth,
          """|let doc = Col.all().first()!
             |doc.update({ foo: 'baz' })
             |{ now: Time.now(), doc: doc, ttl: doc.ttl! }
             |""".stripMargin
        ),
        1.day
      )

      // TTL on create
      assertTTL(
        evalOk(
          auth,
          """|let doc = Col.create({ fizz: 'buzz' })
             |{ now: Time.now(), doc: doc, ttl: doc.ttl! }
             |""".stripMargin
        ),
        1.day
      )

      // Preserve document's ttl
      assertTTL(
        evalOk(
          auth,
          """|let doc = Col.create({
             |  buz: 'fizz',
             |  ttl: Time.now().add(10, 'days')
             |})
             |{ now: Time.now(), doc: doc, ttl: doc.ttl! }
             |""".stripMargin
        ),
        10.day
      )

      // Preserve data.ttl field
      val res =
        evalOk(
          auth,
          """|let doc = Col.createData({
             |  buz: 'fizz',
             |  ttl: 'something else'
             |})
             |{ now: Time.now(), doc: doc, ttl: doc.ttl!, data_ttl: doc.data.ttl! }
             |""".stripMargin
        )

      assertTTL(res, 1.day)
      (res / "data_ttl") shouldBe Value.Str("something else")
    }

    "doesn't give hint for user collections that don't exist" in {
      evalErr(
        auth,
        """|Collection("Foo")
           |""".stripMargin
      ).errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: invalid argument `collection`: No such user collection `Foo`.
           |at *query*:1:11
           |  |
           |1 | Collection("Foo")
           |  |           ^^^^^^^
           |  |""".stripMargin
      )
    }

    "give pretty error for collections created in this transaction" in {
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo"
           |})
           |Collection("Foo")
           |""".stripMargin
      ).errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: invalid argument `collection`: No such user collection `Foo`.
           |at *query*:4:11
           |  |
           |4 | Collection("Foo")
           |  |           ^^^^^^^
           |  |
           |hint: A collection cannot be created and used in the same query.
           |  |
           |4 | Collection("Foo")
           |  |           ^^^^^^^
           |  |""".stripMargin
      )
    }

    "disallow collection writes and doc writes in the same query" in {
      evalOk(auth, "Collection.create({ name: 'User' })")

      evalErr(
        auth,
        """|User.definition.update({ data: { hi: 3 }})
           |User.create({ foo: "bar" })
           |""".stripMargin
      ).errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Cannot modify a document and its collection in the same query.
           |at *query*:2:12
           |  |
           |2 | User.create({ foo: "bar" })
           |  |            ^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "disallows combinations of writes in the same query" - {
      def setupDB = {
        val auth = newDB
        evalOk(auth, "Collection.create({ name: 'User' })")
        evalOk(auth, "User.create({ id: '1234' })")
        auth
      }

      val docWrites = Seq(
        "User.create({ foo: 'bar' })",
        "User.byId('1234')!.update({ foo: 'bar' })",
        "User.byId('1234')!.replace({ foo: 'bar' })",
        "User.byId('1234')!.delete()"
      )

      val collWrites = Seq(
        "Collection.byName('User')!.update({ data: { hi: 3 } })",
        "Collection.byName('User')!.replace({ name: 'User', data: { hi: 3 } })",
        "Collection.byName('User')!.delete()"
      )

      "each query works in its own" in {
        (docWrites ++ collWrites).foreach { q =>
          val auth = setupDB
          evalOk(auth, q)
        }
      }

      "every combination of queries fails when used together" in {
        for {
          doc  <- docWrites
          coll <- collWrites
          q <- Seq(
            s"$doc\n$coll",
            s"$coll\n$doc"
          )
        } {
          val auth = setupDB
          val err = evalErr(auth, q)
          err.code shouldBe "invalid_write"
          err.errors.head.message shouldBe "Cannot modify a document and its collection in the same query."
        }
      }
    }

    "handles rapid collection recreation (w/ typecheck)" in {
      ctx.service.schemaCacheInvalidationService foreach { _.stop(false) }
      try {
        evalOk(auth, "Collection.create({ name: 'a' })")
        evalOk(
          auth,
          """|Collection.byName('a')!.delete()
             |Collection.create({name: 'a'})
             |""".stripMargin
        )
        evalOk(auth, "Collection.byName('a')!.delete()")
        evalOk(auth, "Collection.create({ name: 'a' })")
      } finally {
        ctx.service.schemaCacheInvalidationService foreach { _.start() }
      }
    }

    "handles rapid collection recreation (no typecheck)" in {
      ctx.service.schemaCacheInvalidationService foreach { _.stop(false) }
      try {
        evalOk(auth, "Collection.create({ name: 'a' })", typecheck = false)
        evalOk(
          auth,
          """|Collection.byName('a').delete()
             |Collection.create({name: 'a'})
             |""".stripMargin,
          typecheck = false
        )
        evalOk(auth, "Collection.byName('a').delete()", typecheck = false)
        evalOk(auth, "Collection.create({ name: 'a' })", typecheck = false)
      } finally {
        ctx.service.schemaCacheInvalidationService foreach { _.start() }
      }
    }

    "disallows `data` in create, update, and replace (with wildcard)" in {
      evalOk(
        auth,
        """|Collection.create({
           |name: 'Foo'
           |})""".stripMargin
      )

      val c = evalErr(auth, "Foo.create({ data: { a: 0, b: 1 } })")
      c.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ a: 0, b: 1 }` is not a subtype of `Null`
           |at *query*:1:20
           |  |
           |1 | Foo.create({ data: { a: 0, b: 1 } })
           |  |                    ^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 0 })")
      val u = evalErr(auth, "Foo.byId(0)!.update({ id:1, data: { a: 0, b: 1 } })")
      u.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ a: 0, b: 1 }` is not a subtype of `Null`
           |at *query*:1:35
           |  |
           |1 | Foo.byId(0)!.update({ id:1, data: { a: 0, b: 1 } })
           |  |                                   ^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      val r = evalErr(auth, "Foo.byId(0)!.replace({ data: { a: 0, b: 1 } })")
      r.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ a: 0, b: 1 }` is not a subtype of `Null`
           |at *query*:1:30
           |  |
           |1 | Foo.byId(0)!.replace({ data: { a: 0, b: 1 } })
           |  |                              ^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }
  }

  "defaults in nested fields" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  a: {
           |    foo: Int = 3
           |    bar: String
           |  }
           |}""".stripMargin
    )

    renderErr(auth, "User.create({})") shouldBe (
      """|error: Type `{}` does not have field `a`
         |at *query*:1:13
         |  |
         |1 | User.create({})
         |  |             ^^
         |  |""".stripMargin
    )

    renderErr(auth, "User.create({})", typecheck = false) shouldBe (
      """|error: Failed to create document in collection `User`.
         |constraint failures:
         |  a.bar: Missing required field of type String
         |at *query*:1:12
         |  |
         |1 | User.create({})
         |  |            ^^^^
         |  |""".stripMargin
    )

    evalOk(auth, "User.create({ id: 0, a: { bar: 'hi' } }).a") shouldBe Value.Struct(
      "foo" -> Value.Int(3),
      "bar" -> Value.Str("hi"))
  }

  "update signature with defaults" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  a: {
           |    foo: Int = 3
           |    bar: String
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ id: 0, a: { bar: 'hi' } }).a") shouldBe Value.Struct(
      "foo" -> Value.Int(3),
      "bar" -> Value.Str("hi"))

    evalOk(auth, "User(0)!.update({ a: { foo: 4 } }).a") shouldBe Value.Struct(
      "foo" -> Value.Int(4),
      "bar" -> Value.Str("hi"))

    evalOk(auth, "User(0)!.update({}).a") shouldBe Value.Struct(
      "foo" -> Value.Int(4),
      "bar" -> Value.Str("hi"))
  }

  "defaults where all nested values are defined" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  a: {
           |    foo: Int = 3
           |    bar: String = "a"
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({}).a") shouldBe Value.Struct(
      "foo" -> Value.Int(3),
      "bar" -> Value.Str("a"))
  }

  "changes by collection picks up native collections" in {
    // Make two collections, then update one. The updated one should appear in the
    // changes by collection first.
    val foo = evalOk(auth, "Collection.create({ name: 'Foo' })").as[DocID]
    val bar = evalOk(auth, "Collection.create({ name: 'Bar' })").as[DocID]

    evalOk(auth, "Collection.byName('Foo')!.update({ data: { foo: 3 } })")

    val allDocs = {
      val idx = NativeIndex.DocumentsByCollection(auth.scopeID)
      val term = Vector(Scalar(NativeCollectionID.Collection.toDocID))
      val pq = Store.collection(idx, term, Timestamp.MaxMicros)
      ctx ! (pq mapValuesT { _.docID }).flattenT
    }

    val changedDocs = {
      val idx = NativeIndex.ChangesByCollection(auth.scopeID)
      val term = Vector(Scalar(NativeCollectionID.Collection.toDocID))
      val pq = Store.collection(idx, term, Timestamp.MaxMicros)
      ctx ! (pq mapValuesT { _.docID }).flattenT
    }

    // The last element of the set is the highest value. So `bar` has the highest ID,
    // and `foo` has the highest updated timestamp.
    allDocs shouldBe Seq(foo, bar)
    changedDocs shouldBe Seq(bar, foo)
  }

  "nested nullable fields combined with parent default" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  address: {
           |    street: String
           |    city: String?
           |  } = {
           |    street: "unknown"
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({}).address") shouldBe Value.Struct(
      "street" -> Value.Str("unknown"))
  }

  "cannot set a struct with defaults to null" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  address: {
           |    street: Number = 10
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({}).address") shouldBe Value.Struct(
      "street" -> Value.Int(10))
    evalOk(auth, "User.create({ address: {} }).address") shouldBe Value.Struct(
      "street" -> Value.Int(10))
    evalOk(auth, "User.create({ address: { street: 11 } }).address") shouldBe Value
      .Struct("street" -> Value.Int(11))

    // This used to be allowed, and it would remove the `address` field from the doc.
    renderErr(auth, "User.create({ address: null }).address") shouldBe (
      """|error: Failed to create document in collection `User`.
         |constraint failures:
         |  address: Missing required field of type { street: Number }
         |at *query*:1:12
         |  |
         |1 | User.create({ address: null }).address
         |  |            ^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "defaults will not be inserted on update with top-level defaults" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  street: Number = 10
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ id: 0, street: 11 }).street") shouldBe Value.Int(11)

    evalOk(auth, "User(0)!.update({}).street") shouldBe Value.Int(11)

    // This used to be allowed, and it would replace the `street` field with the
    // default value.
    renderErr(auth, "User(0)!.update({ street: null }).street") shouldBe (
      """|error: Failed to update document with id 0 in collection `User`.
         |constraint failures:
         |  street: Missing required field of type Number
         |at *query*:1:16
         |  |
         |1 | User(0)!.update({ street: null }).street
         |  |                ^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "defaults will not be inserted on update with nested defaults" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  address: {
           |    street: Number = 10
           |  }
           |}""".stripMargin
    )

    evalOk(
      auth,
      "User.create({ id: 0, address: { street: 11 } }).address") shouldBe Value
      .Struct("street" -> Value.Int(11))

    evalOk(auth, "User(0)!.update({}).address") shouldBe Value.Struct(
      "street" -> Value.Int(11))

    // This used to be allowed, and it would replace the `address` field with the
    // default value.
    renderErr(auth, "User(0)!.update({ address: null }).address") shouldBe (
      """|error: Failed to update document with id 0 in collection `User`.
         |constraint failures:
         |  address: Missing required field of type { street: Number }
         |at *query*:1:16
         |  |
         |1 | User(0)!.update({ address: null }).address
         |  |                ^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "defaults will be inserted on replace" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  address: {
           |    street: Number = 10
           |  }
           |}""".stripMargin
    )

    evalOk(
      auth,
      "User.create({ id: 0, address: { street: 11 } }).address") shouldBe Value
      .Struct("street" -> Value.Int(11))

    evalOk(auth, "User(0)!.replace({}).address") shouldBe Value.Struct(
      "street" -> Value.Int(10))

    // This used to be allowed, and it would remove the `address` field from the doc.
    renderErr(auth, "User(0)!.replace({ address: null }).address") shouldBe (
      """|error: Failed to update document with id 0 in collection `User`.
         |constraint failures:
         |  address: Missing required field of type { street: Number }
         |at *query*:1:17
         |  |
         |1 | User(0)!.replace({ address: null }).address
         |  |                 ^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "MVT works with staged schema" - {
    // This is the offset from the snapshot time to the MVT floor in the active doc.
    // It is not used by doc GC directly.
    def getActiveMvtFloorOffset() = ctx ! (for {
      doc <- Collection.getUncached(auth.scopeID, CollectionID(1024))
      mvt = doc.get.active.get.config.minValidTimeFloor
      snap <- Query.snapshotTime
    } yield snap.difference(mvt).toDays)

    // This is the offset from the snapshot time to the MVT floor in the staged doc.
    // It is not used by doc GC directly.
    def getStagedMvtFloorOffset() = ctx ! (for {
      doc <- Collection.getUncached(auth.scopeID, CollectionID(1024))
      mvt = doc.get.staged.get.config.minValidTimeFloor
      snap <- Query.snapshotTime
    } yield snap.difference(mvt).toDays)

    // This is the _actual_ MVT that doc GC will use.
    def getDerivedMvt() = ctx ! (for {
      mvt  <- Collection.deriveMinValidTime(auth.scopeID, CollectionID(1024))
      snap <- Query.snapshotTime
    } yield snap.difference(mvt).toDays)

    "floor is not advanced while in a staged schema" in {
      evalOk(auth, "Collection.create({ name: 'Foo', history_days: 3 })")

      getActiveMvtFloorOffset() shouldBe 3
      getDerivedMvt() shouldBe 3

      // First pin, then increase history days.
      ctx ! SchemaStatus.pin(new FQLInterpreter(auth))
      evalOk(auth, "Collection.byName('Foo')!.update({ history_days: 1 })")

      getActiveMvtFloorOffset() shouldBe 3
      getStagedMvtFloorOffset() shouldBe 1
      getDerivedMvt() shouldBe 3

      // Increasing the history days should increase the staged MVT floor. This
      // wouldn't be allowed in a normal push, but it is allowed the derived MVT
      // can't change in a staged push.
      evalOk(auth, "Collection.byName('Foo')!.update({ history_days: 2 })")

      getActiveMvtFloorOffset() shouldBe 3
      getStagedMvtFloorOffset() shouldBe 2
      getDerivedMvt() shouldBe 3

      // Committing a schema should bump the MVT floor.
      ctx ! SchemaStatus.commit(new FQLInterpreter(auth))
      ctx ! CacheStore.invalidateScope(auth.scopeID)

      // NB: Even though we decreased the history days to 1, and then increased it
      // back up to 2, that was in a staged push. So the end result shouldn't care
      // that the history days was ever 1.
      getActiveMvtFloorOffset() shouldBe 2
      getDerivedMvt() shouldBe 2
    }

    "floor is not advanced while staging a schema in the same txn" in {
      evalOk(auth, "Collection.create({ name: 'Foo', history_days: 3 })")

      getActiveMvtFloorOffset() shouldBe 3
      getDerivedMvt() shouldBe 3

      // Pin and decrease the history days in the same transaction.
      val intp = new FQLInterpreter(auth)
      val q = Parser
        .query("Collection.byName('Foo')!.update({ history_days: 1 })")
        .toOption
        .get

      val pinQ = SchemaStatus.pin(intp)
      val evalQ = intp
        .evalWithTypecheck(q, Map.empty, FQLInterpreter.TypeMode.InferType)
        .flatMap { _ =>
          intp.runPostEvalHooks().map {
            case Result.Ok(_)        => ()
            case err @ Result.Err(_) => fail(err.toString)
          }
        }

      ctx ! ((pinQ, evalQ) par { (_, _) => Query.unit })
      ctx ! CacheStore.invalidateScope(auth.scopeID)

      getActiveMvtFloorOffset() shouldBe 3
      getStagedMvtFloorOffset() shouldBe 1
      getDerivedMvt() shouldBe 3
    }
  }

  "refs to keys, tokens, and credentials work" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  key: Ref<Key>
           |  token: Ref<Token>
           |  cred: Ref<Credential>
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|User.create({
         |  key: Key.create({ role: "admin" }),
         |  token: Token.create({ document: User(0) }),
         |  cred: Credential.create({ document: User(0), password: "foo" }),
         |})""".stripMargin
    )
  }

  "refs to named collections do not work" in {
    // NB: The schema type validator does not catch this! Only the
    // `SchemaTypeResolver` up in model catches this.
    //
    // Also, not a great error, but works well enough.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection User {
           |  c: Ref<Collection>
           |}""".stripMargin
    ) shouldBe (
      """|error: Unknown type `Collection`
         |at main.fsl:2:10
         |  |
         |2 |   c: Ref<Collection>
         |  |          ^^^^^^^^^^
         |  |
         |error: Reference type must refer to a doc type
         |at main.fsl:2:6
         |  |
         |2 |   c: Ref<Collection>
         |  |      ^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }
}
