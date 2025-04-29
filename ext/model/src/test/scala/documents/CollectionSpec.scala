package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.schema._
import fauna.model.schema.index._
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.test.{ CassandraHelper, MapVOps }
import fauna.storage.doc.{ Data, Field, InvalidNegative, InvalidType }
import fauna.storage.ir._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

class CollectionSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val exec = TaskExecutor(ctx)

  "CollectionSpec" - {
    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    // NB. only legacy databases have the "has_class_index" field
    val clsID = ctx ! {
      for {
        config <- CollectionConfig(scope, CollectionID.collID)
        schema = config.get.Schema
        collID = UserCollectionID.MinValue.toDocID
        data = MapV("name" -> "someclass", "has_class_index" -> true).toData
        version <- Store.insert(schema, collID, data, isCreate = true)
      } yield version.id
    }

    "writes collection ttls in documents" in {
      val colName = "writettls"
      ctx ! mkCollection(auth, MkObject("name" -> colName, "ttl_days" -> 1))

      {
        // Set a short TTL explicitly. The short TTL should show up in the doc.
        val shortTTL = Clock.time + 1.minute
        val ttlJSON = MkObject("ttl" -> JSObject("@ts" -> shortTTL.toString))
        val withTTL = ctx ! mkDoc(auth, colName, ttlJSON)
        val ttl = (ctx ! getInstance(auth, withTTL.refObj, Clock.time))
          .data(Version.TTLField)
          .value
        (ttl.difference(Clock.time) < 12.hours) should be(true)

        // Insert a version into history with a short TTL, which should also remain.
        ctx ! runQuery(auth, InsertVers(withTTL.refObj, 1, "create", ttlJSON))
        val insertTTL =
          ctx ! (runQuery(auth, Clock.time, At(1, Get(withTTL.refObj))) map {
            case VersionL(v, _) => v.data(Version.TTLField).value
            case _              => fail("huh")
          })
        (insertTTL.difference(Clock.time) < 12.hours) should be(true)
      }

      {
        // Don't set a TTL. The long (for a test) collection TTL should show up in
        // the doc.
        // NB: The TTL field must be a timestamp so there's no need to
        // test the case when the TTL is another type.
        val withoutTTL = ctx ! mkDoc(auth, colName, MkObject())
        val ttl = (ctx ! getInstance(auth, withoutTTL.refObj, Clock.time))
          .data(Version.TTLField)
          .value
        (ttl.difference(Clock.time) > 12.hours) should be(true)

        // Insert a version lacking TTL into history. It should get the col. TTL.
        ctx ! runQuery(auth, InsertVers(withoutTTL.refObj, 1, "create", MkObject()))
        val insertTTL =
          ctx ! (runQuery(auth, Clock.time, At(1, Get(withoutTTL.refObj))) map {
            case VersionL(v, _) => v.data(Version.TTLField).value
            case _              => fail("huh")
          })
        (insertTTL.difference(Clock.time) > 12.hours) should be(true)
      }

    }

    "default history_days = 0" in {
      val col =
        ctx ! mkCollection(auth, MkObject("name" -> "default"))
      (ctx ! Collection.get(scope, col)).value.config.historyDuration shouldBe 0.days
    }

    "rejects invalid history days type" in {
      val res =
        ctx ! evalQuery(
          auth,
          CreateCollection(
            MkObject(
              "name" -> "bad",
              "history_days" -> "10"
            )))

      inside(res) { case Left(List(err)) =>
        err.validationFailures should matchPattern {
          case List(
                InvalidType(
                  List("history_days"),
                  LongV.Type,
                  StringV.Type,
                  _
                )) =>
        }
      }
    }

    "rejects negative history days" in {
      val negHistory = ctx ! evalQuery(
        auth,
        CreateCollection(MkObject("name" -> "bad", "history_days" -> -1)))
      negHistory match {
        case Left(List(err)) =>
          err.validationFailures match {
            case List(InvalidNegative(_)) => // OK.
            case _                        => fail()
          }
        case _ => fail()
      }
    }

    "read indexes configs" in {
      val scope = ctx ! newScope

      val collectionData = Data(
        SchemaNames.NameField -> SchemaNames.Name("Foo"),
        Field[Vector[Data]]("backingIndexes") -> Vector(
          Data(
            Field[IndexID]("indexID") -> IndexID(1),
            Field[Vector[Data]]("terms") -> Vector(
              Data(Field[String]("field") -> "name")),
            Field[String]("status") -> CollectionIndex.Status.Building.asStr
          ),
          Data(
            Field[IndexID]("indexID") -> IndexID(2),
            Field[Vector[Data]]("terms") -> Vector(
              Data(Field[String]("field") -> "name"),
              Data(Field[String]("field") -> "age")),
            Field[String]("status") -> CollectionIndex.Status.Complete.asStr
          ),
          Data(
            Field[IndexID]("indexID") -> IndexID(3),
            Field[Vector[Data]]("terms") -> Vector(
              Data(Field[String]("field") -> "name")),
            Field[Vector[Data]]("values") -> Vector(
              Data(
                Field[String]("field") -> "age",
                Field[String]("order") -> "asc")),
            Field[String]("status") -> CollectionIndex.Status.Failed.asStr
          )
        ),
        Field[Data]("indexes") -> Data(
          Field[Data]("byName") -> Data(
            Field[Vector[Data]]("terms") -> Vector(
              Data(Field[String]("field") -> "name")),
            Field[String]("status") -> CollectionIndex.Status.Building.asStr,
            Field[Boolean]("queryable") -> true
          ),
          Field[Data]("byNameAndAge") -> Data(
            Field[Vector[Data]]("terms") -> Vector(
              Data(Field[String]("field") -> "name"),
              Data(Field[String]("field") -> "age")),
            Field[String]("status") -> CollectionIndex.Status.Complete.asStr,
            Field[Boolean]("queryable") -> false
          ),
          Field[Data]("byNameSortedByAge") -> Data(
            Field[Vector[Data]]("terms") -> Vector(
              Data(Field[String]("field") -> "name")),
            Field[Vector[Data]]("values") -> Vector(
              Data(
                Field[String]("field") -> "age",
                Field[String]("order") -> "asc")),
            Field[String]("status") -> CollectionIndex.Status.Failed.asStr,
            Field[Boolean]("queryable") -> true
          )
        )
      )

      ctx ! SchemaCollection
        .Collection(scope)
        .insert(UserCollectionID.MinValue, collectionData)

      val coll = (ctx ! Collection.get(scope, UserCollectionID.MinValue)).value

      coll.config.collIndexes shouldBe List(
        UserDefinedIndex(
          IndexID(1),
          Vector(
            CollectionIndex.Term(CollectionIndex.Field.Fixed("name"), mvaOpt = None)
          ),
          Vector.empty,
          queryable = true,
          CollectionIndex.Status.Building,
          name = "byName"
        ),
        UserDefinedIndex(
          IndexID(2),
          Vector(
            CollectionIndex.Term(CollectionIndex.Field.Fixed("name"), mvaOpt = None),
            CollectionIndex.Term(CollectionIndex.Field.Fixed("age"), mvaOpt = None)
          ),
          Vector.empty,
          queryable = false,
          CollectionIndex.Status.Complete,
          name = "byNameAndAge"
        ),
        UserDefinedIndex(
          IndexID(3),
          Vector(
            CollectionIndex.Term(CollectionIndex.Field.Fixed("name"), mvaOpt = None)
          ),
          Vector(
            CollectionIndex.Value(
              CollectionIndex.Field.Fixed("age"),
              ascending = true,
              mvaOpt = None)
          ),
          queryable = true,
          status = CollectionIndex.Status.Failed,
          name = "byNameSortedByAge"
        )
      )
    }

    "hides the class index field on render" in {
      val get = runQuery(auth, Clock.time, Get(ClassRef("someclass"))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unknown result: $r")
      }

      val readable = ReadAdaptor.readableData(ctx ! get)
      readable.fields.contains(List("has_class_index")) should be(false)
    }

    "hides the class index field on contains" in {
      val contains = runQuery(
        auth,
        Clock.time,
        Contains(JSString("has_class_index"), Get(ClassRef("someclass"))))
      (ctx ! contains) should equal(FalseL)
    }

    "hides the class index field on contains if nested" in {
      val contains = runQuery(
        auth,
        Clock.time,
        Contains(
          JSArray("foo", "has_class_index"),
          MkObject("foo" -> Get(ClassRef("someclass")))))
      (ctx ! contains) should equal(FalseL)
    }

    "hides the class index field on select" in {
      val select = evalQuery(
        auth,
        Clock.time,
        Select(JSString("has_class_index"), Get(ClassRef("someclass"))))
      val res = (ctx ! select).left.value
      res should have size (1)
      res(0) shouldBe a[ValueNotFound]
    }

    "hides the class index field on select if nested" in {
      val select = evalQuery(
        auth,
        Clock.time,
        Select(
          JSArray("foo", "has_class_index"),
          MkObject("foo" -> Get(ClassRef("someclass")))))
      val res = (ctx ! select).left.value
      res should have size (1)
      res(0) shouldBe a[ValueNotFound]
    }

    "computes the minimum valid timestamp" in {
      val create = mkCollection(
        auth,
        MkObject(
          "name" -> "withhistorydays",
          "history_days" -> 10
        )) flatMap { id =>
        Query.snapshotTime map { (_, id) }
      }
      val RepoContext.Result(txnTS0, (snapTS0, collID)) = ctx !! create

      def minValidTimeFloor =
        (ctx ! Collection.get(scope, collID)).get.config.minValidTimeFloor

      def minValidTS(snapTS: Timestamp) = {
        ctx.withStaticSnapshotTime(snapTS) ! {
          Collection.deriveMinValidTime(scope, collID)
        }
      }

      // Min valid time moves forward with snapshot time.
      minValidTimeFloor shouldBe snapTS0 - 10.days
      minValidTS(txnTS0) shouldBe txnTS0 - 10.days - Collection.MVTOffset
      minValidTS(txnTS0 + 1.days) shouldBe txnTS0 - 9.days - Collection.MVTOffset
      minValidTS(txnTS0 + 10.days) shouldBe txnTS0 - Collection.MVTOffset
      minValidTS(txnTS0 + 11.days) shouldBe txnTS0 + 1.day - Collection.MVTOffset

      val zero = runQuery(
        auth,
        Update(
          ClassRef("withhistorydays"),
          MkObject("history_days" -> 0)
        )) flatMap { _ =>
        Query.snapshotTime
      }

      val RepoContext.Result(txnTS1, snapTS1) = ctx !! zero

      // Truncates history. Min valid time moves forward with snapshot time.
      minValidTimeFloor shouldBe snapTS1
      minValidTS(txnTS1) shouldBe txnTS1 - Collection.MVTOffset
      minValidTS(txnTS1 + 1.day) shouldBe txnTS1 + 1.day - Collection.MVTOffset

      val thirty = runQuery(
        auth,
        Update(
          ClassRef("withhistorydays"),
          MkObject("history_days" -> 30)
        )) flatMap { _ =>
        Query.snapshotTime
      }

      val RepoContext.Result(txnTS2, snapTS2) = ctx !! thirty

      // Extends history. Min valid time stays on hold for 30 days.
      minValidTimeFloor shouldBe snapTS2
      minValidTS(txnTS2) shouldBe snapTS2 - Collection.MVTOffset
      minValidTS(txnTS2 + 29.days) shouldBe snapTS2 - Collection.MVTOffset
      minValidTS(txnTS2 + 30.days) shouldBe txnTS2 - Collection.MVTOffset
      minValidTS(txnTS2 + 31.days) shouldBe txnTS2 + 1.day - Collection.MVTOffset

      // Ignores history after collection's deletion
      val deleted = runQuery(auth, DeleteF(ClassRef("withhistorydays")))
      val RepoContext.Result(txnTS3, _) = ctx !! deleted
      minValidTS(txnTS3) shouldBe Timestamp.Epoch
    }

    "computes the minimum valid time on a non-existing collections" in {
      // Versions pointing to non-existing collections are left alone.
      val now = Clock.time
      var collID = CollectionID.MaxValue

      ctx.withStaticSnapshotTime(now) ! {
        Collection.deriveMinValidTime(scope, collID)
      } shouldBe Timestamp.Epoch

      collID = ctx ! mkCollection(
        auth,
        MkObject(
          "name" -> "withhistorydaystoberemoved",
          "history_days" -> 10
        ))
    }

    "computes the minimum valid timestamp floor upon pending writes" in {
      val createQ =
        mkCollection(
          auth,
          MkObject(
            "name" -> "withhistorydayspending",
            "history_days" -> 0
          ))

      val RepoContext.Result(txnTS, minValidTS) =
        ctx !! createQ.flatMap {
          Collection.computeMinValidTimeFloor(scope, _)
        }

      minValidTS should be < txnTS
    }

    "minimum valid time is Epoch on infinite histoy" in {
      val coll = (ctx ! CollectionConfig(scope, DatabaseID.collID)).value
      val mvt = ctx ! Collection.deriveMinValidTime(scope, coll.id)
      coll.historyDuration shouldBe Duration.Inf
      mvt shouldBe Timestamp.Epoch
    }

    "computes the minimum valid time floor" in {
      // NB. Start from a collection with no `history_days` field as somepoint the
      // field was not required and there are legacy data without it throughout the
      // environments.
      val RepoContext.Result(_, collID) =
        ctx !! SchemaCollection
          .Collection(scope)
          .nextID()
          .flatMap {
            case None => sys.error("unreachable")
            case Some(id) =>
              val insertQ =
                ModelStore.insert(
                  scope,
                  id.toDocID,
                  MapV("name" -> "mvtfloor").toData,
                  isCreate = true
                )
              insertQ map { _ => id }
          }

      def computedMVTFloor = ctx ! Collection.computeMinValidTimeFloor(scope, collID)
      computedMVTFloor shouldBe Timestamp.Epoch

      val RepoContext.Result(txnTS0, _) =
        ctx !! runQuery(
          auth,
          Update(
            ClassRef("mvtfloor"),
            MkObject("history_days" -> 10)
          ))

      computedMVTFloor shouldBe txnTS0 - 10.days

      val RepoContext.Result(txnTS1, _) =
        ctx !! runQuery(
          auth,
          Update(
            ClassRef("mvtfloor"),
            MkObject("history_days" -> 0)
          ))

      computedMVTFloor shouldBe txnTS1

      val RepoContext.Result(txnTS2, _) =
        ctx !! runQuery(
          auth,
          Update(
            ClassRef("mvtfloor"),
            MkObject("history_days" -> 30)
          ))

      computedMVTFloor shouldBe txnTS2
    }

    "deleting a class removes it from the cache" in {
      val fooClsID = ctx ! mkCollection(auth, MkObject("name" -> "fooclass"))
      val fooCls = ctx ! (Collection.get(scope, fooClsID) map { _.get })
      val q = evalQuery(auth, Clock.time, Get(ClassRef("fooclass")))

      (ctx ! q) match {
        case Right(VersionL(v, _)) => v.id should equal(fooCls.id.toDocID)
        case r                     => sys.error(s"Unexpected: $r")
      }

      (ctx ! runQuery(auth, Clock.time, DeleteF(ClassRef("fooclass"))))
      exec.step()

      (ctx ! q) match {
        case Right(r)        => sys.error(s"Unexpected: $r")
        case Left(List(err)) => err shouldBe a[UnresolvedRefError]
        case Left(errors)    => fail(s"Unexpected errors: $errors.")
      }
    }

    "deleting a collection also deletes all indexes for which it was the only source" in {
      val colName = "cascade"
      ctx ! mkCollection(auth, MkObject("name" -> colName))

      val idxName0 = "cascadeIndex0"
      val idxName1 = "cascadeIndex1"

      def mkIndex(name: String) = CreateIndex(
        MkObject(
          "name" -> name,
          "source" -> ClassRef(colName),
          "terms" -> JSArray(MkObject("field" -> List("data", "name")))))

      // make two indexes
      ctx ! runQuery(auth, mkIndex(idxName0))
      ctx ! runQuery(auth, mkIndex(idxName1))

      // delete the collection
      ctx ! runQuery(auth, Clock.time, DeleteF(ClassRef(colName)))
      exec.step()

      // check that the collection actually deleted
      (ctx ! evalQuery(
        auth,
        Clock.time,
        Get(ClassRef(colName)))).isLeft shouldBe true

      // wipe the cache
      ctx.cacheContext.schema.invalidate()

      // query for the indexes and expect them to be gone
      (ctx ! evalQuery(auth, Get(IndexRef(idxName0)))).isLeft shouldBe true
      (ctx ! evalQuery(auth, Get(IndexRef(idxName1)))).isLeft shouldBe true
    }

    "updating a class reflects in the cache" in {
      val oldClsID = ctx ! mkCollection(auth, MkObject("name" -> "oldclass"))
      val oldCls = ctx ! (Collection.get(scope, oldClsID) map { _.get })
      val q = evalQuery(auth, Clock.time, Get(ClassRef("oldclass")))

      (ctx ! q) match {
        case Right(VersionL(v, _)) => v.id should equal(oldCls.id.toDocID)
        case r                     => sys.error(s"Unexpected: $r")
      }

      (ctx ! runQuery(
        auth,
        Clock.time,
        Update(ClassRef("oldclass"), MkObject("name" -> "newclass"))))
      exec.step()

      (ctx ! q) match {
        case Right(r)        => sys.error(s"Unexpected: $r")
        case Left(List(err)) => err shouldBe a[UnresolvedRefError]
        case Left(errors)    => fail(s"Unexpected errors: $errors.")
      }
    }

    "can create multiple classes simultaneously" in {
      ctx ! runQuery(
        auth,
        JSArray(
          CreateClass(MkObject("name" -> "simulone")),
          CreateClass(MkObject("name" -> "simultwo"))))

      val c1 = ctx ! runQuery(auth, Get(ClassRef("simulone")))
      val c2 = ctx ! runQuery(auth, Get(ClassRef("simultwo")))
      (c1, c2) match {
        case (VersionL(c1, _), VersionL(c2, _)) =>
          c1.docID should not equal (c2.docID)
        case _ => fail()
      }
    }

    "concurrent creation" in {
      val futs = 1 to 10 map { i =>
        ctx
          .withRetryOnContention(maxAttempts = 10)
          .runNow(runQuery(auth, CreateClass(MkObject("name" -> s"concurrent$i"))))
      }

      Await.result(futs.join, 30.seconds)

      val ids = 1 to 10 map { i =>
        (ctx ! runQuery(auth, Clock.time, Get(ClassRef(s"concurrent$i")))) match {
          case VersionL(vers, _) => vers.docID.subID.toLong
          case _                 => fail()
        }
      }

      ids.distinct.size should equal(10)
    }

    "collection already exists" in {
      (ctx ! evalQuery(auth, CreateClass(MkObject("name" -> "someclass")))) match {
        case Left(errors) =>
          errors shouldBe List(
            InstanceAlreadyExists(clsID, RootPosition at "create_class"))
        case Right(_) => fail()
      }
    }

    "protected mode guards collections" in {
      (ctx ! evalQuery(
        Auth.adminForScope(auth.scopeID),
        CreateDatabase(MkObject("name" -> "child", "protected" -> true)))).map {
        case VersionL(v, _) =>
          val childAuth = Auth.forScope(v.data(Database.ScopeField))

          // Not allowed to delete the collection in protected mode.
          ctx ! evalQuery(childAuth, CreateCollection(MkObject("name" -> "a")))
          val res1 = ctx ! evalQuery(childAuth, DeleteF(ClassRef("a")))
          inside(res1) { case Left(List(err)) =>
            err.code shouldBe "schema validation failed"
            err
              .asInstanceOf[SchemaValidationError]
              .msg shouldBe "Cannot delete collection: destructive change forbidden because database is in protected mode."
          }

          // Not allowed to lower history days in protected mode.
          ctx ! evalQuery(
            childAuth,
            CreateCollection(MkObject("name" -> "b", "history_days" -> 3)))
          val res2 = ctx ! evalQuery(
            childAuth,
            Update(ClassRef("b"), MkObject("history_days" -> 1)))
          inside(res2) { case Left(List(err)) =>
            err.code shouldBe "schema validation failed"
            err
              .asInstanceOf[SchemaValidationError]
              .msg shouldBe "Cannot decrease `history_days` field: destructive change forbidden because database is in protected mode."
          }

          // Not allowed to delete an index protected mode.
          ctx ! evalQuery(
            childAuth,
            CreateIndex(
              MkObject(
                "name" -> "aIdx",
                "source" -> ClassRef("a"),
                "terms" -> JSArray(MkObject("field" -> List("data", "x"))))))
          val res3 = ctx ! evalQuery(childAuth, DeleteF(IndexRef("aIdx")))
          inside(res3) { case Left(List(err)) =>
            err.code shouldBe "schema validation failed"
            err
              .asInstanceOf[SchemaValidationError]
              .msg shouldBe "Cannot cause deletion of backing index: destructive change forbidden because database is in protected mode."
          }

        case _ => fail()
      }
    }

    "disallows writes when schema is staged" in {
      try {
        (ctx ! evalQuery(
          auth,
          CreateCollection(MkObject("name" -> "foo")))) should matchPattern {
          case Right(_) =>
        }

        ctx ! SchemaStatus.pin(new FQLInterpreter(auth))

        inside(ctx ! evalQuery(auth, DeleteF(ClassRef("foo")))) {
          case Left(List(err)) =>
            err.code shouldBe "schema validation failed"
            err.asInstanceOf[SchemaValidationError].msg shouldBe
              "Cannot update schema from FQL v4 if schema has been staged."
        }

      } finally {
        ctx ! SchemaStatus.clearActiveSchema(auth.scopeID)
      }
    }

    // NB: Don't put tests after this that create collections because it fakes
    // that the ID space is full.
    "limit max collection id" in {
      val id = DocID(SubID(CollectionID.MaxValue.toLong - 1), CollectionID.collID)

      val config = ctx ! CollectionConfig(scope, UserCollectionID.MinValue)
      val schema = config.get.Schema

      ctx ! Store.insert(schema, id, Data.empty)

      ctx ! evalQuery(auth, CreateCollection(MkObject("name" -> "coll0")))

      the[MaximumIDException.type] thrownBy {
        ctx ! evalQuery(auth, CreateCollection(MkObject("name" -> "coll1")))
      } should have message "Maximum sequential ID reached."
    }
  }
}
