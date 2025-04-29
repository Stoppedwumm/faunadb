package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.flags._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.schema.{ CollectionConfig, NativeIndex, SchemaCollection }
import fauna.model.tasks._
import fauna.prop.Generators
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.test.{ CassandraHelper, MapVOps }
import fauna.stats._
import fauna.storage.{ Value => _, _ }
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.index.IndexSources
import fauna.storage.ir._
import org.scalatest.tags.Slow
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

@Slow
class IndexSpec extends Spec with Generators {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model").withRetryOnContention(2)
  val exec = TaskExecutor(ctx)

  def tasks =
    Task.getRunnableByHost(CassandraService.instance.localID.get).flattenT

  val collectionID = CollectionID.collID.toDocID
  val indexID = IndexID.collID.toDocID

  def checkPartitions(res: Literal, parts: Long) = {
    val vers = res.asInstanceOf[VersionL].version
    vers.data(Index.PartitionsField) should equal(Some(parts))
  }

  after {
    CassandraHelper.ffService.reset()
  }

  "IndexConfigurationValidatorSpec" - {
    def patchSource(data: Data) = {
      val validator =
        Index.SourceField.validator[Query] +
          Index.SourceConfigValidator +
          Index.SourceBindingsValidator
      ctx ! validator.patch(Data.empty, Data.empty diffTo data) match {
        case Left(v)  => v
        case Right(_) => Nil
      }
    }

    def patchTerms(data: Data) =
      ctx ! Index.IndexByField
        .validator[Query]
        .patch(Data.empty, Data.empty diffTo data) match {
        case Left(v)  => v
        case Right(_) => Nil
      }

    def patchValues(data: Data) =
      ctx ! Index.CoveredField
        .validator[Query]
        .patch(Data.empty, Data.empty diffTo data) match {
        case Left(v)  => v
        case Right(_) => Nil
      }

    "source may not be empty" in {
      val errs = patchSource(Data.empty)
      errs should have size (1)
      errs.head.code should be("value required")
      errs.head.path should be(List("source"))
    }

    "source may be a ref" in {
      val data = MapV("source" -> collectionID).toData
      patchSource(data).isEmpty should be(true)
    }

    "source ref must be a class" in {
      val data = MapV("source" -> IndexID(1).toDocID).toData
      val err = patchSource(data)
      err.head.code should be("invalid type")
    }

    "source may be a wildcard" in {
      val data = MapV("source" -> "_").toData
      patchSource(data).isEmpty should be(true)
    }

    "source may be a list of refs" in {
      val data = MapV("source" -> ArrayV(collectionID, indexID)).toData
      patchSource(data).isEmpty should be(true)
    }

    "source may be a config" in {
      val data = MapV("source" -> MapV("class" -> collectionID)).toData
      patchSource(data).isEmpty should be(true)
    }

    "source may be a config with a wildcard" in {
      val data = MapV("source" -> MapV("class" -> "_")).toData
      patchSource(data).isEmpty should be(true)
    }

    "source may be a list of configs" in {
      val data = MapV(
        "source" -> ArrayV(
          MapV("class" -> collectionID),
          MapV("class" -> indexID))).toData
      patchSource(data).isEmpty should be(true)
    }

    "source must be valid object" in {
      val data =
        MapV("source" -> MapV("object" -> MapV("class" -> "post_prod_class"))).toData
      val errs = patchSource(data)
      errs should have size (1)
      errs.head.code should be("value required")
      errs.head.path should be(List("class"))
    }

    "source may not include a wildcard in any list" in {
      val data1 = MapV("source" -> ArrayV(StringV("_"), collectionID)).toData
      val err1 = patchSource(data1)
      err1 should have size (1)
      err1.head.code should be("mixed wildcards")

      val data2 = MapV(
        "source" -> ArrayV(MapV("class" -> "_"), MapV("class" -> indexID))).toData
      val err2 = patchSource(data2)
      err2 should have size (1)
      err2.head.code should be("mixed wildcards")
    }

    "source may mix refs and wildcards if bindings are used" in {
      val bindings = MapV(
        "field1" -> QueryV(
          "instance",
          MapV(
            "select" -> ArrayV("data", "nickname"),
            "from" -> MapV("var" -> "instance"))))

      val data = MapV(
        "source" -> ArrayV(
          MapV("class" -> "_", "fields" -> bindings),
          MapV("class" -> indexID)),
        "terms" -> ArrayV(MapV("binding" -> StringV("field1")))).toData
      patchSource(data).isEmpty should be(true)
    }

    "source cannot mix refs and wildcards in a single object" in {
      val data = MapV(
        "source" -> MapV(
          "class" -> ArrayV(StringV("_"), DocIDV(CollectionID(1025).toDocID)),
          "fields" -> MapV(
            "field1" -> QueryV(
              "instance",
              MapV(
                "select" -> ArrayV("data", "nickname"),
                "from" -> MapV("var" -> "instance"))))
        ),
        "terms" -> ArrayV(MapV("binding" -> StringV("field1")))
      ).toData

      val errs = patchSource(data)
      errs should have size (1)
      errs.head.code should be("invalid type")
      errs.head.path should be(List("class"))
    }

    "source can bind queries to field names" in {
      val data = MapV(
        "source" -> MapV(
          "class" -> DocIDV(CollectionID(1025).toDocID),
          "fields" -> MapV(
            "field1" -> QueryV(
              "instance",
              MapV(
                "select" -> ArrayV("data", "nickname"),
                "from" -> MapV("var" -> "instance"))))
        ),
        "terms" -> ArrayV(MapV("binding" -> StringV("field1")))
      ).toData
      patchSource(data).isEmpty should be(true)
    }

    "source can bind multiple field names" in {
      val data = MapV(
        "source" -> MapV(
          "class" -> DocIDV(CollectionID(1025).toDocID),
          "fields" -> MapV(
            "field1" -> QueryV(
              "instance",
              MapV(
                "select" -> ArrayV("data", "nickname"),
                "from" -> MapV("var" -> "instance"))),
            "field2" -> QueryV(
              "instance",
              MapV(
                "select" -> ArrayV("data", "nickname"),
                "from" -> MapV("var" -> "instance")))
          )
        ),
        "terms" -> ArrayV(
          MapV("binding" -> StringV("field1")),
          MapV("binding" -> StringV("field2"))
        )
      ).toData
      patchSource(data).isEmpty should be(true)
    }

    "source may be multiple classes with bindings" in {
      val data = MapV(
        "source" -> ArrayV(
          MapV(
            "class" -> DocIDV(CollectionID(1025).toDocID),
            "fields" -> MapV(
              "field1" -> QueryV(
                "instance",
                MapV(
                  "select" -> ArrayV("data", "nickname"),
                  "from" -> MapV("var" -> "instance"))))
          ),
          MapV(
            "class" -> DocIDV(CollectionID(1026).toDocID),
            "fields" -> MapV(
              "field2" -> QueryV(
                "instance",
                MapV(
                  "select" -> ArrayV("data", "nickname"),
                  "from" -> MapV("var" -> "instance"))))
          )
        ),
        "terms" -> ArrayV(
          MapV("binding" -> StringV("field1")),
          MapV("binding" -> StringV("field2"))
        )
      ).toData
      patchSource(data).isEmpty should be(true)
    }

    "source field bindings cannot read" in {
      val data = MapV(
        "source" -> MapV(
          "class" -> DocIDV(CollectionID(1025).toDocID),
          "fields" -> MapV(
            "field1" -> QueryV("ref", MapV("get" -> MapV("var" -> "ref"))))),
        "terms" -> ArrayV(MapV("binding" -> StringV("field1")))
      ).toData
      val errs = patchSource(data)
      errs should have size (1)
      errs.head.code should be("invalid binding")
    }

    "source field bindings cannot write" in {
      val data = MapV(
        "source" -> MapV(
          "class" -> DocIDV(CollectionID(1025).toDocID),
          "fields" -> MapV(
            "field1" -> QueryV("ref", MapV("update" -> MapV("var" -> "ref"))))),
        "terms" -> ArrayV(MapV("binding" -> StringV("field1")))
      ).toData
      val errs = patchSource(data)
      errs should have size (1)
      errs.head.code should be("invalid binding")
    }

    "source field bindings must be used in either terms or values" in {
      val source = MapV(
        "class" -> DocIDV(CollectionID(1025).toDocID),
        "fields" -> MapV(
          "field1" -> QueryV(
            "instance",
            MapV(
              "select" -> ArrayV("data", "nickname"),
              "from" -> MapV("var" -> "instance"))))
      )

      val errs = patchSource(MapV("source" -> source).toData)
      errs should have size (1)
      errs.head.code should be("unused binding")

      val errsT = patchSource(
        MapV("source" -> source, "terms" -> ArrayV(MapV("field" -> "foo"))).toData)
      errsT should have size (1)
      errsT.head.code should be("unused binding")

      val errsV = patchSource(
        MapV("source" -> source, "values" -> ArrayV(MapV("field" -> "foo"))).toData)
      errsV should have size (1)
      errsV.head.code should be("unused binding")

      val field = ArrayV(MapV("binding" -> "field1"))
      patchSource(
        MapV("source" -> source, "terms" -> field).toData).isEmpty should be(true)
      patchSource(
        MapV("source" -> source, "values" -> field).toData).isEmpty should be(true)
    }

    "term validator ignores unused fields" in {
      val data = MapV(
        "terms" -> ArrayV(MapV("field" -> ArrayV("data", "name"))),
        "bogus" -> 1,
        "source" -> collectionID,
        "unique" -> false).toData
      patchTerms(data).isEmpty should be(true)
    }

    "value validator ignores unused fields" in {
      val data = MapV(
        "values" -> ArrayV(MapV("field" -> ArrayV("data", "name"))),
        "bogus" -> 1,
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data).isEmpty should be(true)
    }

    "terms may be a term object" in {
      val data = MapV(
        "terms" -> MapV("field" -> ArrayV("data", "name")),
        "source" -> collectionID,
        "unique" -> false).toData
      patchTerms(data).isEmpty should be(true)
    }

    "values may be a term object" in {
      val data = MapV(
        "values" -> MapV("field" -> ArrayV("data", "name")),
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data).isEmpty should be(true)
    }

    "terms may be an array of term objects" in {
      val data = MapV(
        "terms" -> ArrayV(MapV("field" -> ArrayV("data", "name"))),
        "source" -> collectionID,
        "unique" -> false).toData
      patchTerms(data).isEmpty should be(true)
    }

    "values may be an array of term objects" in {
      val data = MapV(
        "values" -> ArrayV(MapV("field" -> ArrayV("data", "name"))),
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data).isEmpty should be(true)
    }

    "terms may be empty" in {
      val data1 =
        MapV("terms" -> NullV, "source" -> collectionID, "unique" -> false).toData
      patchTerms(data1).isEmpty should be(true)

      val data2 =
        MapV(
          "terms" -> ArrayV(NullV),
          "source" -> collectionID,
          "unique" -> false).toData
      patchTerms(data2).isEmpty should be(true)

      val data3 = MapV("source" -> collectionID, "unique" -> false).toData
      patchTerms(data3).isEmpty should be(true)
    }

    "values may be empty" in {
      val data1 =
        MapV("values" -> NullV, "source" -> collectionID, "unique" -> false).toData
      patchValues(data1).isEmpty should be(true)

      val data2 = MapV(
        "values" -> ArrayV(NullV),
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data2).isEmpty should be(true)

      val data3 = MapV("source" -> collectionID, "unique" -> false).toData
      patchValues(data3).isEmpty should be(true)
    }

    "term paths are not checked for validity" in {
      // Index built with invalid paths will never match
      val data1 = MapV(
        "terms" -> ArrayV(MapV("field" -> ArrayV("bogus", "data"))),
        "source" -> collectionID,
        "unique" -> false).toData
      patchTerms(data1).isEmpty should be(true)

      val data2 =
        MapV(
          "terms" -> ArrayV(MapV("field" -> ArrayV("data", "person", "name"))),
          "source" -> collectionID,
          "unique" -> false).toData
      patchTerms(data2).isEmpty should be(true)

      val data3 = MapV(
        "terms" -> ArrayV(MapV("field" -> "email")),
        "source" -> collectionID,
        "unique" -> false).toData
      patchTerms(data3).isEmpty should be(true)
    }

    "value paths are not checked for validity" in {
      val data1 = MapV(
        "values" -> ArrayV(MapV("field" -> ArrayV("bogus", "data"))),
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data1).isEmpty should be(true)

      val data2 =
        MapV(
          "values" -> ArrayV(MapV("field" -> ArrayV("data", "person", "name"))),
          "source" -> collectionID,
          "unique" -> false).toData
      patchValues(data2).isEmpty should be(true)

      val data3 = MapV(
        "values" -> ArrayV(MapV("field" -> "email")),
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data3).isEmpty should be(true)
    }

    "term should contain a `field` of one or more strings" in {
      val data1 =
        MapV("terms" -> ArrayV(MapV("field" -> ArrayV("data", "name")))).toData
      patchTerms(data1).isEmpty should be(true)

      val data2 = MapV("terms" -> ArrayV(MapV("field" -> "ref"))).toData
      patchTerms(data2).isEmpty should be(true)

      val data3 = MapV(
        "terms" -> ArrayV(MapV("field" -> MapV("object" -> "data.name")))).toData
      val err = patchTerms(data3)
      err should have size (1)
      err.head.code should be("invalid type")
    }

    "values should contain a `field` of one or more strings" in {
      val data1 =
        MapV("values" -> ArrayV(MapV("field" -> ArrayV("data", "name")))).toData
      patchValues(data1).isEmpty should be(true)

      val data2 = MapV("values" -> ArrayV(MapV("field" -> "ref"))).toData
      patchValues(data2).isEmpty should be(true)

      val data3 = MapV(
        "values" -> ArrayV(MapV("field" -> MapV("object" -> "data.name")))).toData
      val err = patchValues(data3)
      err should have size (1)
      err.head.code should be("invalid type")
    }

    "term reverse must be valid" in {
      val data1 =
        MapV(
          "terms" -> ArrayV(MapV("field" -> "ref", "reverse" -> "true")),
          "source" -> collectionID).toData
      patchTerms(data1).isEmpty should be(false)

      val data2 = MapV(
        "terms" -> ArrayV(MapV("field" -> "ref", "reverse" -> 1)),
        "source" -> collectionID).toData
      patchTerms(data2).isEmpty should be(false)

      val data3 = MapV(
        "terms" -> ArrayV(MapV("field" -> "ref", "reverse" -> true)),
        "source" -> collectionID).toData
      patchTerms(data3).isEmpty should be(true)
    }

    "value reverse must be valid (though it is meaningless)" in {
      val data1 =
        MapV(
          "values" -> ArrayV(MapV("field" -> "ref", "reverse" -> "true")),
          "source" -> collectionID).toData
      patchValues(data1).isEmpty should be(false)

      val data2 = MapV(
        "values" -> ArrayV(MapV("field" -> "ref", "reverse" -> 1)),
        "source" -> collectionID).toData
      patchValues(data2).isEmpty should be(false)

      val data3 = MapV(
        "values" -> ArrayV(MapV("field" -> "ref", "reverse" -> true)),
        "source" -> collectionID).toData
      patchValues(data3).isEmpty should be(true)
    }

    "term transforms must be valid transforms" in {
      val data1 =
        MapV(
          "terms" -> ArrayV(MapV("field" -> "ref", "transform" -> "wrong")),
          "source" -> collectionID).toData
      patchTerms(data1).isEmpty should be(false)

      val data2 = MapV(
        "terms" -> ArrayV(MapV("field" -> "ref", "transform" -> 1)),
        "source" -> collectionID).toData
      patchTerms(data2).isEmpty should be(false)

      val data3 =
        MapV(
          "terms" -> ArrayV(MapV("field" -> "ref", "transform" -> "casefold")),
          "source" -> collectionID).toData
      patchTerms(data3).isEmpty should be(true)
    }

    "value transforms must be valid transforms" in {
      val data1 =
        MapV(
          "values" -> ArrayV(MapV("field" -> "ref", "transform" -> "wrong")),
          "source" -> collectionID).toData
      patchValues(data1).isEmpty should be(false)

      val data2 = MapV(
        "values" -> ArrayV(MapV("field" -> "ref", "transform" -> 1)),
        "source" -> collectionID).toData
      patchValues(data2).isEmpty should be(false)

      val data3 =
        MapV(
          "values" -> ArrayV(MapV("field" -> "ref", "transform" -> "casefold")),
          "source" -> collectionID).toData
      patchValues(data3).isEmpty should be(true)
    }

    "terms may include virtual fields" in {
      val data = MapV(
        "terms" ->
          ArrayV(
            MapV("field" -> "database"),
            MapV("field" -> "class"),
            MapV("field" -> "ref"),
            MapV("field" -> "ts")),
        "source" -> collectionID,
        "unique" -> false).toData
      patchTerms(data).isEmpty should be(true)
    }

    "values may include virtual fields" in {
      val data = MapV(
        "values" ->
          ArrayV(
            MapV("field" -> "database"),
            MapV("field" -> "class"),
            MapV("field" -> "ref"),
            MapV("field" -> "ts")),
        "source" -> collectionID,
        "unique" -> false).toData
      patchValues(data).isEmpty should be(true)
    }
  }

  "IndexSpec for native classes" - {
    val scopeID = ctx ! newScope(RootAuth)
    val auth = Auth.forScope(scopeID)

    ctx ! mkCollection(auth, MkObject("name" -> "animals"))

    "can index native class" in {
      noException should be thrownBy {
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "keys_by_name",
              "source" -> KeysRef,
              "terms" -> JSArray(MkObject("field" -> List("data", "name"))))))
      }
    }

    "wildcard indexes do not index native collections" in {
      ctx ! allSourcesIndex(auth, "all_the_things", active = true)

      ctx ! mkCollection(auth, MkObject("name" -> "invisible"))
      ctx ! mkIndex(auth, "invisible", "invisible", Nil)
      val doc = ctx ! mkDoc(auth, "invisible")

      val all = ctx ! collection(auth, Match("all_the_things"))

      all.elems should contain only (RefL(scopeID, doc.id))
    }

    "can index credentials" in {
      noException should be thrownBy {
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "creds_by_instance",
              "source" -> CredentialsRef,
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> List("instance")))))
        )
      }

      val animal =
        ctx ! runQuery(
          auth,
          CreateF(
            ClassRef("animals"),
            MkObject("credentials" -> MkObject("password" -> "sekrit"))))

      val ref = MkRef(
        ClassRef("animals"),
        animal.asInstanceOf[VersionL].version.id.subID.toLong)

      val res =
        (ctx ! runQuery(auth, Paginate(Match(IndexRef("creds_by_instance"), ref))))
          .asInstanceOf[PageL]

      res.elems should have size (1)
    }

    "Ensures getting the first key works" in {
      val scopeID = ctx ! newScope(RootAuth)
      val dbName = (ctx ! Database.forScope(scopeID).mapT(_.name)).get
      (0 to 5) foreach { _ =>
        ctx ! mkKey(dbName)
      }

      val head = ctx ! runQuery(RootAuth, Get(Select(0, Paginate((Ref("keys"))))))
      (ctx ! runQuery(RootAuth, Get(Ref("keys")))) should equal(head)
    }
  }

  "IndexSpec" - {
    val scopeID = ctx ! newScope
    val auth = Auth.forScope(scopeID)

    ctx ! mkCollection(auth, MkObject("name" -> "animals"))

    "can index tokens" in {
      noException should be thrownBy {
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "tokens_by_instance",
              "source" -> TokensRef,
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> List("instance"))))))
      }

      val animal =
        ctx ! runQuery(
          auth,
          CreateF(
            ClassRef("animals"),
            MkObject("credentials" -> MkObject("password" -> "sekrit"))))

      val ref = MkRef(
        ClassRef("animals"),
        animal.asInstanceOf[VersionL].version.id.subID.toLong)

      ctx ! runQuery(auth, Login(ref, MkObject("password" -> "sekrit")))
      ctx ! runQuery(auth, Login(ref, MkObject("password" -> "sekrit")))

      val res =
        (ctx ! runQuery(auth, Paginate(Match(IndexRef("tokens_by_instance"), ref))))
          .asInstanceOf[PageL]

      res.elems should have size (2)
    }

    "can control multi-value attribute behavior" in {
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "animals_mva1",
            "source" -> ClassRef("animals"),
            "values" -> JSArray(MkObject("field" -> JSArray("data", "foo"))))))

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "animals_no_mva1",
            "source" -> ClassRef("animals"),
            "values" -> JSArray(
              MkObject(
                "field" -> JSArray("data", "foo"),
                "mva" -> false,
                "reverse" -> true))))
      )

      ctx ! runQuery(
        auth,
        CreateF(
          ClassRef("animals"),
          MkObject("data" -> MkObject("foo" -> JSArray(1, 2, 3)))))

      val res1 =
        (ctx ! runQuery(auth, Paginate(Match("animals_mva1")))).asInstanceOf[PageL]
      res1.elems shouldEqual List(LongL(1), LongL(2), LongL(3))

      val res2 =
        (ctx ! runQuery(auth, Paginate(Match("animals_no_mva1"))))
          .asInstanceOf[PageL]
      res2.elems shouldEqual List(ArrayL(LongL(1), LongL(2), LongL(3)))
    }

    "index creation is transactional" in {
      CassandraHelper.DisableSchemaCacheInvalidationService = true
      try {
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "transactional_index",
              "source" -> ClassRef("animals"),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> List("data", "name")))))
        )
      } finally {
        CassandraHelper.DisableSchemaCacheInvalidationService = false
      }

      ctx ! runQuery(
        auth,
        CreateF(
          ClassRef("animals"),
          MkObject("data" -> MkObject("name" -> "sheep"))))

      val count =
        ctx ! runQuery(auth, Count(Match(IndexRef("transactional_index"), "sheep")))
      count should equal(LongL(1))
    }

    "index creation is transactional with multiple indexes" in {
      CassandraHelper.DisableSchemaCacheInvalidationService = true
      try {
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "transactional_index_one",
              "source" -> ClassRef("animals"),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> List("data", "name")))))
        )

        ctx ! runQuery(
          auth,
          CreateF(
            ClassRef("animals"),
            MkObject("data" -> MkObject("name" -> "turtle"))))

        val count1 = ctx ! runQuery(
          auth,
          Count(Match(IndexRef("transactional_index_one"), "turtle")))
        count1 should equal(LongL(1))

        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "transactional_index_two",
              "source" -> ClassRef("animals"),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> List("data", "name")))))
        )
      } finally {
        CassandraHelper.DisableSchemaCacheInvalidationService = false
      }

      ctx ! runQuery(
        auth,
        CreateF(ClassRef("animals"), MkObject("data" -> MkObject("name" -> "dove"))))

      val count2 = ctx ! runQuery(
        auth,
        Count(Match(IndexRef("transactional_index_two"), "dove")))
      count2 should equal(LongL(1))
    }

    "disallows index rebuild" in {
      val data =
        MkObject(
          "name" -> "animals_by_name",
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      val res1 = ctx ! runQuery(auth, CreateIndex(data))

      val res2 = ctx ! runQuery(auth, Update(IndexRef("animals_by_name"), data))

      res1.asInstanceOf[VersionL].version.id should equal(
        res2.asInstanceOf[VersionL].version.id)

      a[RuntimeException] should be thrownBy {
        ctx ! runQuery(
          auth,
          Update(
            IndexRef("animals_by_name"),
            MkObject(
              "terms" -> JSArray(MkObject("field" -> List("data", "full_name"))))))
      }

      a[RuntimeException] should be thrownBy {
        ctx ! runQuery(
          auth,
          Update(IndexRef("animals_by_name"), MkObject("partitions" -> 3)))
      }
    }

    "does not assign a new id on rename" in {
      val data =
        MkObject(
          "name" -> "animals_by_species",
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "species"))))

      val res1 = ctx ! runQuery(auth, CreateIndex(data))

      val res2 = ctx ! runQuery(
        auth,
        Update(
          IndexRef("animals_by_species"),
          MkObject("name" -> "animals_by_kind")))

      res1.asInstanceOf[VersionL].version.id should equal(
        res2.asInstanceOf[VersionL].version.id)
    }

    "new empty indexes are active" in {
      val data =
        MkObject(
          "name" -> "animals_by_other_name",
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      val res = ctx ! runQuery(auth, CreateIndex(data))

      val vers = res.asInstanceOf[VersionL].version
      vers.data(Index.ActiveField) should equal(true)
    }

    "active field may be written" in {
      val data =
        MkObject(
          "name" -> "active_index",
          "active" -> true,
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      val res = ctx ! runQuery(auth, CreateIndex(data))

      val vers = res.asInstanceOf[VersionL].version
      vers.data(Index.ActiveField) should equal(true)

      val res2 = ctx ! runQuery(
        auth,
        Update(IndexRef("active_index"), MkObject("active" -> false)))

      // active field was latched
      val vers2 = res2.asInstanceOf[VersionL].version
      vers2.data(Index.ActiveField) should equal(true)
    }

    "active indexes may build synchronously" in {
      ctx ! mkCollection(auth, MkObject("name" -> "smallthings"))

      val data =
        MkObject(
          "name" -> "buildmenow",
          "active" -> true,
          "source" -> ClassRef("smallthings"))

      for (_ <- 0 until Index.BuildSyncSize) {
        ctx ! mkDoc(auth, "smallthings")
      }

      val res = ctx ! runQuery(auth, CreateIndex(data))
      val version = res.asInstanceOf[VersionL].version
      val id = version.docID.as[IndexID]
      version.data(Index.ActiveField) should be(true)

      val ts = ctx ! Task.getAllRunnable().flattenT

      val indexes = ts collect {
        case t if t.name == IndexBuild.RootTask.name =>
          (t.data(IndexBuild.ScopeField), t.data(IndexBuild.IndexField))
      }

      // No async build task.
      indexes shouldNot contain((scopeID, id))

      val pg = ctx ! collection(auth, Match(IndexRef("buildmenow")))
      pg.elems.size should equal(Index.BuildSyncSize)
    }

    "active indexes create a build task" in {
      val data =
        MkObject(
          "name" -> "buildme",
          "active" -> true,
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      // Avoid the synchronous build path.
      for (_ <- 0 until Index.BuildSyncSize + 1) {
        ctx ! mkDoc(auth, "animals")
      }

      val res = ctx ! runQuery(auth, CreateIndex(data))
      val id = res.asInstanceOf[VersionL].version.docID.as[IndexID]

      val ts = ctx ! Task.getAllRunnable().flattenT

      val indexes = ts collect {
        case t if t.name == IndexBuild.RootTask.name =>
          (t.data(IndexBuild.ScopeField), t.data(IndexBuild.IndexField))
      }

      indexes should contain((scopeID, id))
    }

    "wildcard indexes with no collections do not create a build task" in {
      val scopeID = ctx ! newScope
      val auth = Auth.forScope(scopeID)

      val data =
        MkObject(
          "name" -> "nobuild",
          "source" -> "_",
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      val res = ctx ! runQuery(auth, CreateIndex(data))
      val id = res.asInstanceOf[VersionL].version.docID.as[IndexID]

      val ts = ctx ! Task.getAllRunnable().flattenT

      val indexes = ts collect {
        case t if t.name == IndexBuild.RootTask.name =>
          (t.data(IndexBuild.ScopeField), t.data(IndexBuild.IndexField))
      }

      indexes shouldNot contain((scopeID, id))
    }

    "limits build tasks" in {
      val limit = 10

      CassandraHelper.ffService.setAccount(MaxConcurrentBuilds, limit)

      val user_scope = ctx ! (runQuery(
        RootAuth,
        CreateDatabase(
          MkObject("name" -> "user_db", "account" -> MkObject("id" -> 42)))) map {
        case VersionL(v, _) => v.data(Database.ScopeField)
        case r              => sys.error(s"Unexpected: $r")
      })

      val user = Auth.forScope(user_scope)

      ctx ! mkCollection(user, MkObject("name" -> "animals"))

      // Avoid the synchronous build path.
      for (_ <- 0 until Index.BuildSyncSize + 1) {
        ctx ! mkDoc(user, "animals")
      }

      (0 until limit) foreach { i =>
        ctx ! mkIndex(user, s"index_$i", "animals", Nil)
      }

      an[IndexBuildLimitExceeded] should be thrownBy {
        ctx ! mkIndex(user, s"index_$limit", "animals", Nil)
      }

      // step all tasks - builds move from Runnable -> Blocked
      exec.step()

      an[IndexBuildLimitExceeded] should be thrownBy {
        ctx ! mkIndex(user, s"index_$limit", "animals", Nil)
      }

      val target = (ctx ! tasks) find { t =>
        val parent = t.parent flatMap { id =>
          ctx ! Task.get(id)
        }

        parent exists {
          _.state.isForked
        }
      }

      target.isEmpty should be(false)

      target foreach { t =>
        // pause one build's root task; it still counts against the limit
        (ctx ! t.pause("test")).isPaused should be(true)
      }

      an[IndexBuildLimitExceeded] should be thrownBy {
        ctx ! mkIndex(user, s"index_$limit", "animals", Nil)
      }

      target foreach { t =>
        ctx ! (Task.get(t.id) mapT { task => task.unpause() })
      }

      while ((ctx ! tasks).nonEmpty) exec.step()

      noException should be thrownBy {
        ctx ! mkIndex(user, s"index_${limit + 1}", "animals", Nil)
      }
    }

    "hidden indexes are hidden from indexes()" in {
      val animals = ctx ! Collection.idByNameActive(scopeID, "animals")
      val src = IndexSources(animals.get)
      val data = Index.DefaultData.update(
        SchemaNames.NameField -> SchemaNames.Name("invisible"),
        Index.SourceField -> Vector(SourceConfig(src)),
        Index.HiddenField -> Some(true))

      ctx ! SchemaCollection.Index(scopeID).flatMap { coll =>
        coll.Schema.nextID.flatMap { id =>
          SchemaCollection
            .Index(scopeID)
            .insert(id.get.as[IndexID], data, isCreate = true)
        }
      }

      val idx = ctx ! runQuery(auth, Get(IndexRef("invisible"))).map {
        _.asInstanceOf[VersionL]
      }
      idx.version.data(Index.HiddenField) should be(Some(true))

      val page = ctx ! runQuery(auth, Paginate(IndexesRef))
      page.asInstanceOf[PageL].elems foreach {
        case ref: RefL => ref.id shouldNot equal(idx.version.id)
        case v         => fail(s"unexpected value $v")
      }
    }

    "partitions field is not read only" in {
      val data =
        MkObject(
          "name" -> "partitioned",
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      val res = ctx ! runQuery(auth, CreateIndex(data))

      checkPartitions(res, 1L)

      val res2 = ctx ! runQuery(
        auth,
        CreateIndex(Merge(data, MkObject("name" -> "p2", "partitions" -> 2L))))

      checkPartitions(res2, 2L)
    }

    "partitions cannot be updated" in {
      val data =
        MkObject(
          "name" -> "partitioned2",
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "name"))))

      val res = ctx ! runQuery(auth, CreateIndex(data))

      checkPartitions(res, 1L)

      val res2 = ctx ! evalQuery(
        auth,
        Update(IndexRef("partitioned2"), MkObject("partitions" -> 2L)))

      res2 should matchPattern {
        case Left(
              List(ValidationError(List(IndexShapeChange.Partition(1, 2)), _))) =>
      }
    }

    "definition cannot be updated" in {
      val data =
        MkObject(
          "name" -> "bySpecies",
          "source" -> ClassRef("animals"),
          "terms" -> JSArray(MkObject("field" -> List("data", "species"))),
          "values" -> JSArray(MkObject("field" -> List("data", "name")))
        )

      ctx ! runQuery(auth, CreateIndex(data))

      val res = ctx ! evalQuery(
        auth,
        Update(
          IndexRef("bySpecies"),
          MkObject(
            "terms" -> JSArray(MkObject("field" -> List("data", "speecees"))))))

      res should matchPattern {
        case Left(
              List(ValidationError(
                List(IndexShapeChange.Fields(Diff(
                  MapV(List("terms" -> ArrayV(Vector(MapV(List("field" -> ArrayV(
                    Vector(StringV("data"), StringV("speecees")))))))))))),
                _
              ))) =>
      }
    }

    "partitions must be between 1 and 8" in {
      val data = MkObject(
        "name" -> "parted",
        "source" -> ClassRef("animals"),
        "terms" -> JSArray(MkObject("field" -> "class")))

      an[Exception] should be thrownBy {
        ctx ! runQuery(auth, CreateIndex(Merge(data, MkObject("partitions" -> -1))))
      }

      an[Exception] should be thrownBy {
        ctx ! runQuery(auth, CreateIndex(Merge(data, MkObject("partitions" -> 0))))
      }

      an[Exception] should be thrownBy {
        ctx ! runQuery(auth, CreateIndex(Merge(data, MkObject("partitions" -> 9))))
      }

      noException should be thrownBy {
        1 to 8 foreach { i =>
          val idx = ctx ! runQuery(
            auth,
            CreateIndex(
              Merge(data, MkObject("name" -> i.toString, "partitions" -> i))))

          checkPartitions(idx, i)
        }
      }
    }

    "partitions class indexes" in {
      val nullterms = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "source" -> ClassRef("animals"),
            "terms" -> JSNull,
            "name" -> "nulls")))

      val emptyterms = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "source" -> ClassRef("animals"),
            "terms" -> JSArray(),
            "name" -> "empty")))

      val noterms = ctx ! runQuery(
        auth,
        CreateIndex(MkObject("source" -> ClassRef("animals"), "name" -> "missing")))

      val classterm = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "source" -> ClassRef("animals"),
            "terms" -> JSArray(MkObject("field" -> "class")),
            "name" -> "classonly")))

      checkPartitions(nullterms, 8L)
      checkPartitions(emptyterms, 8L)
      checkPartitions(noterms, 8L)
      checkPartitions(classterm, 8L)
    }

    "allows class index partitions outside the default" in {
      val nullterms = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "partitions" -> 1,
            "source" -> ClassRef("animals"),
            "terms" -> JSNull,
            "name" -> "nulls2")))

      val emptyterms = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "partitions" -> 1,
            "source" -> ClassRef("animals"),
            "terms" -> JSArray(),
            "name" -> "empty2")))

      val noterms = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "partitions" -> 1,
            "source" -> ClassRef("animals"),
            "name" -> "missing2")))

      val classterm = ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "partitions" -> 1,
            "source" -> ClassRef("animals"),
            "terms" -> JSArray(MkObject("field" -> "class")),
            "name" -> "classonly2")))

      checkPartitions(nullterms, 1L)
      checkPartitions(emptyterms, 1L)
      checkPartitions(noterms, 1L)
      checkPartitions(classterm, 1L)

      // Updating doesn't try to change the partition count
      val nullterms2 = ctx ! runQuery(
        auth,
        Update(Ref("indexes/nulls2"), MkObject("active" -> true)))
      val emptyterms2 = ctx ! runQuery(
        auth,
        Update(Ref("indexes/empty2"), MkObject("active" -> true)))
      val noterms2 = ctx ! runQuery(
        auth,
        Update(Ref("indexes/missing2"), MkObject("active" -> true)))
      val classterm2 = ctx ! runQuery(
        auth,
        Update(Ref("indexes/classonly2"), MkObject("active" -> true)))

      checkPartitions(nullterms2, 1L)
      checkPartitions(emptyterms2, 1L)
      checkPartitions(noterms2, 1L)
      checkPartitions(classterm2, 1L)
    }

    "creates indexes with no terms" in {
      noException should be thrownBy {
        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "source" -> ClassRef("animals"),
              "active" -> true,
              "terms" -> JSNull,
              "name" -> "nullterms")))

        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "source" -> ClassRef("animals"),
              "active" -> true,
              "terms" -> JSArray(),
              "name" -> "emptyterms")))

        ctx ! runQuery(
          auth,
          CreateIndex(
            MkObject(
              "source" -> ClassRef("animals"),
              "active" -> true,
              "name" -> "noterms")))

        ctx ! runQuery(auth, Match(IndexRef("nullterms"), JSNull))
        ctx ! runQuery(auth, Match(IndexRef("emptyterms"), JSNull))
        ctx ! runQuery(auth, Match(IndexRef("noterms"), JSNull))
      }
    }

    "can not toggle unique for partitioned indexes" in {
      def validateFailedIndexUpdate(q: JSValue) =
        ctx ! evalQuery(auth, q) match {
          case Left(value) =>
            value.size shouldBe 1
            value.head shouldBe a[ValidationError]
            value.head
              .asInstanceOf[ValidationError]
              .validationFailures
              .head
              .code shouldBe "invalid index"
          case Right(_) => fail("unexpected success updating index")
        }

      def validateSuccessfulIndexUpdate(q: JSValue) =
        ctx ! evalQuery(auth, q) match {
          case Left(value) => fail(s"unexpected failure updating index, $value")
          case Right(_)    =>
        }

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "animals_unique",
            "source" -> ClassRef("animals"),
            "unique" -> true,
            "values" -> JSArray(MkObject("field" -> JSArray("data", "foo")))))
      )

      validateFailedIndexUpdate(
        Update(IndexRef("animals_unique"), MkObject("unique" -> false))
      )

      validateSuccessfulIndexUpdate(
        Update(IndexRef("animals_unique"), MkObject("unique" -> true))
      )

      validateFailedIndexUpdate(
        Update(IndexRef("animals_unique"), MkObject("unique" -> JSNull))
      )

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "animals_default_unique",
            "source" -> ClassRef("animals"),
            "values" -> JSArray(MkObject("field" -> JSArray("data", "foo")))))
      )

      validateFailedIndexUpdate(
        Update(IndexRef("animals_default_unique"), MkObject("unique" -> true))
      )

      validateSuccessfulIndexUpdate(
        Update(IndexRef("animals_default_unique"), MkObject("unique" -> false))
      )

      validateSuccessfulIndexUpdate(
        Update(IndexRef("animals_default_unique"), MkObject("unique" -> JSNull))
      )

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "animals_non_unique",
            "source" -> ClassRef("animals"),
            "unique" -> false,
            "values" -> JSArray(MkObject("field" -> JSArray("data", "foo")))))
      )

      validateFailedIndexUpdate(
        Update(IndexRef("animals_non_unique"), MkObject("unique" -> true))
      )
      validateSuccessfulIndexUpdate(
        Update(IndexRef("animals_non_unique"), MkObject("unique" -> JSNull))
      )
      validateSuccessfulIndexUpdate(
        Update(IndexRef("animals_non_unique"), MkObject("unique" -> false))
      )
    }

    "updating uniqueness does not rebuild" in {
      ctx ! mkCollection(auth, MkObject("name" -> "semiunique"))

      (1 to Index.BuildSyncSize + 1) foreach { i =>
        ctx ! runQuery(
          auth,
          CreateF(
            ClassRef("semiunique"),
            MkObject(
              "data" -> MkObject("foo" -> s"bar$i")
            )
          ))
      }

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "source" -> ClassRef("semiunique"),
            "name" -> "semiunique",
            "terms" -> JSArray(MkObject("field" -> JSArray("data", "foo"))))))

      val ts0 = ctx ! tasks
      ts0.nonEmpty shouldBe true
      while ((ctx ! tasks).nonEmpty) {
        exec.step()
      }

      ctx ! runQuery(
        auth,
        Update(IndexRef("semiunique"), MkObject("unique" -> true)))

      val ts1 = ctx ! tasks
      ts1.size should equal(0)

      ctx ! runQuery(
        auth,
        Update(IndexRef("semiunique"), MkObject("unique" -> false)))

      val ts2 = ctx ! tasks
      ts2.size should equal(0)
    }

    "dead reckoning" in {
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "source" -> ClassRef("animals"),
            "active" -> true,
            "terms" -> JSArray(MkObject("field" -> "class")),
            "name" -> "reckoning")))

      // invalidate by source schema
      ctx.cacheContext.schema.invalidate()

      ctx ! runQuery(
        auth,
        CreateF(
          ClassRef("animals"),
          MkObject("data" -> MkObject("name" -> "sheep"))))

      val q = Match(IndexRef("reckoning"), ClassRef("animals"))

      val pg1 = (ctx ! runQuery(auth, Paginate(q, Before(0)))).asInstanceOf[PageL]
      pg1.elems.isEmpty should be(true)
      pg1.before should equal(None)
      pg1.after should equal(Some(CursorL(Right(ArrayL(List(LongL(0)))))))

      val pg2 = (ctx ! runQuery(auth, Paginate(q, After(0)))).asInstanceOf[PageL]
      pg2.elems.isEmpty should be(false)
      pg2.before should equal(Some(CursorL(Right(ArrayL(List(LongL(0)))))))
      pg2.after should equal(None)

      val pg3 =
        (ctx ! runQuery(auth, Paginate(q, Before(JSNull)))).asInstanceOf[PageL]
      pg3.elems.isEmpty should be(false)
      pg3.before should equal(None)
      pg3.after should equal(Some(CursorL(Right(ArrayL(List(NullL))))))

      val pg4 =
        (ctx ! runQuery(auth, Paginate(q, After(JSNull)))).asInstanceOf[PageL]
      pg4.elems.isEmpty should be(true)
      pg4.before should equal(Some(CursorL(Right(ArrayL(List(NullL))))))
      pg4.after should equal(None)
    }

    "can index nested term paths" in {
      ctx ! mkCollection(auth, MkObject("name" -> "deep_sheep"))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "deep_sheep_finder",
            "source" -> ClassRef("deep_sheep"),
            "active" -> true,
            "terms" -> JSArray(
              MkObject("field" -> List("data", "nest", "this", "path", "deep")))
          ))
      )

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "bound_deep_sheep_finder",
            "source" -> MkObject(
              "class" -> ClassRef("deep_sheep"),
              "fields" -> MkObject("go_deep" -> QueryF(Lambda("inst" -> Select(
                JSArray("data", "nest", "this", "path", "deep"),
                Var("inst")))))
            ),
            "active" -> true,
            "terms" -> JSArray(MkObject("binding" -> "go_deep"))
          ))
      )

      val sheep = ctx ! mkDoc(
        auth,
        "deep_sheep",
        params = MkObject(
          "data" -> MkObject("nest" -> MkObject(
            "this" -> MkObject("path" -> MkObject("deep" -> "hi!"))))))

      val pg1 = ctx ! collection(auth, Match(IndexRef("deep_sheep_finder"), "hi!"))
      pg1.elems should equal(Seq(RefL(scopeID, sheep.id)))

      val pg2 =
        ctx ! collection(auth, Match(IndexRef("bound_deep_sheep_finder"), "hi!"))
      pg2.elems should equal(Seq(RefL(scopeID, sheep.id)))
    }

    "casefolds terms" in {
      ctx ! mkCollection(auth, MkObject("name" -> "paper_animals"))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "origami",
            "source" -> ClassRef("paper_animals"),
            "active" -> true,
            "terms" -> JSArray(
              MkObject("field" -> List("data", "name"), "transform" -> "casefold"))
          ))
      )

      val sheep = ctx ! mkDoc(
        auth,
        "paper_animals",
        params = MkObject("data" -> MkObject("name" -> "sHeEp")))

      val pg = ctx ! collection(auth, Match(IndexRef("origami"), "sheep"))
      pg.elems should equal(Seq(RefL(scopeID, sheep.id)))
    }

    "ngrams terms" in {
      ctx ! mkCollection(auth, MkObject("name" -> "named_animals"))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "origami_search",
            "source" -> MkObject(
              "class" -> ClassRef("named_animals"),
              "fields" -> MkObject("ngram" -> QueryF(Lambda(
                "inst" -> NGram(Select(JSArray("data", "name"), Var("inst"))))))),
            "active" -> true,
            "terms" -> JSArray(MkObject("binding" -> "ngram"))
          ))
      )

      val sheep = ctx ! mkDoc(
        auth,
        "named_animals",
        params = MkObject("data" -> MkObject("name" -> "sheep")))

      val sh = ctx ! collection(auth, Match(IndexRef("origami_search"), "sh"))
      sh.elems should equal(Seq(RefL(scopeID, sheep.id)))

      val ee = ctx ! collection(auth, Match(IndexRef("origami_search"), "ee"))
      ee.elems should equal(Seq(RefL(scopeID, sheep.id)))

      val ep = ctx ! collection(auth, Match(IndexRef("origami_search"), "ep"))
      ep.elems should equal(Seq(RefL(scopeID, sheep.id)))
    }

    "binds values to fields" in {
      ctx ! mkCollection(auth, MkObject("name" -> "querified_class"))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "querified_index",
            "source" -> MkObject(
              "class" -> ClassRef("querified_class"),
              "fields" -> MkObject(
                "field1" -> QueryF(
                  Lambda("inst" -> Select(JSArray("data", "my-field"), Var("inst"))))
              )
            ),
            "active" -> true,
            "terms" -> JSArray(MkObject("binding" -> "field1"))
          ))
      )

      val inst = ctx ! mkDoc(
        auth,
        "querified_class",
        params = MkObject("data" -> MkObject("my-field" -> "my-value")))

      val pg = ctx ! collection(auth, Match(IndexRef("querified_index"), "my-value"))
      pg.elems should equal(Seq(RefL(scopeID, inst.id)))
    }

    "deleting an index removes it from the cache" in {
      ctx ! mkCollection(auth, MkObject("name" -> "fooclass"))
      val fooIndex = ctx ! mkIndex(auth, "fooindex", "fooclass", Nil)
      val q = evalQuery(auth, Clock.time, Get(IndexRef("fooindex")))

      (ctx ! q) match {
        case Right(VersionL(v, _)) => v.id should equal(fooIndex.id.toDocID)
        case r                     => sys.error(s"Unexpected: $r")
      }

      (ctx ! runQuery(auth, Clock.time, DeleteF(IndexRef("fooindex"))))
      exec.step()

      (ctx ! q) match {
        case Right(r)        => sys.error(s"Unexpected: $r")
        case Left(List(err)) => err shouldBe a[UnresolvedRefError]
        case Left(errors)    => fail(s"Unexpected errors: $errors.")
      }
    }

    "new index creation reloads source cache" in {
      ctx ! mkCollection(auth, MkObject("name" -> "class1"))
      ctx ! mkIndex(auth, "index1", "class1", Nil)
      val inst1 = ctx ! mkDoc(auth, "class1")

      ctx ! mkIndex(auth, "index2", "class1", Nil, active = false)
      // wait for build to complete
      while ((ctx ! tasks).nonEmpty) exec.step()

      val inst2 = ctx ! mkDoc(auth, "class1")
      val res1 = (ctx ! collection(auth, Match(IndexRef("index1")))).elems
      val res2 = (ctx ! collection(auth, Match(IndexRef("index2")))).elems
      res1 should equal(Seq(RefL(scopeID, inst1.id), RefL(scopeID, inst2.id)))
      res2 should equal(Seq(RefL(scopeID, inst1.id), RefL(scopeID, inst2.id)))
    }

    "updating an index reflects in the cache" in {
      ctx ! mkCollection(auth, MkObject("name" -> "indexsrc"))
      val oldIdx = ctx ! mkIndex(auth, "oldindex", "indexsrc", Nil)
      val q = evalQuery(auth, Clock.time, Get(IndexRef(oldIdx.name)))

      (ctx ! q) match {
        case Right(VersionL(v, _)) => v.id should equal(oldIdx.id.toDocID)
        case r                     => sys.error(s"Unexpected: $r")
      }

      (ctx ! runQuery(
        auth,
        Clock.time,
        Update(IndexRef(oldIdx.name), MkObject("name" -> "newindex"))))
      exec.step()

      (ctx ! q) match {
        case Right(r)        => sys.error(s"Unexpected: $r")
        case Left(List(err)) => err shouldBe a[UnresolvedRefError]
        case Left(errors)    => fail(s"Unexpected errors: $errors.")
      }
    }

    "source style round-trips" in {
      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(MkObject("name" -> "source_ref", "source" -> ClassesRef)))
      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "source_ref_list",
            "source" -> JSArray(ClassesRef, IndexesRef))))
      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "source_config",
            "source" -> MkObject("class" -> ClassesRef))))
      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "source_config_list",
            "source" -> JSArray(
              MkObject("class" -> ClassesRef),
              MkObject("class" -> IndexesRef)))))

      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "querified",
            "source" ->
              MkObject(
                "class" -> ClassesRef,
                "fields" -> MkObject("field1" -> QueryF(Lambda(
                  "inst" -> Select(JSArray("data", "nickname"), Var("inst")))))),
            "terms" -> JSArray(MkObject("binding" -> "field1"))
          ))
      )

      val collectionIDV = DocIDV(collectionID)
      val indexIDV = DocIDV(indexID)

      val sourceRef = ctx ! getInstance(auth, IndexRef("source_ref"), Clock.time)
      sourceRef.data.fields.get(List("source")) should equal(Some(collectionIDV))

      val sourceRefList =
        ctx ! getInstance(auth, IndexRef("source_ref_list"), Clock.time)
      sourceRefList.data.fields.get(List("source")) should equal(
        Some(ArrayV(collectionIDV, indexIDV)))

      val sourceConfig =
        ctx ! getInstance(auth, IndexRef("source_config"), Clock.time)
      sourceConfig.data.fields.get(List("source")) should equal(
        Some(MapV("class" -> collectionIDV)))

      val sourceConfigList =
        ctx ! getInstance(auth, IndexRef("source_config_list"), Clock.time)
      sourceConfigList.data.fields.get(List("source")) should equal(
        Some(ArrayV(MapV("class" -> collectionIDV), MapV("class" -> indexIDV))))

      val querified = ctx ! getInstance(auth, IndexRef("querified"), Clock.time)
      querified.data.fields.get(List("source")) should equal(
        Some(
          MapV(
            "class" -> collectionIDV,
            "fields" -> MapV(
              "field1" -> QueryV(
                "inst",
                MapV(
                  "select" -> ArrayV("data", "nickname"),
                  "from" -> MapV("var" -> "inst"))))
          )))
    }

    "enforces uniqueness" in {
      ctx ! mkCollection(auth, MkObject("name" -> "a"))
      ctx ! mkCollection(auth, MkObject("name" -> "b"))

      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "as",
            "source" -> ClassRef("a"),
            "terms" -> List(MkObject("field" -> List("data", "a"))),
            "unique" -> true)))

      ctx ! runQuery(
        auth,
        Clock.time,
        CreateIndex(
          MkObject(
            "name" -> "bs",
            "source" -> ClassRef("b"),
            "terms" -> List(MkObject("field" -> List("data", "b"))),
            "values" -> List(MkObject("field" -> List("data", "c"))),
            "unique" -> true
          ))
      )

      ctx ! mkDoc(auth, "a", params = MkObject("data" -> MkObject("a" -> 1)))
      ctx ! mkDoc(auth, "a", params = MkObject("data" -> MkObject("a" -> 2)))

      a[RuntimeException] should be thrownBy {
        ctx ! mkDoc(auth, "a", params = MkObject("data" -> MkObject("a" -> 1)))
      }

      ctx ! mkDoc(
        auth,
        "b",
        params = MkObject("data" -> MkObject("b" -> 1, "c" -> 1)))
      ctx ! mkDoc(
        auth,
        "b",
        params = MkObject("data" -> MkObject("b" -> 1, "c" -> 2)))

      a[RuntimeException] should be thrownBy {
        ctx ! mkDoc(
          auth,
          "b",
          params = MkObject("data" -> MkObject("b" -> 1, "c" -> 1)))
      }
    }

    "detects contention" in {
      ctx ! mkCollection(auth, MkObject("name" -> "contended"))

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "cont1",
            "source" -> ClassRef("contended"),
            "active" -> true,
            "terms" -> List(MkObject("field" -> List("data", "a"))),
            "values" -> List(
              MkObject("field" -> List("data", "b"), "reverse" -> true)),
            "serialized" -> true
          ))
      )

      ctx ! mkDoc(
        auth,
        "contended",
        params = MkObject("data" -> MkObject("a" -> 0, "b" -> 0)))

      val futs = 1 to 20 map { _ =>
        ctx
          .withRetryOnContention(maxAttempts = 20)
          .runNow(runQuery(
            auth,
            Let("i" -> Select(0, Paginate(Match(IndexRef("cont1"), 0))))(CreateF(
              ClassRef("contended"),
              MkObject("data" -> MkObject("a" -> 0, "b" -> AddF(1, Var("i"))))))
          ))
      }

      Await.result(futs.join, 30.seconds)

      (ctx ! runQuery(
        auth,
        Clock.time,
        Select(0, Paginate(Match(IndexRef("cont1"), 0))))) should equal(LongL(20))
    }

    "unique indexes are also serialized" in {
      ctx ! mkCollection(auth, MkObject("name" -> "unique_cont"))

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "ucont1",
            "source" -> ClassRef("unique_cont"),
            "active" -> true,
            "terms" -> List(MkObject("field" -> List("data", "a"))),
            "values" -> List(
              MkObject("field" -> List("data", "b"), "reverse" -> true),
              MkObject("field" -> List("data", "c"))),
            "unique" -> true
          ))
      )

      ctx ! mkDoc(
        auth,
        "unique_cont",
        params = MkObject("data" -> MkObject("a" -> 0, "b" -> 0, "c" -> 0)))

      val futs = 1 to 20 map { i =>
        ctx
          .withRetryOnContention(maxAttempts = 20)
          .runNow(runQuery(
            auth,
            Let("i" -> Select(List(0, 0), Paginate(Match(IndexRef("ucont1"), 0))))(
              CreateF(
                ClassRef("unique_cont"),
                MkObject(
                  "data" -> MkObject("a" -> 0, "b" -> AddF(1, Var("i")), "c" -> i))))
          ))
      }

      Await.result(futs.join, 30.seconds)

      (ctx ! runQuery(
        auth,
        Clock.time,
        Select(List(0, 0), Paginate(Match(IndexRef("ucont1"), 0))))) should equal(
        LongL(20))
    }

    "concurrent creation" in {
      ctx ! mkCollection(auth, MkObject("name" -> "concurrent_cls"))

      val futs = 1 to 10 map { i =>
        ctx
          .withRetryOnContention(maxAttempts = 10)
          .runNow(
            runQuery(
              auth,
              CreateIndex(
                MkObject(
                  "name" -> s"concurrent$i",
                  "source" -> ClassRef("concurrent_cls")))))
      }

      Await.result(futs.join, 30.seconds)

      val ids = 1 to 10 map { i =>
        (ctx ! runQuery(auth, Clock.time, Get(IndexRef(s"concurrent$i")))) match {
          case VersionL(vers, _) => vers.docID.subID.toLong
          case _                 => fail()
        }
      }

      ids.distinct.size should equal(10)
    }

    "complex index fanout" in {
      ctx ! mkCollection(auth, MkObject("name" -> "people"))
      ctx ! mkCollection(auth, MkObject("name" -> "relationships"))

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "active_followees_by_follower",
            "source" -> ClassRef("relationships"),
            "active" -> true,
            "terms" -> List(
              MkObject("field" -> List("data", "follower")),
              MkObject("field" -> List("data", "active"))),
            "values" -> List(MkObject("field" -> List("data", "followee")))
          ))
      )

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "latest_active_followees_by_follower",
            "source" -> ClassRef("relationships"),
            "active" -> true,
            "terms" -> List(
              MkObject("field" -> List("data", "follower")),
              MkObject("field" -> List("data", "active"))),
            "values" -> List(
              MkObject("field" -> "ts", "reverse" -> true),
              MkObject("field" -> List("data", "followee")))
          ))
      )

      val alice = ctx ! mkDoc(auth, "people")
      val bob = ctx ! mkDoc(auth, "people")

      val follow = ctx ! mkDoc(
        auth,
        "relationships",
        params = MkObject(
          "data" -> MkObject(
            "follower" -> bob.refObj,
            "followee" -> alice.refObj,
            "active" -> true)))

      val active = ctx ! collection(
        auth,
        Match(IndexRef("active_followees_by_follower"), JSArray(bob.refObj, true)))
      active.elems.size should equal(1)

      val latest = ctx ! collection(
        auth,
        Match(
          IndexRef("latest_active_followees_by_follower"),
          JSArray(bob.refObj, true)))
      latest.elems.size should equal(1)

      ctx ! runQuery(auth, DeleteF(follow.refObj))

      val fs = ctx ! collection(
        auth,
        Match(
          IndexRef("latest_active_followees_by_follower"),
          JSArray(bob.refObj, true)))
      fs.elems.size should equal(0)

    }

    "index already exists" in {
      val ref = (ctx ! runQuery(
        auth,
        Select(
          "ref",
          CreateIndex(
            MkObject("name" -> "someindex", "source" -> ClassRef("animals"))))))
        .asInstanceOf[RefL]
      (ctx ! evalQuery(
        auth,
        CreateIndex(
          MkObject("name" -> "someindex", "source" -> ClassRef("animals"))))) match {
        case Left(errors) =>
          errors shouldBe List(
            InstanceAlreadyExists(ref.id, RootPosition at "create_index"))
        case Right(_) => fail()
      }
    }

    "ttl index entries" in {
      val collName = aName.sample
      val indexName = aName.sample

      ctx ! runQuery(auth, CreateCollection(MkObject("name" -> collName)))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> indexName,
            "active" -> true,
            "source" -> ClassRef(collName))))

      val nonTTLDoc = (ctx ! runQuery(auth, CreateF(ClassRef(collName), MkObject())))
        .asInstanceOf[VersionL]
        .version

      val ttlDoc = (ctx ! runQuery(
        auth,
        CreateF(
          ClassRef(collName),
          MkObject("ttl" -> TimeAdd(Now(), 5, "seconds")))))
        .asInstanceOf[VersionL]
        .version

      eventually(timeout(10.seconds), interval(200.millis)) {
        val page = ctx ! collection(auth, Match(IndexRef(indexName)))

        page.elems should contain(RefL(nonTTLDoc.parentScopeID, nonTTLDoc.id))
        page.elems should not contain (RefL(ttlDoc.parentScopeID, ttlDoc.id))
      }
    }

    "update ttl" in {
      val collName = aName.sample
      val indexName = aName.sample

      ctx ! runQuery(auth, CreateCollection(MkObject("name" -> collName)))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> indexName,
            "partitions" -> 1,
            "active" -> true,
            "source" -> ClassRef(collName))))

      val nonTTLDoc = (ctx ! runQuery(auth, CreateF(ClassRef(collName), MkObject())))
        .asInstanceOf[VersionL]
        .version

      val ttlDoc = (ctx ! runQuery(auth, CreateF(ClassRef(collName), MkObject())))
        .asInstanceOf[VersionL]
        .version

      ctx ! runQuery(
        auth,
        Update(
          MkRef(ClassRef(collName), ttlDoc.id.subID.toLong),
          MkObject("ttl" -> TimeAdd(Now(), 5, "seconds"))))

      eventually(timeout(10.seconds), interval(200.millis)) {
        val page = ctx ! collection(auth, Match(IndexRef(indexName)))

        page.elems should contain(RefL(nonTTLDoc.parentScopeID, nonTTLDoc.id))
        page.elems should not contain RefL(ttlDoc.parentScopeID, ttlDoc.id)
      }
    }

    "allow insert duplicate entry when document is TTL'ed" in {
      val collName = aName.sample
      val indexName = aName.sample

      ctx ! runQuery(auth, CreateCollection(MkObject("name" -> collName)))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> indexName,
            "unique" -> true,
            "active" -> true,
            "source" -> ClassRef(collName),
            "values" -> JSArray(
              MkObject("field" -> JSArray("data", "name"))
            )
          ))
      )

      val name = aName.sample

      val oldDoc = (ctx ! runQuery(
        auth,
        CreateF(
          ClassRef(collName),
          MkObject(
            "ttl" -> TimeAdd(Now(), 5, "seconds"),
            "data" -> MkObject("name" -> name)
          )))).asInstanceOf[VersionL].version

      eventually(timeout(10.seconds), interval(200.millis)) {
        val newDoc = (ctx ! runQuery(
          auth,
          CreateF(
            ClassRef(collName),
            MkObject(
              "data" -> MkObject("name" -> name)
            )))).asInstanceOf[VersionL].version

        newDoc.id should not equal oldDoc.id
      }
    }

    "resolves conflicting history" in {
      ctx ! mkCollection(auth, MkObject("name" -> "widgets"))

      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "widgets_by_price",
            "source" -> ClassRef("widgets"),
            "active" -> true,
            "terms" -> List(MkObject("field" -> List("data", "price")))
          ))
      )

      val doc = ctx ! mkDoc(
        auth,
        "widgets",
        params = MkObject("data" -> MkObject("price" -> 10)))

      // sanity check index snapshot
      val exists = ctx ! collection(auth, Match("widgets_by_price", 10))
      exists.elems should have size 1

      ctx ! runQuery(auth, DeleteF(doc.refObj))

      // sanity check index snapshot again
      val gone = ctx ! collection(auth, Match("widgets_by_price", 10))
      gone.elems should have size 0

      val history = ctx ! events(auth, doc.refObj)
      history.elems should have size 2

      val create = history.elems.head.asInstanceOf[DocEventL].event
      val delete = history.elems.last.asInstanceOf[DocEventL].event

      // attempt to confuse history revision with a conflict
      ctx ! runQuery(
        auth,
        InsertVers(doc.refObj, delete.ts.validTS.micros, delete.action.toString))

      // insert() did not corrupt index history
      val aftermath = ctx ! events(auth, Match("widgets_by_price", 10))
      aftermath.elems should have size 2

      ctx ! runQuery(
        auth,
        RemoveVers(doc.refObj, create.ts.validTS.micros, create.action.toString))

      val fin = ctx ! events(auth, Match("widgets_by_price", 10))
      fin.elems should have size 0
    }

    "preserves multi-value attributes across updates" in {
      val coll = ctx ! mkCollection(auth, MkObject("name" -> "organizations"))
      ctx ! mkIndex(
        auth,
        "organizations_by_user",
        "organizations",
        List(List("data", "teams", "users")))

      ctx ! runQuery(
        auth,
        InsertVers(
          MkRef(ClassRef("organizations"), 1),
          1,
          "create",
          MkObject(
            "data" ->
              MkObject(
                "teams" -> JSArray(
                  MkObject("name" -> "admins", "users" -> JSArray("alice", "bob")),
                  MkObject("name" -> "moderators", "users" -> JSArray("alice")))))
        )
      )

      val initial = ctx ! collection(auth, Match("organizations_by_user", "alice"))
      initial.elems should contain only (RefL(scopeID, ID(1, coll)))

      ctx ! runQuery(
        auth,
        Update(
          MkRef(ClassRef("organizations"), 1),
          MkObject(
            "data" ->
              MkObject(
                "teams" -> JSArray(
                  MkObject("name" -> "admins", "users" -> JSArray("alice", "bob")),
                  MkObject("name" -> "moderators", "users" -> JSArray("charlie")))))
        )
      )

      val update = ctx ! collection(auth, Match("organizations_by_user", "alice"))
      update.elems should contain only (RefL(scopeID, ID(1, coll)))

      ctx ! runQuery(
        auth,
        Update(
          MkRef(ClassRef("organizations"), 1),
          MkObject(
            "data" ->
              MkObject(
                "teams" -> JSArray(
                  MkObject("name" -> "admins", "users" -> JSArray("bob")),
                  MkObject("name" -> "moderators", "users" -> JSArray("charlie")))))
        )
      )

      val remove = ctx ! collection(auth, Match("organizations_by_user", "alice"))
      remove.elems shouldBe empty
    }

    // This is currently a somewhat nonsensical thing to do, because
    // "now()" has not been resolved when index entries are
    // emitted. Users don't know that, however, and have hit a long
    // overflow with this binding form. Someday, this
    // will return the same value as selecting "ts" from the doc.
    "indexes now" in {
      ctx ! mkCollection(auth, MkObject("name" -> "nows"))
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "nows",
            "source" -> MkObject(
              "class" -> ClassRef("nows"),
              "fields" -> MkObject(
                "field1" -> QueryF(Lambda("inst" -> ToMillis(Now())))
              )
            ),
            "active" -> true,
            "values" -> JSArray(MkObject("binding" -> "field1"))
          ))
      )

      ctx ! mkDoc(auth, "nows")

      val pg = ctx ! collection(auth, Match(IndexRef("nows")))
      pg.elems should equal(Seq(LongL(Timestamp.MaxMicros.millis)))
    }

    "limit max index id" in {
      val id = IndexID(Index.UserIndexMaxID.toLong - 1)
      ctx ! SchemaCollection.Index(scopeID).insert(id, Data.empty)

      ctx ! mkCollection(auth, MkObject("name" -> "coll"))
      ctx ! evalQuery(
        auth,
        CreateIndex(MkObject("name" -> "idx0", "source" -> ClsRefV("coll"))))

      the[MaximumIDException.type] thrownBy {
        ctx ! evalQuery(
          auth,
          CreateIndex(MkObject("name" -> "idx1", "source" -> ClsRefV("coll"))))
      } should have message "Maximum sequential ID reached."
    }

    "initial tasks are ordered correctly" in {
      val scopeID1 = ctx ! newScope(RootAuth, "foo", accountID = Some(1))
      val task0 = ctx ! IndexBuild.RootTask.create(scopeID1, IndexID(875098570 + 0))
      val task1 = ctx ! IndexBuild.RootTask.create(scopeID1, IndexID(875098570 + 1))
      val task2 = ctx ! IndexBuild.RootTask.create(scopeID1, IndexID(875098570 + 2))
      val task3 = ctx ! IndexBuild.RootTask.create(scopeID1, IndexID(875098570 + 3))
      val task4 = ctx ! IndexBuild.RootTask.create(scopeID1, IndexID(875098570 + 4))

      val scopeID2 = ctx ! newScope(RootAuth, "bar", accountID = Some(2))
      val otherTask0 =
        ctx ! IndexBuild.RootTask.create(scopeID2, IndexID(875098570 + 0))

      val localID = CassandraService.instance.localID.get
      // Filter out tasks created by other tests
      val tasks = ctx ! Task.getRunnableByHost(localID).flattenT filter { task =>
        task.scopeID == scopeID1 || task.scopeID == scopeID2
      } map {
        _.id
      }

      tasks should contain.inOrderOnly(
        task0.id,
        otherTask0.id,
        task1.id,
        task2.id,
        task3.id,
        task4.id)
    }

    "cannot index databases" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! evalQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "databases_by_name",
            "source" -> DatabasesRef,
            "terms" -> JSArray(MkObject("field" -> List("name")))))) shouldBe Left(
        List(
          ValidationError(
            List(InvalidReference(List("source"))),
            Position("create_index"))))
    }
  }

  "IndexConsistencyCheckSpec" - {

    "increments stat on invalid entry when feature flag is on" in {
      val stats = new StatsRequestBuffer(Set("Index.Consistency.InvalidEntries"))

      val accountID = Random.nextInt(1 << 20)
      val scope = ctx ! newScope(
        RootAuth,
        "index_consistency_check_flag",
        accountID = Some(accountID))
      val auth = Auth.forScope(scope)

      val cls = ctx ! mkCollection(auth, MkObject("name" -> "missing"))
      val schema = (ctx ! CollectionConfig(scope, cls)).get.Schema

      ctx ! Store.insertCreate(schema, ID(1, cls), TS(1), Data.empty)
      ctx ! Store.insertCreate(schema, ID(2, cls), TS(1), Data.empty)

      val ec = EvalContext.read(auth, TS(2), APIVersion.Default, "")

      val readQ = Store
        .collection(
          NativeIndex.DocumentsByCollection(scope),
          Vector(Scalar(cls.toDocID)),
          TS(2))
        .flattenT flatMap { _ =>
        ReadAdaptor.getInstance(ec, scope, ID(1, cls), TS(2), RootPosition)
      }

      CassandraHelper.ffService.setAccount(CheckIndexConsistency, true)
      CassandraHelper.withStats(stats) { ctx ! readQ }
      ctx.cacheContext.schema.invalidate()
      stats.countOrZero("Index.Consistency.InvalidEntries") should equal(0)

      ctx ! Store.removeVersionUnmigrated(
        scope,
        ID(1, cls),
        VersionID(TS(1), Create))

      ctx ! Store.removeVersionUnmigrated(
        scope,
        ID(2, cls),
        VersionID(TS(1), Create))

      CassandraHelper.ffService.setAccount(CheckIndexConsistency, false)
      CassandraHelper.invalidateCaches(ctx)
      CassandraHelper.withStats(stats) { ctx ! readQ }
      stats.countOrZero("Index.Consistency.InvalidEntries") should equal(0)

      CassandraHelper.ffService.setAccount(CheckIndexConsistency, true)
      CassandraHelper.invalidateCaches(ctx)
      CassandraHelper.withStats(stats) { ctx ! readQ }
      stats.countOrZero("Index.Consistency.InvalidEntries") should equal(1)

      val readQ2 = Store
        .collection(
          NativeIndex.DocumentsByCollection(scope),
          Vector(Scalar(cls.toDocID)),
          TS(2))
        .flattenT flatMap { _ =>
        ReadAdaptor.getInstance(ec, scope, ID(1, cls), TS(2), RootPosition)
      } flatMap { _ =>
        ReadAdaptor.getInstance(ec, scope, ID(1, cls), TS(2), RootPosition)
      } flatMap { _ =>
        ReadAdaptor.getInstance(ec, scope, ID(2, cls), TS(2), RootPosition)
      } flatMap { _ =>
        ReadAdaptor.getInstance(ec, scope, ID(3, cls), TS(2), RootPosition)
      }

      // Check that a duplicate docID was skipped
      CassandraHelper.withStats(stats) { ctx ! readQ2 }
      stats.countOrZero("Index.Consistency.InvalidEntries") should equal(1 + 2)
    }
  }
}
