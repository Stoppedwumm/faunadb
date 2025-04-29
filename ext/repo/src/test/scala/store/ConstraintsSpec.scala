package fauna.repo.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.repo.{ IndexConfig => _, _ }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.storage.{ AtValid, Create }
import fauna.storage.index.IndexTerm
import fauna.storage.ir._

class ConstraintsSpec extends Spec {

  val ctx = CassandraHelper.context("repo")

  def newScope = ScopeID(ctx.nextID())

  val faunaClass = CollectionID(1024)
  def mkIndex(scope: ScopeID) =
    IndexConfig(
      scope,
      IndexID(1024),
      faunaClass,
      Vector(DefaultExtractor(List("data", "foo"))),
      constraint = UniqueValues)

  def ID(subID: Long) = DocID(SubID(subID), faunaClass)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  "Constraints" - {
    "applies constraints forwards in time" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(index))

      ctx ! Store.insert(schema, ID(1), MapV("data" -> MapV("foo" -> "bar")).toData)

      the[UniqueConstraintViolation] thrownBy {
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "bar")).toData)
      }
    }

    "applies constraints backwards in time" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(index))

      val insert = PrivateMethod[Query[Version]](Symbol("addVersionAndIndexEntries"))

      ctx ! Store.insertCreate(
        schema,
        ID(1),
        TS(2),
        MapV("data" -> MapV("foo" -> "bar")).toData)

      the[UniqueConstraintViolation] thrownBy {
        val ver = Version.Live(
          scope,
          ID(2),
          AtValid(TS(3)),
          Create,
          SchemaVersion.Min,
          MapV("data" -> MapV("foo" -> "bar")).toData)
        ctx ! (Store.invokePrivate(insert(schema, ver, true, false)))
      }
    }

    "applies constraints in a transaction" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(index))

      the[UniqueConstraintViolation] thrownBy {
        ctx ! (for {
          _ <- Store.insert(
            schema,
            ID(1),
            MapV("data" -> MapV("foo" -> "bar")).toData)
          _ <- Store.insert(
            schema,
            ID(2),
            MapV("data" -> MapV("foo" -> "bar")).toData)
        } yield ())
      }
    }

    "correctly chooses from conflicting constraints" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(index))

      val terms = Vector(IndexTerm(StringV("bar")))
      ctx ! Store.insertCreate(
        schema,
        ID(1),
        TS(1),
        MapV("data" -> MapV("foo" -> "bar")).toData)
      ctx ! Store.insertCreate(
        schema,
        ID(2),
        TS(2),
        MapV("data" -> MapV("foo" -> "bar")).toData)

      ctx ! Store.uniqueIDForKey(index, terms, TS(1)) should equal(Some(ID(1)))
      ctx ! Store.uniqueIDForKey(index, terms, TS(2)) should equal(Some(ID(2)))
    }

    "seeks past deleted instances" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(index))

      val terms = Vector(IndexTerm(StringV("bar")))
      ctx ! Store.insertCreate(
        schema,
        ID(1),
        TS(1),
        MapV("data" -> MapV("foo" -> "bar")).toData)
      ctx ! Store.insertCreate(
        schema,
        ID(2),
        TS(2),
        MapV("data" -> MapV("foo" -> "bar")).toData)
      ctx ! Store.insertDelete(schema, ID(2), TS(3))

      ctx ! Store.uniqueIDForKey(index, terms, TS(1)) should equal(Some(ID(1)))
      ctx ! Store.uniqueIDForKey(index, terms, TS(3)) should equal(Some(ID(1)))
    }

    "removing a constraint releases the lock" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema =
        CollectionSchema.empty(scope, faunaClass).copy(indexes = List(index))

      val terms = Vector(IndexTerm(StringV("bar")))
      ctx ! Store.insert(schema, ID(1), MapV("data" -> MapV("foo" -> "bar")).toData)

      a[UniqueConstraintViolation] should be thrownBy {
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "bar")).toData)
      }

      ctx ! Store.remove(schema, ID(1))
      ctx ! Store.uniqueIDForKey(index, terms, Clock.time) should equal(None)

      ctx ! Store.insert(schema, ID(3), MapV("data" -> MapV("foo" -> "bar")).toData)

      ctx ! Store.uniqueIDForKey(index, terms, Clock.time) should equal(Some(ID(3)))
    }

    "an exception thrown releases the lock" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(index))

      val data = MapV("data" -> MapV("foo" -> "bar")).toData

      an[Exception] should be thrownBy {
        ctx ! (Store.insert(schema, ID(1), data) map { _ =>
          sys.error("oops")
        })
      }

      noException should be thrownBy {
        ctx ! Store.insert(schema, ID(2), data)
      }
    }

    "updating a constraint releases the lock" in {
      val scope = newScope
      val index = mkIndex(scope)
      val schema = CollectionSchema
        .empty(scope, faunaClass)
        .copy(indexes = List(index))

      ctx ! Store.insert(schema, ID(1), MapV("data" -> MapV("foo" -> "bar")).toData)
      ctx ! Store.insert(schema, ID(1), MapV("data" -> MapV("foo" -> "baz")).toData)
      ctx ! Store.insert(schema, ID(1), MapV("data" -> MapV("foo" -> "bar")).toData)

      a[UniqueConstraintViolation] should be thrownBy {
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "bar")).toData)
      }

      noException should be thrownBy {
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "baz")).toData)
      }

      ctx ! Store.remove(schema, ID(1))

      noException should be thrownBy {
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "bar")).toData)
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "baz")).toData)
        ctx ! Store.insert(
          schema,
          ID(2),
          MapV("data" -> MapV("foo" -> "bar")).toData)
      }
    }
  }
}
