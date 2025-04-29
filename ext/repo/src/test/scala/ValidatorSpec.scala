package fauna.repo.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.ir._

class ValidatorSpec extends Spec {
  val ctx = CassandraHelper.context("repo", Nil)
  val db = ScopeID(ctx.nextID())
  val id = None
  val ts = Timestamp.Epoch

  "Validator" - {
    "filters out invalid fields" in {
      val validator = Field[String]("foo", "bar").validator[Query]

      val diff = MapV("foo" -> MapV("bar" -> "good!", "bad" -> "bad!")).toDiff
      val data = ctx ! validator.patch(Data.empty, diff)

      data should equal (Right(MapV("foo" -> MapV("bar" -> "good!")).toData))
    }

    "masks across array boundaries" in {
      object validator extends Validator[Query] {
        val filterMask = MaskTree(List("foo"), List("bar"))
      }

      val data = MapV("foo" -> ArrayV(MapV("bar" -> "good!", "bad" -> "bad!"), MapV("bar" -> "very good!", "bad" -> "still bad!"))).toData

      validator.select(data) should equal (MapV("foo" -> ArrayV(MapV("bar" -> "good!"), MapV("bar" -> "very good!"))).toData)
    }

    "returns error on mismatched field" in {
      val validator = Field[String]("foo", "bar").validator[Query]
      val diff = MapV("foo" -> MapV("bar" -> 2)).toDiff
      val data = ctx ! validator.patch(Data.empty, diff)

      data should equal (Left(List(InvalidType(List("foo", "bar"), StringV.Type, LongV.Type))))
    }

    "returns error on missing required fields" in {
      val validator = Field[String]("foo").validator[Query]
      val data1 = MapV("foo" -> "yay").toData
      val data2 = Data.empty
      val diff = data1 diffTo data2
      val err = List(ValueRequired(List("foo")))

      (ctx ! validator.validate(data1)).isEmpty should be(true)
      (ctx ! validator.validate(data2)) should equal (err)
      (ctx ! validator.patch(data1, diff)) should equal (Left(err))
    }

    "validates missing optional field" in {
      val validator = Field[Option[String]]("foo").validator[Query]
      val data1 = MapV("foo" -> "yay").toData
      val data2 = Data.empty
      val diff = data1 diffTo data2

      (ctx ! validator.validate(data1)).isEmpty should be(true)
      (ctx ! validator.validate(data2)).isEmpty should be(true)
      (ctx ! validator.patch(data1, diff)) should equal (Right(data2))
    }

    "respect filterMask even when validator fails with empty list of errors" in {
      val failingValidator = new Validator[Query] {
        val HashField = Field[Option[String]]("hashed_secret")

        protected val filterMask = MaskTree(HashField.path)

        override protected def validatePatch(current: Data, diff: Diff) =
          Query.value(Left(List()))
      }

      val validator = Field[String]("role").validator[Query] +
        Field[Option[String]]("database").validator[Query] +
        Field[Option[String]]("priority").validator[Query] +
        failingValidator

      val ttl = TimeV(Timestamp.ofMicros(1))

      val data = Data.empty
      val diff = MapV("role" -> "admin", "ttl" -> ttl).toDiff

      (ctx ! validator.patch(data, diff)) should equal (Right(MapV("role" -> "admin").toData))
    }
  }

  "MapValidator" - {
    "optionally allows a map" in {
      val validator = MapValidator[Query](LongV.Type, "foo")

      val diff1 = MapV("foo" -> MapV("bar" -> LongV(1))).toDiff
      val diff2 = MapV("foo" -> NullV).toDiff
      val diff3 = MapV("foo" -> "bad!").toDiff
      val diff4 = MapV("foo" -> MapV("bar" -> 1, "baz" -> "bad!")).toDiff

      (ctx ! validator.validate(Data.empty patch diff1)) should equal (Nil)
      (ctx ! validator.validate(Data.empty patch diff2)) should equal (Nil)
      (ctx ! validator.validate(Data.empty patch diff3)) should equal (List(InvalidType(List("foo"), MapV.Type, StringV.Type)))
      (ctx ! validator.validate(Data.empty patch diff4)) should equal (List(InvalidType(List("foo", "baz"), LongV.Type, StringV.Type)))

      (ctx ! validator.patch(Data.empty, diff1)) should equal (Right(MapV("foo" -> MapV("bar" -> 1)).toData))
      (ctx ! validator.patch(Data.empty, diff2)) should equal (Right(Data.empty))
      (ctx ! validator.patch(Data.empty, diff3)) should equal (Left(List(InvalidType(List("foo"), MapV.Type, StringV.Type))))
      (ctx ! validator.patch(Data.empty, diff4)) should equal (Left(List(InvalidType(List("foo", "baz"), LongV.Type, StringV.Type))))
    }
  }

  "ArrayValidator" - {
    "optionally allows a array" in {
      val validator = ArrayValidator[Query](LongV.Type, "foo")

      val diff1 = MapV("foo" -> ArrayV(1, 2)).toDiff
      val diff2 = MapV("foo" -> NullV).toDiff
      val diff3 = MapV("foo" -> "bad!").toDiff
      val diff4 = MapV("foo" -> ArrayV(1, 2, "bad!")).toDiff

      (ctx ! validator.validate(Data.empty patch diff1)) should equal (Nil)
      (ctx ! validator.validate(Data.empty patch diff2)) should equal (Nil)
      (ctx ! validator.validate(Data.empty patch diff3)) should equal (List(InvalidType(List("foo"), ArrayV.Type, StringV.Type)))
      (ctx ! validator.validate(Data.empty patch diff4)) should equal (List(InvalidType(List("foo", "2"), LongV.Type, StringV.Type)))

      (ctx ! validator.patch(Data.empty, diff1)) should equal (Right(MapV("foo" -> ArrayV(1, 2)).toData))
      (ctx ! validator.patch(Data.empty, diff2)) should equal (Right(Data.empty))
      (ctx ! validator.patch(Data.empty, diff3)) should equal (Left(List(InvalidType(List("foo"), ArrayV.Type, StringV.Type))))
      (ctx ! validator.patch(Data.empty, diff4)) should equal (Left(List(InvalidType(List("foo", "2"), LongV.Type, StringV.Type))))
    }
  }
}
