package fauna.repo.test

import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.ir._

class FieldSpec extends Spec {
  "read" - {
    val field1 = Field[String]("foo", "bar", "baz")
    val fieldOpt1 = Field[Option[String]]("foo", "bar", "baz")
    val field2 = Field[String]("foo", "bar")
    val fieldOpt2 = Field[Option[String]]("foo", "bar")

    "reads a value" in {
      val data1 = MapV("foo" -> MapV("bar" -> MapV("baz" -> "quux"))).toData
      val data2 = MapV("foo" -> MapV("bar" -> "baz")).toData
      val data3 = MapV("bar" -> MapV("baz" -> "quux")).toData

      field1.read(data1.fields) should equal (Right("quux"))
      data1(field1) should equal ("quux")
      data1(fieldOpt1) should equal (Some("quux"))

      field1.read(data2.fields) should equal (Left(List(InvalidType(field1.path, MapV.Type, StringV.Type))))
      a[Field.ReadException] should be thrownBy data2(field1)
      a[Field.ReadException] should be thrownBy data2(fieldOpt1)

      field1.read(data3.fields) should equal (Left(List(ValueRequired(field1.path))))
      a[Field.ReadException] should be thrownBy data3(field1)
      data3(fieldOpt1) should equal (None)

      field2.read(data1.fields) should equal (Left(List(InvalidType(field2.path, StringV.Type, MapV.Type))))
      a[Field.ReadException] should be thrownBy data1(field2)
      a[Field.ReadException] should be thrownBy data1(fieldOpt2)

      field2.read(data2.fields) should equal (Right("baz"))
      data2(field2) should equal ("baz")
      data2(fieldOpt2) should equal (Some("baz"))

      field2.read(data3.fields) should equal (Left(List(ValueRequired(field2.path))))
      a[Field.ReadException] should be thrownBy data3(field2)
      data3(fieldOpt2) should equal (None)
    }

    "reads one or more values" in {
      object Foo {
        lazy implicit val codec = FieldType.RecordCodec[Foo]
      }
      case class Foo(bar: Int, baz: Int)

      val invalid = MapV("foo" -> MapV("bar" -> 0)).toData
      val valid = MapV("foo" -> MapV("bar" -> 0, "baz" -> 0)).toData
      val hybrid = MapV("foo" -> ArrayV(MapV("bar" -> 0, "baz" -> 0), MapV("bar" -> 0))).toData
      val field = Field.OneOrMore[Foo]("foo")

      valid(field) should equal (Vector(Foo(0, 0)))
      field.read(invalid.fields) should equal (Left(List(ValueRequired(List("baz")))))
      a[Field.ReadException] should be thrownBy invalid(field)
      field.read(hybrid.fields) should equal (Left(List(ValueRequired(List("baz")))))
      a[Field.ReadException] should be thrownBy hybrid(field)
    }

    "reads an array of values" in {
      val field = Field[Vector[Long]]("foo", "bar")
      val data = MapV("foo" -> MapV("bar" -> ArrayV(LongV(1), LongV(2)))).toData
      val bad = MapV("foo" -> MapV("bar" -> ArrayV(StringV("bad"), LongV(2)))).toData

      data(field) should equal (List(1, 2))
      field.read(bad.fields) should equal (Left(List(InvalidType(field.path, LongV.Type, StringV.Type))))
      a[Field.ReadException] should be thrownBy bad(field)
    }

    "reads a large array of values" in {
      val field = Field[Vector[Long]]("foo")
      val list = for (i <- 0 until 1_000_000) yield i
      val data = MapV("foo" -> ArrayV(list map { LongV(_) }: _*)).toData

      data(field) should equal (list.toList)
    }

    "reads a map of values" in {
      val field = Field[List[(String, Long)]]("foo", "bar")
      val data = MapV("foo" -> MapV("bar" -> MapV("baz" -> LongV(1), "qux" -> LongV(2)))).toData
      val bad = MapV("foo" -> MapV("bar" -> MapV("baz" -> StringV("bad"), "qux" -> LongV(2)))).toData

      data(field) should equal (List("baz" -> 1, "qux" -> 2))
      field.read(bad.fields) should equal (Left(List(InvalidType(List("foo", "bar", "baz"), LongV.Type, StringV.Type))))
      a[Field.ReadException] should be thrownBy bad(field)
    }

    "reading a nulled value" in {
      val diff1 = MapV("foo" -> NullV).toDiff
      val diff2 = MapV("foo" -> MapV("bar" -> NullV)).toDiff

      field2.read(diff1.fields) should equal (Left(List(ValueRequired(field2.path))))
      field2.read(diff2.fields) should equal (Left(List(ValueRequired(field2.path))))
      fieldOpt2.read(diff1.fields) should equal (Right(None))
      fieldOpt2.read(diff2.fields) should equal (Right(None))
    }
  }

  "update/set/clear" - {
    val field1 = Field[String]("foo", "bar")
    val fieldOpt1 = Field[Option[String]]("foo", "bar")
    val field2 = Field[Long]("baz")
    val field3 = Field[Boolean]("foo", "qux", "quux")

    "update" - {
      "modifies a Data" in {
        val data = Data(field1 -> "a string", field2 -> 123, field3 -> false)
        data should equal (MapV("foo" -> MapV("bar" -> "a string", "qux" -> MapV("quux" -> false)), "baz" -> 123).toData)
      }

      "updating a value replaces the previous one" in {
        val longField = Field[Long]("foo", "bar")

        val data1 = MapV("foo" -> MapV("bar" -> "a string")).toData

        field1.read(data1.fields) should equal (Right("a string"))
        longField.read(data1.fields).isLeft should be(true)

        val data2 = data1.update(longField -> 123)

        field1.read(data2.fields).isLeft should be(true)
        longField.read(data2.fields) should equal (Right(123))
      }

      "updating a value to None removes it" in {
        val data = MapV("foo" -> MapV("bar" -> "baz")).toData

        data.update(fieldOpt1 -> None) should equal (Data.empty)
      }
    }

    "set" - {
      "modifies a Diff" in {
        val diff = Diff(field1 -> "a string", field2 -> 123, field3 -> false)
        diff should equal (MapV("foo" -> MapV("bar" -> "a string", "qux" -> MapV("quux" -> false)), "baz" -> 123).toDiff)
      }

      "setting a value replaces the previous one" in {
        val longField = Field[Long]("foo", "bar")

        val diff1 = MapV("foo" -> MapV("bar" -> "a string")).toDiff

        field1.read(diff1.fields) should equal (Right("a string"))
        longField.read(diff1.fields).isLeft should be(true)

        val diff2 = diff1.update(longField -> 123)

        field1.read(diff2.fields).isLeft should be(true)
        longField.read(diff2.fields) should equal (Right(123))
      }

      "setting a value to None produces a diff that will remove it" in {
        val diff = MapV("foo" -> MapV("bar" -> "baz")).toDiff

        diff.update(fieldOpt1 -> None) should equal (MapV("foo" -> MapV("bar" -> NullV)).toDiff)
      }
    }

    "clear" - {
      "removes a value from a Diff" in {
        val field = Field[String]("foo")
        val diff = MapV("foo" -> "yay").toDiff

        diff.clear(field) should equal (Diff.empty)
      }
    }
  }

  "validator" - {
    type Obj = List[(String, String)]

    val ctx = CassandraHelper.context("repo")

    def patched(validator: Validator[Query], map: MapV): MapV = {
      val res = ctx ! validator.patch(Data.empty, map.toDiff)
      res.getOrElse(fail()).fields
    }

    "no sub mask" - {
      "patch ignore invalid keys" in {
        val field = Field[String]("foo")

        patched(field.validator[Query],
          MapV(
            "foo" -> "good!",
            "bad" -> "bad!"
          )
        ) should equal (MapV("foo" -> "good!"))
      }
    }

    "with sub mask" - {
      "patch ignore invalid keys in an object" in {
        val field = Field[Obj]("foo")

        val validator = field.validator[Query](
          MaskTree(List("foo", "bar")) merge
            MaskTree(List("sibling"))
        )

        patched(validator,
          MapV(
            "foo" -> MapV(
              "bar" -> "good!",
              "bad" -> "bad!"
            ),
            "sibling" -> "good!",
            "bad" -> "bad!"
          )
        ) should equal (MapV(
          "foo" -> MapV("bar" -> "good!"),
          "sibling" -> "good!"
        ))
      }

      "patch ignore invalid keys in an array of objects" in {
        val field = Field[Vector[Obj]]("foo")
        val validator = field.validator[Query](MaskTree(List("foo"), List("bar")))

        patched(validator,
          MapV(
            "foo" -> ArrayV(
              MapV("bar" -> "good!", "bad" -> "bad!"),
              MapV("bar" -> "good!", "bad" -> "bad!")
            )
          )
        ) should equal (MapV(
          "foo" -> ArrayV(
            MapV("bar" -> "good!"),
            MapV("bar" -> "good!")
          )
        ))
      }

      "patch ignore invalid keys in one or more objects" in {
        val field = Field.OneOrMore[Obj]("foo")
        val validator = field.validator[Query](MaskTree(List("foo"), List("bar")))

        patched(validator,
          MapV(
            "foo" -> MapV(
              "bar" -> "good!",
              "bad" -> "bad!"
            )
          )
        ) should equal (MapV(
          "foo" -> MapV("bar" -> "good!")
        ))

        patched(validator,
          MapV(
            "foo" -> ArrayV(
              MapV("bar" -> "good!", "bad" -> "bad!"),
              MapV("bar" -> "good!", "bad" -> "bad!")
            )
          )
        ) should equal (MapV(
          "foo" -> ArrayV(
            MapV("bar" -> "good!"),
            MapV("bar" -> "good!")
          )
        ))
      }
    }
  }
}
