package fauna.repo.test

import fauna.repo.schema._
import fauna.repo.schema.migration._
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType._
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._

class SplitFieldMigrationSpec extends Spec {
  import IRValue._

  implicit class DataOps(data: Data) {
    def split(
      from: ConcretePath,
      to: ConcretePath,
      expected: SchemaType,
      replace: IRValue,
      backfill: IRValue): Data =
      Migration
        .SplitField(from, to, expected, replace, backfill)
        .migrate(data)
  }

  "renames a value when type differs" in {
    Data(MapV("data" -> MapV("my_field" -> 5)))
      .split(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Str,
        "backfill",
        NullV) shouldBe Data(
      MapV("data" -> MapV("my_field" -> "backfill", "new_field" -> 5))
    )
  }

  "keeps a value when type matches" in {
    Data(MapV("data" -> MapV("my_field" -> 5)))
      .split(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Int,
        "backfill",
        NullV) shouldBe Data(
      MapV("data" -> MapV("my_field" -> 5))
    )
  }

  "fills in backfill value when type matches" in {
    Data(MapV("data" -> MapV("my_field" -> 5)))
      .split(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "iroh"),
        Int,
        "backfill",
        "leaf_me_alone") shouldBe Data(
      MapV("data" -> MapV("my_field" -> 5, "iroh" -> "leaf_me_alone"))
    )
  }

  "example of one migration running over a bunch of versions" in {
    val m =
      Migration.SplitField(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Str,
        "my_backfill",
        NullV)

    val versions = Seq(
      Data(MapV("data" -> MapV("my_field" -> 5))),
      Data(MapV("data" -> MapV("my_field" -> "hello"))),
      Data(MapV("data" -> MapV("my_field" -> true, "other_field" -> 3))),
      Data(MapV("data" -> MapV("my_field" -> "bye", "other_field" -> 3)))
    )

    // behold, the migration engine :P
    val transformed = versions.map(m.migrate)

    transformed shouldBe Seq(
      Data(MapV("data" -> MapV("my_field" -> "my_backfill", "new_field" -> 5))),
      Data(MapV("data" -> MapV("my_field" -> "hello"))),
      Data(
        MapV(
          "data" -> MapV(
            "my_field" -> "my_backfill",
            "new_field" -> true,
            "other_field" -> 3))),
      Data(MapV("data" -> MapV("my_field" -> "bye", "other_field" -> 3)))
    )
  }

  "example of one migration replacing with nulls over a bunch of versions" in {
    val m =
      Migration.SplitField(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Union(Str, Null),
        NullV,
        NullV)

    val versions = Seq(
      Data(MapV("data" -> MapV("my_field" -> 5))),
      Data(MapV("data" -> MapV("my_field" -> "hello"))),
      Data(MapV("data" -> MapV("my_field" -> true, "other_field" -> 3))),
      Data(MapV("data" -> MapV("my_field" -> "bye", "other_field" -> 3)))
    )

    // behold, the migration engine :P
    val transformed = versions.map(m.migrate)

    transformed shouldBe Seq(
      Data(MapV("data" -> MapV("new_field" -> 5))),
      Data(MapV("data" -> MapV("my_field" -> "hello"))),
      Data(MapV("data" -> MapV("new_field" -> true, "other_field" -> 3))),
      Data(MapV("data" -> MapV("my_field" -> "bye", "other_field" -> 3)))
    )
  }

  "example of one migration filling in a new field for conforming data" in {
    val m =
      Migration.SplitField(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Str,
        "my_backfill",
        "new_field_default")

    val versions = Seq(
      Data(MapV("data" -> MapV("my_field" -> 5))),
      Data(MapV("data" -> MapV("my_field" -> "hello"))),
      Data(MapV("data" -> MapV("my_field" -> true, "other_field" -> 3))),
      Data(MapV("data" -> MapV("my_field" -> "bye", "other_field" -> 3)))
    )

    // behold, the migration engine :P
    val transformed = versions.map(m.migrate)

    transformed shouldBe Seq(
      Data(MapV("data" -> MapV("my_field" -> "my_backfill", "new_field" -> 5))),
      Data(MapV(
        "data" -> MapV("my_field" -> "hello", "new_field" -> "new_field_default"))),
      Data(
        MapV(
          "data" -> MapV(
            "my_field" -> "my_backfill",
            "new_field" -> true,
            "other_field" -> 3))),
      Data(
        MapV(
          "data" -> MapV(
            "my_field" -> "bye",
            "new_field" -> "new_field_default",
            "other_field" -> 3)))
    )
  }

  "works when there is no input" in {
    Data(MapV()).split(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "new_field"),
      Union(Int, Null),
      NullV,
      NullV) shouldBe Data(
      MapV()
    )

    Data(MapV()).split(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "new_field"),
      Union(Int, Null),
      NullV,
      "hello") shouldBe Data(
      MapV("data" -> MapV("new_field" -> "hello"))
    )

    Data(MapV()).split(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "new_field"),
      Int,
      3,
      NullV) shouldBe Data(
      MapV("data" -> MapV("my_field" -> 3))
    )
  }

  "works with nested structs" in {
    // Note that structs may not have extra fields, unlike typescript.
    Data(MapV("data" -> MapV("my_field" -> MapV("nested1" -> 5, "nested2" -> 6))))
      .split(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Union(Record("nested1" -> Int), Null),
        NullV,
        NullV) shouldBe Data(
      // Field has extra field `nested2`, so it _is_ renamed.
      MapV("data" -> MapV("new_field" -> MapV("nested1" -> 5, "nested2" -> 6)))
    )

    Data(MapV("data" -> MapV("my_field" -> MapV("nested1" -> 5, "nested2" -> 6))))
      .split(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Union(Record("nested1" -> Int, "nested2" -> Int)),
        NullV,
        NullV) shouldBe Data(
      // Field matches, so it is left alone.
      MapV("data" -> MapV("my_field" -> MapV("nested1" -> 5, "nested2" -> 6)))
    )

    Data(MapV("data" -> MapV("my_field" -> MapV("nested1" -> 5, "nested2" -> 6))))
      .split(
        ConcretePath("data", "my_field"),
        ConcretePath("data", "new_field"),
        Union(Record("nested1" -> Int, "nested2" -> Int, "nested3" -> Int), Null),
        NullV,
        NullV
      ) shouldBe Data(
      // Field is missing `nested3`, so it is renamed.
      MapV("data" -> MapV("new_field" -> MapV("nested1" -> 5, "nested2" -> 6)))
    )
  }

  "custom validators aren't run" in {
    Data(MapV("data" -> MapV("my_field" -> MapV("nested" -> 5)))).split(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "new_field"),
      ObjectType(
        StructSchema(
          fields = Map(
            "nested" -> FieldSchema(
              Int,
              validator = (_, _, _) =>
                throw new IllegalStateException("custom validators aren't run"))
          )
        )
      ),
      NullV,
      NullV
    )
  }

  "won't overwrite existing data" in {
    val ex = the[IllegalStateException] thrownBy Data(
      MapV("data" -> MapV("new_field" -> 3))).split(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "new_field"),
      Str,
      NullV,
      NullV
    )
    ex.toString should include("Refusing to overwrite existing field")
  }

  "allows nested fields" in {
    Data(MapV("data" -> MapV("my_field" -> MapV("foo" -> "foo")))).split(
      ConcretePath("data", "my_field", "foo"),
      ConcretePath("data", "my_field", "bar"),
      Int,
      3,
      "") shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV("foo" -> 3, "bar" -> "foo"))))
  }

  "nested fields with different parents" in {
    Data(
      MapV(
        "data" -> MapV("my_field" -> MapV("foo" -> "foo"), "other_field" -> MapV())))
      .split(
        ConcretePath("data", "my_field", "foo"),
        ConcretePath("data", "other_field", "bar"),
        Int,
        3,
        "") shouldBe Data(
      MapV(
        "data" -> MapV(
          "my_field" -> MapV("foo" -> 3),
          "other_field" -> MapV("bar" -> "foo"))))
  }
}
