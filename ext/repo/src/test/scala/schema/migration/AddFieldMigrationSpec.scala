package fauna.repo.test

import fauna.repo.schema.migration._
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType
import fauna.repo.schema.SchemaType._
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._

class AddFieldMigrationSpec extends Spec {
  import IRValue._

  implicit class DataOps(data: Data) {
    def add(path: ConcretePath, discriminator: SchemaType, value: IRValue): Data =
      Migration.AddField(path, discriminator, value).migrate(data)
  }

  "adds a value" in {
    Data().add(ConcretePath("data", "my_field"), Int, 5) shouldBe Data(
      MapV("data" -> MapV("my_field" -> 5))
    )
    Data().add(
      ConcretePath("data", "my_field"),
      ObjectType(StructSchema.wildcard(Any)),
      MapV("a" -> "b")) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV("a" -> "b")))
    )
  }

  "adds a value next to an existing one" in {
    Data(MapV("data" -> MapV("old_field" -> 5)))
      .add(ConcretePath("data", "my_field"), Int, 3) shouldBe Data(
      MapV("data" -> MapV("old_field" -> 5, "my_field" -> 3))
    )
  }

  "adds a top-level field" in {
    Data(MapV("data" -> MapV("old_field" -> 5)))
      .add(ConcretePath("top_level_field"), Int, 3) shouldBe Data(
      MapV("data" -> MapV("old_field" -> 5), "top_level_field" -> 3)
    )
  }

  "doesn't add a null" in {

    Data(MapV("data" -> MapV("old_field" -> 5)))
      .add(ConcretePath("top_level_field"), Union(Int, Null), NullV) shouldBe Data(
      MapV("data" -> MapV("old_field" -> 5))
    )
  }

  "leaves existing fields alone" in {
    Data(MapV("data" -> MapV("my_field" -> 3)))
      .add(ConcretePath("data", "my_field"), Int, 5) shouldBe Data(
      MapV("data" -> MapV("my_field" -> 3))
    )
  }

  "moves conflicting fields to __conflicts" in {
    Data(MapV("data" -> MapV("my_field" -> "hello")))
      .add(ConcretePath("data", "my_field"), Int, 5) shouldBe Data(
      MapV(
        "data" -> MapV("my_field" -> 5),
        "__conflicts" -> MapV("my_field" -> "hello"))
    )
  }

  "doesn't backfill when there's a null" in {
    Data(MapV("foo" -> "hello"))
      .add(ConcretePath("foo"), Union(Int, Null), NullV) shouldBe Data(
      MapV("__conflicts" -> MapV("foo" -> "hello"))
    )
  }

  "works for nested fields" in {
    Data(MapV("data" -> MapV("my_field" -> MapV())))
      .add(ConcretePath("data", "my_field", "nested"), Int, 5) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV("nested" -> 5))))
  }

  "adds the parent of a nested field" in {
    // The migration engine can produce this, if all the nested fields of a struct
    // are added in the same migration.
    Data(MapV("data" -> MapV()))
      .add(ConcretePath("data", "my_field", "nested"), Int, 5) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV("nested" -> 5)))
    )

    // It should work recursively.
    Data(MapV("data" -> MapV()))
      .add(ConcretePath("data", "my_field", "nested", "foo"), Int, 5) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV("nested" -> MapV("foo" -> 5))))
    )
  }
}
