package fauna.repo.test

import fauna.repo.schema.migration._
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._

class MoveFieldMigrationSpec extends Spec {
  import IRValue._

  implicit class DataOps(data: Data) {
    def move(path: ConcretePath, to: ConcretePath): Data =
      Migration.MoveField(path, to).migrate(data)
  }

  "moves a value" in {
    Data(MapV("data" -> MapV("my_field" -> 5))).move(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "other_field")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> 5))
    )

    Data(MapV("data" -> MapV("my_field" -> MapV("bar" -> "baz")))).move(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "other_field")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> MapV("bar" -> "baz")))
    )
  }

  // This shouldn't really happen, but might as well define the expected behavior.
  "moves internal field to data" in {
    Data(MapV("data" -> MapV("my_field" -> 5), "foo_internal" -> 3)).move(
      ConcretePath("foo_internal"),
      ConcretePath("data", "other_field")) shouldBe Data(
      MapV("data" -> MapV("my_field" -> 5, "other_field" -> 3))
    )
  }

  // This shouldn't really happen, but might as well define the expected behavior.
  "moves data field to internal" in {
    Data(MapV("data" -> MapV("my_field" -> 5, "other_field" -> 3))).move(
      ConcretePath("data", "my_field"),
      ConcretePath("foo_internal")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> 3), "foo_internal" -> 5)
    )
  }

  "does nothing if the previous field doesn't exist" in {
    Data(MapV("data" -> MapV("other_field" -> 3))).move(
      ConcretePath("data", "my_field"),
      ConcretePath("data", "new_field")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> 3))
    )
  }

  "disallows overwriting a field" in {
    val ex0 =
      the[IllegalStateException] thrownBy Data(
        MapV("data" -> MapV("my_field" -> 3, "other_field" -> 5)))
        .move(ConcretePath("data", "my_field"), ConcretePath("data", "other_field"))
    ex0.toString should include("Refusing to overwrite existing field")

    // Even if the previous field is undefined, if the new field is set, this should
    // blow up.
    val ex1 =
      the[IllegalStateException] thrownBy Data(
        MapV("data" -> MapV("other_field" -> 5)))
        .move(ConcretePath("data", "my_field"), ConcretePath("data", "other_field"))
    ex1.toString should include("Refusing to overwrite existing field")
  }

  "works for nested fields" in {
    Data(MapV("data" -> MapV("my_field" -> MapV("foo" -> 5)))).move(
      ConcretePath("data", "my_field", "foo"),
      ConcretePath("data", "my_field", "bar")) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV("bar" -> 5))))
  }

  "works for nested fields with different parents" in {
    Data(
      MapV("data" -> MapV("my_field" -> MapV("foo" -> 5), "other_field" -> MapV())))
      .move(
        ConcretePath("data", "my_field", "foo"),
        ConcretePath("data", "other_field", "foo")) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV(), "other_field" -> MapV("foo" -> 5))))
  }
}
