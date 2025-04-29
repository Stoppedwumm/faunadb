package fauna.repo.test

import fauna.repo.schema.migration._
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._

class MoveConflictingFieldsMigrationSpec extends Spec {
  import IRValue._

  implicit class DataOps(data: Data) {
    def add(path: ConcretePath, discriminator: SchemaType, value: IRValue): Data =
      Migration.AddField(path, discriminator, value).migrate(data)
    def move_conflicts(into: ConcretePath): Data =
      Migration
        .MoveConflictingFields(into)
        .migrate(data)
  }

  "moves a value from __conflicts" in {
    Data(MapV("__conflicts" -> MapV("foo" -> 3)))
      .move_conflicts(ConcretePath("data", "dump")) shouldBe Data(
      MapV("data" -> MapV("dump" -> MapV("foo" -> 3)))
    )
  }

  "moves multiple values from __conflicts" in {
    Data(MapV("__conflicts" -> MapV("foo" -> 3, "bar" -> 4)))
      .move_conflicts(ConcretePath("data", "dump")) shouldBe Data(
      MapV("data" -> MapV("dump" -> MapV("foo" -> 3, "bar" -> 4)))
    )
  }

  "sequences with add" in {
    Data(MapV("foo" -> 3))
      .add(ConcretePath("foo"), Str, "world")
      .move_conflicts(ConcretePath("dump")) shouldBe Data(
      MapV("foo" -> "world", "dump" -> MapV("foo" -> 3))
    )
  }

  "munges names for values that already exist" in {
    Data(MapV("__conflicts" -> MapV("foo" -> 3), "dump" -> MapV("foo" -> 4)))
      .move_conflicts(ConcretePath("dump")) shouldBe Data(
      MapV("dump" -> MapV("foo" -> 4, "_foo" -> 3))
    )

    // This can happen if fields get moved and re-added in a complex migration.
    Data(
      MapV(
        "__conflicts" -> MapV("foo" -> 3, "foo" -> 4),
        "dump" -> MapV("foo" -> 2)))
      .move_conflicts(ConcretePath("dump")) shouldBe Data(
      MapV("dump" -> MapV("foo" -> 2, "_foo" -> 3, "__foo" -> 4))
    )
  }

  "doesn't create a dump field when its not needed" in {
    Data(MapV("foo" -> 3))
      .add(ConcretePath("foo"), Int, 0)
      .move_conflicts(ConcretePath("dump")) shouldBe Data(
      MapV("foo" -> 3)
    )
  }
}
