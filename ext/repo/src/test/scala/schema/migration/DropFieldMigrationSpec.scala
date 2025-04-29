package fauna.repo.test

import fauna.repo.schema.migration._
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._

class DropFieldMigrationSpec extends Spec {
  import IRValue._

  implicit class DataOps(data: Data) {
    def drop(path: ConcretePath): Data = Migration.DropField(path).migrate(data)
  }

  "removes a value" in {
    Data(MapV("data" -> MapV("my_field" -> 3, "other_field" -> 5)))
      .drop(ConcretePath("data", "my_field")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> 5))
    )
    Data(
      MapV(
        "data" -> MapV(
          "my_field" -> MapV("foo" -> "bar"),
          "other_field" -> MapV("a" -> "b"))))
      .drop(ConcretePath("data", "my_field")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> MapV("a" -> "b")))
    )
  }

  "removes internal field" in {
    Data(MapV("data" -> MapV("my_field" -> 3), "foo_internal" -> 5))
      .drop(ConcretePath("foo_internal")) shouldBe Data(
      MapV("data" -> MapV("my_field" -> 3)))
  }

  "removes parent `data` if empty" in {
    Data(MapV("data" -> MapV("my_field" -> 3)))
      .drop(ConcretePath("data", "my_field")) shouldBe Data(MapV())
  }

  "works if there is no field present" in {
    Data(MapV("data" -> MapV("other_field" -> 3)))
      .drop(ConcretePath("data", "my_field")) shouldBe Data(
      MapV("data" -> MapV("other_field" -> 3)))
  }

  "does not remove parent if empty" in {
    Data(MapV("data" -> MapV("my_field" -> MapV("nested" -> 3))))
      .drop(ConcretePath("data", "my_field", "nested")) shouldBe Data(
      MapV("data" -> MapV("my_field" -> MapV())))
  }
}
