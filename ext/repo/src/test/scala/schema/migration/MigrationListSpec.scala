package fauna.repo.test

import fauna.atoms.SchemaVersion
import fauna.repo.schema.migration.{ Migration, _ }
import fauna.repo.schema.ScalarType._
import fauna.storage.doc._
import fauna.storage.ir._
import scala.collection.SortedMap

class MigrationListSpec extends Spec {
  "sorts migrations into a list" in {
    val list = MigrationList(
      SchemaVersion(2) -> Migration.DropField(ConcretePath("a")),
      SchemaVersion(4) -> Migration.DropField(ConcretePath("b")),
      SchemaVersion(4) -> Migration.DropField(ConcretePath("c"))
    )

    list shouldBe MigrationList(
      SortedMap(
        SchemaVersion(2) -> List(
          Migration.DropField(ConcretePath("a")),
          Migration.DropField(ConcretePath("b")),
          Migration.DropField(ConcretePath("c"))
        ),
        SchemaVersion(4) -> List(
          Migration.DropField(ConcretePath("b")),
          Migration.DropField(ConcretePath("c"))
        )
      ))

    // Make sure the list tails are shared.
    (list.elems(SchemaVersion(2)).tail eq list.elems(SchemaVersion(4))) shouldBe true
  }

  "encodes" in {
    val list = MigrationList(
      SchemaVersion(2) -> Migration.DropField(ConcretePath("a")),
      SchemaVersion(4) -> Migration.DropField(ConcretePath("b")),
      SchemaVersion(4) -> Migration.DropField(ConcretePath("c"))
    )
    list shouldBe MigrationList(
      SortedMap(
        SchemaVersion(2) -> List(
          Migration.DropField(ConcretePath("a")),
          Migration.DropField(ConcretePath("b")),
          Migration.DropField(ConcretePath("c"))),
        SchemaVersion(4) -> List(
          Migration.DropField(ConcretePath("b")),
          Migration.DropField(ConcretePath("c")))
      ))

    list.encode shouldBe ArrayV(
      MapV(
        "version" -> TimeV(SchemaVersion(2).ts),
        "migration" -> MapV(
          "type" -> "drop",
          "path" -> ArrayV("a")
        )),
      MapV(
        "version" -> TimeV(SchemaVersion(4).ts),
        "migration" -> MapV(
          "type" -> "drop",
          "path" -> ArrayV("b")
        )),
      MapV(
        "version" -> TimeV(SchemaVersion(4).ts),
        "migration" -> MapV(
          "type" -> "drop",
          "path" -> ArrayV("c")
        ))
    )
  }

  "decodes" in {
    val decoded = MigrationList.decode(
      ArrayV(
        MapV(
          "version" -> TimeV(SchemaVersion(2).ts),
          "migration" -> MapV(
            "type" -> "drop",
            "path" -> ArrayV("a")
          )),
        MapV(
          "version" -> TimeV(SchemaVersion(4).ts),
          "migration" -> MapV(
            "type" -> "drop",
            "path" -> ArrayV("b")
          )),
        MapV(
          "version" -> TimeV(SchemaVersion(4).ts),
          "migration" -> MapV(
            "type" -> "drop",
            "path" -> ArrayV("c")
          ))
      ))

    decoded shouldBe MigrationList(
      SchemaVersion(2) -> Migration.DropField(ConcretePath("a")),
      SchemaVersion(4) -> Migration.DropField(ConcretePath("b")),
      SchemaVersion(4) -> Migration.DropField(ConcretePath("c"))
    )
    decoded shouldBe MigrationList(
      SortedMap(
        SchemaVersion(2) -> List(
          Migration.DropField(ConcretePath("a")),
          Migration.DropField(ConcretePath("b")),
          Migration.DropField(ConcretePath("c"))),
        SchemaVersion(4) -> List(
          Migration.DropField(ConcretePath("b")),
          Migration.DropField(ConcretePath("c")))
      ))
  }

  "applies migrations in order" in {
    val list0 = MigrationList(
      SchemaVersion(2) -> Migration.AddField(ConcretePath("data", "foo"), Int, 5)
    )

    // A bunch of documents at different versions. Note that if an `AddField`
    // migration finds a field already present, it will throw an illegal state
    // exception. So, we know the version check is working if the migrations run
    // successfully.
    val docs0 = Seq(
      Data(MapV()) -> SchemaVersion(1),
      Data(MapV("data" -> MapV("foo" -> 3))) -> SchemaVersion(2),
      Data(MapV("data" -> MapV("foo" -> 3))) -> SchemaVersion(4),
      Data(MapV("data" -> MapV("foo" -> 3, "bar" -> 6))) -> SchemaVersion(5),
      Data(MapV("data" -> MapV("foo" -> 3, "bar" -> 6))) -> SchemaVersion(6)
    )

    // Migration all the versions up to the latest (version 8 for this example).
    val docs1 = docs0.map { case (data, ver) => list0.migrate(data, ver) }
    docs1 shouldBe Seq(
      Data(MapV("data" -> MapV("foo" -> 5))),
      Data(MapV("data" -> MapV("foo" -> 3))),
      Data(MapV("data" -> MapV("foo" -> 3))),
      Data(MapV("data" -> MapV("foo" -> 3, "bar" -> 6))),
      Data(MapV("data" -> MapV("foo" -> 3, "bar" -> 6)))
    )

    // Add another migration to drop the `foo` field.
    val list1 = MigrationList(
      SchemaVersion(2) -> Migration.AddField(ConcretePath("data", "foo"), Int, 5),
      SchemaVersion(10) -> Migration.DropField(ConcretePath("data", "foo"))
    )

    // Run the migration list over all the docs again.
    val docs2 = docs1.map { data => list1.migrate(data, SchemaVersion(8)) }
    docs2 shouldBe Seq(
      Data(MapV()),
      Data(MapV()),
      Data(MapV()),
      Data(MapV("data" -> MapV("bar" -> 6))),
      Data(MapV("data" -> MapV("bar" -> 6)))
    )
  }
}
