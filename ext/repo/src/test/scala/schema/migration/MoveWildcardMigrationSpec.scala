package fauna.repo.test

import fauna.repo.schema.migration._
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._

class MoveWildcardMigrationSpec extends Spec {
  import IRValue._

  implicit class DataOps(data: Data) {
    def move_wildcard(
      field: ConcretePath,
      into: ConcretePath,
      fields: Set[String]): Data =
      Migration.MoveWildcard(field, into, fields).migrate(data)
  }

  "moves non-conforming values" in {
    // `foo`, `bar`, and `dump` are in the schema. Everything else is moved into
    // `dump`.
    Data(
      MapV(
        "data" -> MapV("foo" -> 3, "bar" -> 4, "baz" -> 5, "qux" -> 6),
        "aaa" -> 7,
        "bbb" -> 8)).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump", "foo", "bar")) shouldBe Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 5, "qux" -> 6)),
        "aaa" -> 7,
        "bbb" -> 8)
    )
  }

  "merges dump field" in {
    Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("already_here" -> 10),
          "baz" -> 5,
          "qux" -> 6),
        "aaa" -> 7,
        "bbb" -> 8)).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump", "foo", "bar")) shouldBe Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("already_here" -> 10, "baz" -> 5, "qux" -> 6)),
        "aaa" -> 7,
        "bbb" -> 8)
    )
  }

  "munges names" in {
    Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 10),
          "baz" -> 5,
          "qux" -> 6))).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump", "foo", "bar")) shouldBe Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 10, "_baz" -> 5, "qux" -> 6)))
    )
  }

  "munges names consistently for weird names" in {
    val res0 = Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 10),
          "baz" -> 5,
          "_baz" -> 6))).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump", "foo", "bar"))

    res0 shouldBe Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 10, "_baz" -> 5, "__baz" -> 6)))
    )
    // Field ordering should be maintained.
    res0.fields
      .get(List("data", "dump"))
      .get
      .asInstanceOf[MapV]
      .elems shouldBe List[(String, IRValue)](
      "baz" -> 10,
      "_baz" -> 5,
      "__baz" -> 6
    )

    // If we don't sort the non-matching fields, we'll get a different result here.
    val res1 = Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 10),
          "_baz" -> 6,
          "baz" -> 5))).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump", "foo", "bar"))
    res1 shouldBe Data(
      MapV(
        "data" -> MapV(
          "foo" -> 3,
          "bar" -> 4,
          "dump" -> MapV("baz" -> 10, "_baz" -> 5, "__baz" -> 6)))
    )
    // Field ordering should be maintained.
    res1.fields
      .get(List("data", "dump"))
      .get
      .asInstanceOf[MapV]
      .elems shouldBe List[(String, IRValue)](
      "baz" -> 10,
      "__baz" -> 6,
      "_baz" -> 5
    )

    // This is a bit odd. `baz` is converted to `_baz`, and `_baz` is converted to
    // `___baz`. This behavior is consistent, and its the best we can do when there
    // are conflicting fields in the mix.
    val res2 = Data(
      MapV(
        "data" -> MapV(
          "dump" -> MapV("baz" -> 10, "__baz" -> 11),
          "_baz" -> 6,
          "baz" -> 5))).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump", "foo", "bar"))
    res2 shouldBe Data(
      MapV(
        "data" -> MapV(
          "dump" -> MapV("baz" -> 10, "_baz" -> 5, "__baz" -> 11, "___baz" -> 6)))
    )
    // Field ordering should be maintained.
    res2.fields
      .get(List("data", "dump"))
      .get
      .asInstanceOf[MapV]
      .elems shouldBe List[(String, IRValue)](
      "baz" -> 10,
      "__baz" -> 11,
      "___baz" -> 6,
      "_baz" -> 5
    )
  }

  "blows up if `field` is not a struct" in {
    val ex =
      the[IllegalStateException] thrownBy Data(MapV("data" -> 3)).move_wildcard(
        ConcretePath("data"),
        ConcretePath("data", "dump"),
        Set("dump", "foo", "bar"))
    ex.toString should include("Expected struct")
  }

  "refuses to overwrite non-struct dump field" in {
    val ex =
      the[IllegalStateException] thrownBy Data(MapV("data" -> MapV("dump" -> 3)))
        .move_wildcard(
          ConcretePath("data"),
          ConcretePath("data", "dump"),
          Set("dump", "foo", "bar"))
    ex.toString should include("Refusing to overwrite existing into field")
  }

  "doesn't add a struct when nothing conflicts" in {
    // `foo`, `bar`, and `dump` are in the schema. Everything else is moved into
    // `dump`.
    Data(MapV("data" -> MapV())).move_wildcard(
      ConcretePath("data"),
      ConcretePath("data", "dump"),
      Set("dump")) shouldBe Data(
      MapV("data" -> MapV())
    )
  }
}
