package fauna.repo.test

import fauna.lang.clocks.Clock
import fauna.repo.schema.migration._
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType._
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._
import org.scalactic.source.Position

class EncodeMigrationSpec extends Spec {
  import IRValue._
  import MigrationCodec._

  def roundtrip(m: Migration, v: MapV)(implicit pos: Position) = {
    // Note that this will get stored in a doc, where nulls will be elided.
    val ir = Data(v).elideNulls.fields

    Data(m.encode).elideNulls.fields shouldBe ir
    MigrationCodec.decode(ir) shouldBe m
  }

  "it roundtrips" in {
    roundtrip(
      Migration.AddField(ConcretePath("foo", "bar", "baz"), Int, 5),
      MapV(
        "type" -> "add",
        "path" -> ArrayV("foo", "bar", "baz"),
        "discriminator" -> "Int",
        "value" -> 5
      ))

    roundtrip(
      Migration.DropField(ConcretePath("foo", "bar", "baz")),
      MapV(
        "type" -> "drop",
        "path" -> ArrayV("foo", "bar", "baz")
      ))

    roundtrip(
      Migration
        .MoveField(
          ConcretePath("foo", "bar", "baz"),
          ConcretePath("aaa", "bbb", "ccc")),
      MapV(
        "type" -> "move",
        "path" -> ArrayV("foo", "bar", "baz"),
        "move_to" -> ArrayV("aaa", "bbb", "ccc")
      )
    )

    roundtrip(
      Migration.MoveConflictingFields(ConcretePath("foo", "bar", "baz")),
      MapV("type" -> "move_conflicting", "into" -> ArrayV("foo", "bar", "baz"))
    )

    roundtrip(
      Migration
        .SplitField(
          ConcretePath("foo", "bar", "baz"),
          ConcretePath("aaa", "bbb", "ccc"),
          Union(Str, Null),
          "i am a value",
          "i am another value"),
      MapV(
        "type" -> "split",
        "path" -> ArrayV("foo", "bar", "baz"),
        "move_to" -> ArrayV("aaa", "bbb", "ccc"),
        "expected" -> MapV("type" -> "Union", "elems" -> ArrayV("Str", "Null")),
        "replace" -> "i am a value",
        "backfill" -> "i am another value"
      )
    )

    roundtrip(
      Migration
        .MoveWildcard(
          ConcretePath("foo", "bar", "baz"),
          ConcretePath("aaa", "bbb", "ccc"),
          Set("foo", "bar", "", "123")),
      MapV(
        "type" -> "move_wildcard",
        "path" -> ArrayV("foo", "bar", "baz"),
        "into" -> ArrayV("aaa", "bbb", "ccc"),
        "keep" -> ArrayV("foo", "bar", "", "123")
      )
    )
  }

  "it encodes weird values" in {
    def roundtripValue(v: IRValue)(implicit pos: Position) = {
      roundtrip(
        Migration.AddField(ConcretePath("foo"), Int, v),
        MapV(
          "type" -> "add",
          "path" -> ArrayV("foo"),
          "discriminator" -> "Int",
          "value" -> v)
      )
      roundtrip(
        Migration.SplitField(ConcretePath("foo"), ConcretePath("bar"), Str, v, v),
        MapV(
          "type" -> "split",
          "path" -> ArrayV("foo"),
          "move_to" -> ArrayV("bar"),
          "expected" -> "Str",
          "replace" -> v,
          "backfill" -> v)
      )
    }

    roundtripValue("bar")
    roundtripValue(3)
    roundtripValue(2.5)
    roundtripValue(true)
    roundtripValue(TimeV(Clock.time))
    roundtripValue(ArrayV(1, 2, 3))
    roundtripValue(MapV("foo" -> "bar", "baz" -> 5))
    // this should never happen. ah well might as well test behavior.
    roundtripValue(TransactionTimeV(false))
    roundtripValue(NullV)
  }

  "it encodes complex types" in {
    roundtrip(
      Migration.SplitField(
        ConcretePath("foo"),
        ConcretePath("bar"),
        Record(
          "foo" -> Union(Int, Str),
          "bar" -> Tuple(Str, Union(Int, Str), Int)
        ),
        "aaa",
        "bbb"),
      MapV(
        "type" -> "split",
        "path" -> ArrayV("foo"),
        "move_to" -> ArrayV("bar"),
        "expected" -> MapV(
          "type" -> "Struct",
          "fields" ->
            MapV(
              "foo" -> MapV("type" -> "Union", "elems" -> ArrayV("Int", "Str")),
              "bar" -> MapV(
                "type" -> "Tuple",
                "elems" -> ArrayV(
                  "Str",
                  MapV("type" -> "Union", "elems" -> ArrayV("Int", "Str")),
                  "Int"))
            )
        ),
        "replace" -> "aaa",
        "backfill" -> "bbb"
      )
    )
  }
}
