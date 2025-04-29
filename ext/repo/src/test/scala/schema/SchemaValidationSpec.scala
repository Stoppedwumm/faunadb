package fauna.repo.test

import fauna.lang.clocks.Clock
import fauna.repo.schema._
import fauna.storage.ir._
import fql.ast.Literal
import java.time.{ LocalDate, ZoneOffset }
import org.scalactic.source.Position
import scala.language.implicitConversions

class SchemaValidationSpec extends Spec {
  val repo = CassandraHelper.context("repo")

  // helps constructing paths
  implicit def strToRight(s: String): Either[Long, String] = Right(s)
  implicit def longToLeft(i: Long): Either[Long, String] = Left(i)

  def validateOk(s: SchemaType, v: IRValue)(implicit pos: Position) = {
    validateErr(s, v) shouldBe empty
  }
  def validateErr(s: SchemaType, v: IRValue)(implicit pos: Position) = {
    val errs = repo ! SchemaType.validate(s, new Path.Prefix(List.empty), v, None)
    SchemaType.isValueOfType(s, v) shouldBe errs.isEmpty
    errs
  }

  def today = LocalDate.ofInstant(Clock.time.toInstant, ZoneOffset.UTC)

  "basic types" in {
    validateOk(ScalarType.Int, 5)
    validateErr(ScalarType.Int, "bar") shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Int, ScalarType.Str)
    )

    validateOk(SchemaType.Record("foo" -> ScalarType.Int), MapV("foo" -> 5))

    validateErr(
      SchemaType.Record("foo" -> ScalarType.Int),
      MapV("foo" -> "bar")) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path("foo"), ScalarType.Int, ScalarType.Str)
    )
  }

  "literal types" in {
    validateOk(ScalarType.Singleton(5), 5)

    // TODO: This definitely should be `Scalar.Singleton(6)` instead of `Scalar.Int`.
    // "Scalar 5 is not a subtype of Int" is meaningless.
    validateErr(ScalarType.Singleton(5), 6) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(5),
        ScalarType.Int)
    )

    // TODO: Maybe this should be `Scalar.Int` instead of `Scalar.Singleton`?
    // "String is not a subtype of 5" is kinda confusing.
    validateErr(ScalarType.Singleton(5), "bar") shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(5),
        ScalarType.Str)
    )

    validateOk(ScalarType.Singleton(Literal.Null), NullV)
    validateErr(ScalarType.Singleton(Literal.Null), true) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(Literal.Null),
        ScalarType.Boolean)
    )

    validateOk(ScalarType.Singleton(Literal.True), true)
    validateErr(ScalarType.Singleton(Literal.True), false) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(Literal.True),
        ScalarType.Singleton(Literal.False))
    )

    validateOk(ScalarType.Singleton(Literal.False), false)
    validateErr(ScalarType.Singleton(Literal.False), true) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(Literal.False),
        ScalarType.Singleton(Literal.True))
    )

    validateOk(ScalarType.Singleton(Literal.Int(5)), 5)
    validateErr(ScalarType.Singleton(Literal.Int(5)), 6) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(Literal.Int(5)),
        ScalarType.Int // FIXME: This should be the literal `6`
      )
    )

    validateOk(ScalarType.Singleton(Literal.Float(2.3)), 2.3)
    validateErr(ScalarType.Singleton(Literal.Float(2.3)), 2.4) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(Literal.Float(2.3)),
        ScalarType.Double // FIXME: This should be the literal `2.4`
      )
    )

    validateOk(ScalarType.Singleton(Literal.Str("hello")), "hello")
    validateErr(ScalarType.Singleton(Literal.Str("hello")), "bye") shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        ScalarType.Singleton(Literal.Str("hello")),
        ScalarType.Singleton(Literal.Str("bye")))
    )
  }

  "numbers" in {
    val anInt = 5
    val aLong = Int.MaxValue + 1L
    val aDouble = 2.3d

    validateOk(ScalarType.Int, anInt)
    validateErr(ScalarType.Int, aLong) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Int, ScalarType.Long)
    )
    validateErr(ScalarType.Int, aDouble) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Int, ScalarType.Double)
    )

    validateOk(ScalarType.Long, anInt)
    validateOk(ScalarType.Long, aLong)
    validateErr(ScalarType.Long, aDouble) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Long, ScalarType.Double)
    )

    validateErr(ScalarType.Double, anInt) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Double, ScalarType.Int)
    )
    validateErr(ScalarType.Double, aLong) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Double, ScalarType.Long)
    )
    validateOk(ScalarType.Double, aDouble)

    validateOk(ScalarType.Number, anInt)
    validateOk(ScalarType.Number, aLong)
    validateOk(ScalarType.Number, aDouble)
  }

  "time" in {
    validateOk(ScalarType.Time, TimeV(Clock.time))
    validateErr(ScalarType.Time, DateV(today)) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Time, ScalarType.Date)
    )
    validateErr(ScalarType.Time, 5) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Time, ScalarType.Int)
    )
  }

  "date" in {
    validateOk(ScalarType.Date, DateV(today))
    validateErr(ScalarType.Date, TimeV(Clock.time)) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Date, ScalarType.Time)
    )
    validateErr(ScalarType.Date, 5) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path.empty, ScalarType.Date, ScalarType.Int)
    )
  }

  "arrays" in {
    validateOk(SchemaType.Array(ScalarType.Int), ArrayV(2, 3, 4))
    validateErr(SchemaType.Array(ScalarType.Int), ArrayV("bar", 3)) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path(0), ScalarType.Int, ScalarType.Str)
    )

    validateOk(SchemaType.Array(ScalarType.Int), ArrayV())
  }

  "tuples" in {
    validateOk(SchemaType.Tuple(ScalarType.Int, ScalarType.Str), ArrayV(2, "hi"))
    validateErr(
      SchemaType.Tuple(ScalarType.Int, ScalarType.Str),
      ArrayV(2, 3)) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path(1), ScalarType.Str, ScalarType.Int)
    )
    validateErr(
      SchemaType.Tuple(ScalarType.Int, ScalarType.Str),
      ArrayV(2)) shouldBe Seq(
      ConstraintFailure.InvalidTupleArity(
        Path.empty,
        SchemaType.Tuple(ScalarType.Int, ScalarType.Str),
        SchemaType.Tuple(ScalarType.Int))
    )
    validateErr(
      SchemaType.Tuple(ScalarType.Int, ScalarType.Str),
      ArrayV(2, 3, 4)) shouldBe Seq(
      ConstraintFailure.InvalidTupleArity(
        Path.empty,
        SchemaType.Tuple(ScalarType.Int, ScalarType.Str),
        SchemaType.Tuple(ScalarType.Int, ScalarType.Int, ScalarType.Int))
    )
  }

  "unions" in {
    validateOk(SchemaType.Union(ScalarType.Int, ScalarType.Str), 2)
    validateOk(SchemaType.Union(ScalarType.Int, ScalarType.Str), "hello")
    validateErr(SchemaType.Union(ScalarType.Int, ScalarType.Str), 2.5) shouldBe Seq(
      ConstraintFailure.TypeMismatch(
        Path.empty,
        SchemaType.Union(ScalarType.Int, ScalarType.Str),
        ScalarType.Double)
    )
  }

  "union of records" in {
    validateOk(
      SchemaType.Union(
        SchemaType.Record("a" -> ScalarType.Int),
        SchemaType.Record("b" -> ScalarType.Int)),
      MapV("a" -> 5))
    validateOk(
      SchemaType.Union(
        SchemaType.Record("a" -> ScalarType.Int),
        SchemaType.Record("b" -> ScalarType.Int)),
      MapV("b" -> 6))

    // "Field `c` doesn't exist in { a: Int } | { b: Int }"
    // "Union requires fields [a] or [b]"
    validateErr(
      SchemaType.Union(
        SchemaType.Record("a" -> ScalarType.Int),
        SchemaType.Record("b" -> ScalarType.Int)),
      MapV("c" -> 7)) shouldBe Seq(
      ConstraintFailure.InvalidField(
        Path("c"),
        SchemaType.Record("a" -> ScalarType.Int),
        SchemaType.Record("c" -> ScalarType.Int),
        "c"),
      ConstraintFailure.UnionMissingFields(
        Path.empty,
        Seq(
          Seq(ConstraintFailure.MissingField(Path("a"), ScalarType.Int, "a")),
          Seq(ConstraintFailure.MissingField(Path("b"), ScalarType.Int, "b"))))
    )
  }

  "records" in {
    validateOk(
      SchemaType.Record("a" -> ScalarType.Int, "b" -> ScalarType.Str),
      MapV("a" -> 5, "b" -> "hello"))
    validateErr(
      SchemaType.Record("a" -> ScalarType.Int, "b" -> ScalarType.Str),
      MapV("a" -> 5, "b" -> 5)) shouldBe Seq(
      ConstraintFailure.TypeMismatch(Path("b"), ScalarType.Str, ScalarType.Int)
    )

    validateErr(
      SchemaType.Record("a" -> ScalarType.Int, "b" -> ScalarType.Str),
      MapV("a" -> 5)) shouldBe Seq(
      ConstraintFailure.MissingField(Path("b"), ScalarType.Str, "b")
    )

    // This error doesn't actually use the expected/provided types in the message, so
    // we can probably remove them.
    validateErr(
      SchemaType.Record("a" -> ScalarType.Int, "b" -> ScalarType.Str),
      MapV("a" -> 5, "b" -> "hello", "c" -> 6)) shouldBe Seq(
      ConstraintFailure.InvalidField(
        Path("c"), // is this path right? the field name seems duplicated.
        SchemaType.Record("a" -> ScalarType.Int, "b" -> ScalarType.Str),
        SchemaType.Record(
          "a" -> ScalarType.Int,
          "b" -> ScalarType.Str,
          "c" -> ScalarType.Int),
        "c"
      )
    )
  }

  "optional fields in records" in {
    validateOk(
      SchemaType.Record(
        "a" -> ScalarType.Int,
        "b" -> SchemaType.Union(ScalarType.Str, ScalarType.Null)),
      MapV("a" -> 5, "b" -> "hello"))
    validateOk(
      SchemaType.Record(
        "a" -> ScalarType.Int,
        "b" -> SchemaType.Union(ScalarType.Str, ScalarType.Null)),
      MapV("a" -> 5))

    // This shouldn't really happen, especially with document data, but we might as
    // well test it.
    validateOk(
      SchemaType.Record(
        "a" -> ScalarType.Int,
        "b" -> SchemaType.Union(ScalarType.Str, ScalarType.Null)),
      MapV("a" -> 5, "b" -> NullV))

    validateErr(
      SchemaType.Record(
        "a" -> ScalarType.Int,
        "b" -> SchemaType.Union(ScalarType.Str, ScalarType.Null)),
      MapV()) shouldBe Seq(
      ConstraintFailure.MissingField(Path("a"), ScalarType.Int, "a")
    )
  }
}
