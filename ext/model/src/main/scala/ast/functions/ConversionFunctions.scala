package fauna.ast

import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import java.lang.{
  Byte => JByte,
  Double => JDouble,
  Float => JFloat,
  Integer => JInteger,
  Long => JLong,
  Short => JShort
}
import java.text.NumberFormat
import java.text.ParseException
import java.time.{ LocalDateTime, ZoneOffset }
import scala.annotation.unused

object ToStringFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    lit: Literal,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    lit match {
      case LongL(l)   => Query(Right(StringL(l.toString)))
      case DoubleL(d) => Query(Right(StringL(d.toString)))
      case BoolL(b)   => Query(Right(StringL(b.toString)))
      case NullL      => Query(Right(StringL("null")))
      case _: StringL => Query(Right(lit))
      case TimeL(t)   => Query(Right(StringL(t.toString)))
      case DateL(d)   => Query(Right(StringL(d.toString)))
      case UUIDL(id)  => Query(Right(StringL(id.toString)))
      case ActionL(a) => Query(Right(StringL(a.toString)))
      case _          => Query(Left(List(InvalidCast(lit, Type.String, pos))))
    }
}

object ToNumberFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    lit: Literal,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    lit match {
      case _: NumericL => Query(Right(lit))
      case StringL(s) =>
        try {
          val fmt = NumberFormat.getInstance
          val n = fmt.parse(s.trim)

          n match {
            case _: JByte | _: JShort | _: JInteger | _: JLong =>
              Query(Right(LongL(n.longValue)))
            case _: JFloat | _: JDouble =>
              Query(Right(DoubleL(n.doubleValue)))
            case _ =>
              // bignums aren't supported yet
              Query(Left(List(InvalidCast(lit, Type.Number, pos))))
          }
        } catch {
          case _: ParseException =>
            Query(Left(List(InvalidCast(lit, Type.Number, pos))))
        }
      case _ => Query(Left(List(InvalidCast(lit, Type.Number, pos))))
    }
}

/** Cast the input to a double value.
  * Accepted inputs are numbers (either double or integer) and string.
  * It will return an invalid cast if the input is not a number value,
  * or if the provided string could not be parsed to a float point number.
  */
object ToDoubleFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    lit: Literal,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = lit match {
    case _: DoubleL => Query(Right(lit))
    case LongL(l)   => Query(Right(DoubleL(l.toDouble)))
    case StringL(s) =>
      try {
        val fmt = NumberFormat.getInstance
        val n = fmt.parse(s.trim)
        Query(Right(DoubleL(n.doubleValue)))
      } catch {
        case _: ParseException =>
          Query(Left(List(InvalidCast(lit, Type.Double, pos at "to_double"))))
      }
    case _ => Query(Left(List(InvalidCast(lit, Type.Double, pos at "to_double"))))
  }
}

/** Cast the input to an integer value.
  * Accepted inputs are numbers (either double or integer) and string.
  * It will return an invalid cast if the input is not a number value,
  * or if the provided string could not be parsed to an integer number.
  */
object ToIntegerFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    lit: Literal,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = lit match {
    case _: LongL   => Query(Right(lit))
    case DoubleL(d) => Query(Right(LongL(d.toLong)))
    case StringL(s) =>
      try {
        val fmt = NumberFormat.getInstance
        val n = fmt.parse(s.trim)
        Query(Right(LongL(n.longValue)))
      } catch {
        case _: ParseException =>
          Query(Left(List(InvalidCast(lit, Type.Integer, pos at "to_integer"))))
      }
    case _ => Query(Left(List(InvalidCast(lit, Type.Integer, pos at "to_integer"))))
  }
}

object ToTimeFunction extends QFunction {
  val effect = Effect.Pure

  def apply(lit: Literal, ec: EvalContext, pos: Position): Query[R[Literal]] =
    lit match {
      case LongL(l)   => Query(Right(TimeL(Timestamp.ofMillis(l))))
      case _: TimeL   => Query(Right(lit))
      case DateL(d)   => Query(Right(TimeL(Timestamp(d))))
      case StringL(s) => TimeFunction(s, ec, pos)
      case _          => Query(Left(List(InvalidCast(lit, Type.Time, pos))))
    }
}

object ToDateFunction extends QFunction {
  val effect = Effect.Pure

  def apply(lit: Literal, ec: EvalContext, pos: Position): Query[R[Literal]] =
    lit match {
      case TimeL(t) =>
        Query(
          Right(
            DateL(LocalDateTime.ofInstant(t.toInstant, ZoneOffset.UTC).toLocalDate)))
      case _: DateL   => Query(Right(lit))
      case StringL(s) => DateFunction(s, ec, pos)
      case _          => Query(Left(List(InvalidCast(lit, Type.Date, pos))))
    }
}

object ToObjectFunction extends QFunction {
  val effect = Effect.Pure

  def apply(lit: Literal, ec: EvalContext, pos: Position): Query[R[Literal]] =
    lit match {
      case a: ArrayL =>
        convert(a, pos at "to_object")

      case _: ObjectL =>
        Query.value(Right(lit))

      case v: VersionL =>
        Query.value(Right(ObjectL(v.fields(ec.apiVers))))

      case _ =>
        Query.value(Left(List(InvalidCast(lit, Type.Object, pos at "to_object"))))
    }

  private def convert(lit: ArrayL, pos: Position): Query[R[ObjectL]] =
    Query.value(Casts.ZeroOrMore(Casts.Field)(lit, pos)) mapT { ObjectL(_) }
}

object ToArrayFunction extends QFunction {
  val effect = Effect.Pure

  def apply(lit: Literal, ec: EvalContext, pos: Position): Query[R[Literal]] =
    lit match {
      case _: ArrayL =>
        Query.value(Right(lit))

      case ObjectL(elems) =>
        Query.value(Right(ArrayL(elems map toArray)))

      case v: VersionL =>
        Query.value(Right(ArrayL(v.fields(ec.apiVers) map toArray)))

      case _ =>
        Query.value(Left(List(InvalidCast(lit, Type.Array, pos at "to_array"))))
    }

  private def toArray(kv: (String, Literal)): ArrayL =
    ArrayL(List(StringL(kv._1), kv._2))
}
