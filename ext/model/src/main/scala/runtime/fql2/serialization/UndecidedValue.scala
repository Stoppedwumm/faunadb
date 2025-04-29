package fauna.model.runtime.fql2.serialization

import fauna.atoms.{ DocID, SubID }
import fauna.codex.json2._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.stdlib.{
  CollectionCompanion,
  SchemaCollectionCompanion
}
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.Span
import java.time.{ DateTimeException, LocalDate }
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.util.control.NoStackTrace

// This is for an invalid value. The message must be lowercase, as this is
// used in the middle of a string in the final response.
case class InvalidValue(message: String) extends NoStackTrace

// This is a class that represents a JSON value that hasn't been decoded
// according to a specific format yet.
sealed trait UndecidedValue {
  def toValue(
    ctx: FQLInterpCtx,
    format: ValueFormat): Query[Either[InvalidValue, Value]] = {
    format match {
      case ValueFormat.Simple => Query.value(Right(parseSimple()))
      case ValueFormat.Tagged | ValueFormat.Decorated => parseTagged(ctx)
    }
  }

  private def parseSimple(): Value = {
    import UndecidedValue._
    this match {
      case Object(v) => Value.Struct.fromSpecific(v.view.mapValues(_.parseSimple()))
      case Array(v)  => Value.Array.fromSpecific(v.map(_.parseSimple()))
      case Int(v)    => Value.Number(v)
      case Float(v)  => Value.Number(v)
      case Str(v)    => Value.Str(v)
      case Bool(v)   => Value.Boolean(v)
      case Null()    => Value.Null(Span.Null)
    }
  }

  private def parseTagged(ctx: FQLInterpCtx): Query[Either[InvalidValue, Value]] = {
    import UndecidedValue._
    this match {
      case Object(values) =>
        if (values.keys.exists(_.startsWith("@"))) {
          if (values.size > 1) {
            return Query.value(Left(InvalidValue("too many keys in tagged value")))
          }

          val (tag, undecided) = values.iterator.next()

          try {
            parseTaggedValue(ctx, tag, undecided)
          } catch {
            case e: InvalidValue => Query.value(Left(e))
          }
        } else {
          parseTaggedObject(ctx, values)
        }
      case Array(values) =>
        values.map { v =>
          v.parseTagged(ctx)
        }.sequenceT mapT { values => Value.Array(values: _*) }
      case Int(_) | Float(_) =>
        Query.value(Left(InvalidValue("numbers must be tagged")))
      case Str(v)  => Query.value(Right(Value.Str(v)))
      case Bool(v) => Query.value(Right(Value.Boolean(v)))
      case Null()  => Query.value(Right(Value.Null(Span.Null)))
    }
  }

  private def parseTaggedObject(
    ctx: FQLInterpCtx,
    fields: SeqMap[String, UndecidedValue]): Query[Either[InvalidValue, Value]] = {
    fields.map { case (k, v) =>
      v.parseTagged(ctx) mapT { (k, _) }
    }.sequenceT mapT { fields => Value.Struct(fields: _*) }
  }

  private def parseTaggedValue(
    ctx: FQLInterpCtx,
    tag: String,
    undecided: UndecidedValue): Query[Either[InvalidValue, Value]] = {
    tag match {
      case ValueFormat.IntTag =>
        val s = undecided.asStrOrFail(tag)
        Query.value(Right(
          Value.Int(s.toIntOption.getOrElse(throw InvalidValue("invalid integer")))))

      case ValueFormat.LongTag =>
        val s = undecided.asStrOrFail(tag)
        Query.value(Right(
          Value.Long(s.toLongOption.getOrElse(throw InvalidValue("invalid long")))))

      case ValueFormat.DoubleTag =>
        val s = undecided.asStrOrFail(tag)
        Query.value(
          Right(
            Value.Double(
              s.toDoubleOption.getOrElse(throw InvalidValue("invalid double")))))

      case ValueFormat.ObjectTag =>
        val inner = undecided.asObjectOrFail(tag)
        parseTaggedObject(ctx, inner)

      case ValueFormat.TimeTag =>
        val time = undecided.asStrOrFail(tag)
        try {
          Query.value(Right(Value.Time(Timestamp.parse(time))))
        } catch {
          case _: DateTimeException => throw InvalidValue("invalid time")
        }

      case ValueFormat.DateTag =>
        val date = undecided.asStrOrFail(tag)
        try {
          Query.value(Right(Value.Date(LocalDate.parse(date))))
        } catch {
          case _: DateTimeException => throw InvalidValue("invalid date")
        }

      case ValueFormat.BytesTag =>
        val encoded = undecided.asStrOrFail(tag)
        try {
          Query.value(Right(Value.Bytes.fromBase64(encoded)))
        } catch {
          case _: IllegalArgumentException => throw InvalidValue("invalid bytes")
        }

      case ValueFormat.ModTag =>
        val mod = undecided.asStrOrFail(tag)
        GlobalContext.lookupSingletonOrUDF(ctx, mod) map {
          case Some(so: Value.SingletonObject) => Right(so)
          case _ => Left(InvalidValue(s"no such module '$mod'"))
        }

      case ValueFormat.DocTag | ValueFormat.RefTag =>
        val doc = undecided.parseTagged(ctx)

        doc.flatMapT {
          case Value.Struct.Full(v, _, _, _) =>
            v.lift("coll") match {
              case Some(c: SchemaCollectionCompanion) =>
                v.lift("name") match {
                  case Some(Value.Str(name)) =>
                    // TODO: Use a staged-only lookup here, as named docs
                    // always use the staged state.
                    c.idByNameStaged(ctx.scopeID, name).map {
                      case Some(id) => Right(Value.Doc(id))
                      case None =>
                        Left(InvalidValue(s"no such schema object '$name'"))
                    }
                  case Some(_) =>
                    Query.value(
                      Left(
                        InvalidValue(s"$tag field 'name' requires a string value")))
                  case None =>
                    Query.value(Left(InvalidValue(s"$tag field 'name' is required")))
                }
              case Some(c: CollectionCompanion) =>
                v.lift("id") match {
                  case Some(Value.Str(v)) =>
                    Query.value(v.toLongOption match {
                      case Some(id) => Right(Value.Doc(DocID(SubID(id), c.collID)))
                      case None =>
                        Left(InvalidValue(s"$tag field 'id' must be an integer"))
                    })
                  case Some(_) =>
                    Query.value(
                      Left(InvalidValue(s"$tag field 'id' requires a string value")))
                  case None =>
                    Query.value(Left(InvalidValue(s"$tag field 'id' is required")))
                }
              case Some(_) =>
                Query.value(
                  Left(InvalidValue(s"$tag field 'coll' requires a module value")))
              case None =>
                Query.value(Left(InvalidValue(s"$tag field 'coll' is required")))
            }
          // FIXME: Remove once migration is complete.
          case Value.Str(doc) =>
            val sections = doc.split(":", 3)

            sections match {
              case Array(coll, id) =>
                GlobalContext.lookupSingletonOrUDF(ctx, coll) flatMap {
                  case Some((c: SchemaCollectionCompanion)) =>
                    c.idByNameStaged(ctx.scopeID, id) map {
                      case Some(id) => Right(Value.Doc(id))
                      case None =>
                        Left(
                          InvalidValue(s"no such schema definition document '$id'"))
                    }
                  case Some((c: CollectionCompanion)) =>
                    Query.value(id.toLongOption match {
                      case Some(id) => Right(Value.Doc(DocID(SubID(id), c.collID)))
                      case None => Left(InvalidValue(s"invalid document id '$id'"))
                    })
                  case _ =>
                    Query.value(Left(InvalidValue(s"no such collection '$coll'")))
                }
              case _ =>
                Query.value(
                  Left(InvalidValue(s"doc id must be in the format <coll>:<id>")))
            }
          case _ => Query.value(Left(InvalidValue(s"$tag requires an object value")))
        }

      case _ => throw InvalidValue(s"invalid tag '$tag'")
    }
  }

  def asStrOrFail(tag: String): String = {
    this match {
      case UndecidedValue.Str(v) => v
      case _ => throw InvalidValue(s"$tag requires a string value")
    }
  }
  def asObjectOrFail(tag: String): SeqMap[String, UndecidedValue] = {
    this match {
      case UndecidedValue.Object(v) => v
      case _ => throw InvalidValue(s"$tag requires an object value")
    }
  }
}

object UndecidedValue {
  final case class Object(values: SeqMap[String, UndecidedValue])
      extends UndecidedValue
  final case class Array(values: Seq[UndecidedValue]) extends UndecidedValue
  final case class Int(value: Long) extends UndecidedValue
  final case class Float(value: Double) extends UndecidedValue
  final case class Str(value: String) extends UndecidedValue
  final case class Bool(value: Boolean) extends UndecidedValue
  final case class Null() extends UndecidedValue

  implicit object UndecidedValueDecoder extends JSON.SwitchDecoder[UndecidedValue] {
    def readInt(int: Long, stream: JSON.In) = UndecidedValue.Int(int)
    def readBoolean(bool: Boolean, stream: JSON.In) = UndecidedValue.Bool(bool)
    def readNull(stream: JSON.In) = UndecidedValue.Null()
    def readDouble(double: Double, stream: JSON.In) = UndecidedValue.Float(double)
    def readString(str: String, stream: JSON.In) = UndecidedValue.Str(str)

    def readArrayStart(stream: JSONParser): UndecidedValue = {
      val b = ArraySeq.newBuilder[UndecidedValue]
      while (!stream.skipArrayEnd) { b += decode(stream) }
      UndecidedValue.Array(b.result())
    }

    def readObjectStart(stream: JSONParser): UndecidedValue = {
      val b = SeqMap.newBuilder[String, UndecidedValue]
      while (!stream.skipObjectEnd) {
        b += (stream.read(JSONParser.ObjectFieldNameSwitch) -> decode(stream))
      }
      UndecidedValue.Object(b.result())
    }
  }
}
