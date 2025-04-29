package fauna.repo.values

import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo.schema.Path
import fauna.repo.values.{ Value, ValueType }
import fauna.storage.ir._
import fql.ast.{ Expr, Literal, Span }
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.util.control.NoStackTrace

final case class NonPersistableValue(path: Path, provided: ValueType) {
  def displayString =
    s"Value at `$path` has type ${provided.displayString}, which cannot be persisted."
}

trait IRValueConversion { self: Value.type =>

  def fromIR(v: IRValue, span: Span, validTime: Option[Timestamp] = None): Value = {
    import fauna.storage.ir._

    v match {
      case LongV(v)   => Value.Number(v)
      case DoubleV(v) => Value.Double(v)
      case TrueV      => Value.True
      case FalseV     => Value.False
      case NullV      => Value.Null(span)
      case StringV(v) => Value.Str(v)
      case BytesV(v)  => Value.Bytes(v.toByteArraySeq)
      // This can be a named document, in which case we insert None anyway, which
      // is handled when we lookup the name in ReadBroker.getRefFields.
      case DocIDV(v) => Value.Doc(v, None, validTime)
      case TimeV(v)  => Value.Time(v)
      case DateV(v)  => Value.Date(v)
      case UUIDV(v)  => Value.UUID(v)

      case TransactionTimeV(_) => Value.TransactionTime

      // FIXME: wrap the IRValue instead of this deep copy
      case ArrayV(vv) => Value.Array(vv.map(fromIR(_, span, validTime)).to(ArraySeq))

      // FIXME: wrap the IRValue instead of this deep copy
      case MapV(vv) =>
        Value.Struct(
          vv.map { case (k, v) => k -> fromIR(v, span, validTime) }.to(SeqMap))

      case QueryV(_, _) =>
        Value.Lambda(
          ArraySeq.empty,
          None,
          Expr.Lit(Literal.Null, Span.Null),
          Map.empty)
    }
  }

  private case class NonPersistable(path: Path, provided: ValueType)
      extends Exception
      with NoStackTrace

  def toIR(value: Value): Either[NonPersistableValue, IRValue] =
    try {
      Right(toIR0(value, Path.RootPrefix))
    } catch {
      case NonPersistable(path, prov) => Left(NonPersistableValue(path, prov))
    }

  def toIR(struct: Value.Struct.Full): Either[NonPersistableValue, MapV] =
    toIR(struct.asInstanceOf[Value])
      .asInstanceOf[Either[NonPersistableValue, MapV]]

  private def toIR0(value: Value, prefix: Path.Prefix): IRValue = {
    import fauna.storage.ir._

    value match {
      // FIXME: Int should probably map to an IntV
      case Value.Int(v)          => LongV(v)
      case Value.Long(v)         => LongV(v)
      case Value.Double(v)       => DoubleV(v)
      case Value.True            => TrueV
      case Value.False           => FalseV
      case Value.Str(v)          => StringV(v)
      case Value.Bytes(v)        => BytesV(v.unsafeByteBuf)
      case Value.Time(v)         => TimeV(v)
      case Value.TransactionTime => TransactionTimeV(false)
      case Value.Date(v)         => DateV(v)
      case Value.UUID(v)         => UUIDV(v)

      case Value.Null(_) => NullV

      case Value.Array(elems) =>
        val arr = elems.iterator.zipWithIndex.map { case (v, i) =>
          toIR0(v, prefix :+ i)
        }
        ArrayV(arr.toVector)

      // This branch can be hit for named documents, but only if the named document
      // was created from storage and was never used.
      case Value.Doc(id, None, _, _, _) => DocIDV(id)

      case Value.Struct.Full(fields, _, _, _) =>
        val f = fields.map { case (k, v) => k -> toIR0(v, prefix :+ k) }
        MapV(f.toList)

      case _: Value.Struct.Partial =>
        throw new IllegalArgumentException("Partials cannot be converted to IR.")

      // This branch will only be hit for named documents, which cannot be stored.
      // FIXME: It would be nice to have a specific error about named docs.
      case Value.Doc(_, Some(_), _, _, _) =>
        throw NonPersistable(prefix.toPath, value.dynamicType)

      case _: Value.ID | _: Value.SetCursor | _: Value.EventSource | _: Value.Set |
          _: Value.Func | _: Value.SingletonObject =>
        throw NonPersistable(prefix.toPath, value.dynamicType)
    }
  }
}
