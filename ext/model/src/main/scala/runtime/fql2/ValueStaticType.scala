package fauna.model.runtime.fql2

import fauna.atoms.DocID
import fauna.lang.syntax._
import fauna.model.schema.CollectionConfig
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.Literal
import fql.ast.Span
import fql.typer.Constraint
import fql.typer.Type
import scala.collection.immutable.SeqMap

object ValueStaticType {
  val sp = Span.Null

  private def findType(ctx: FQLInterpCtx, v: Value): Query[Constraint.Value] = {
    // shameless copy of MaterializedValue.gather
    def gather(v: Value): Map[DocID, () => Query[Type]] =
      v match {
        case Value.Array(vs) => vs.flatMap(gather(_)).toMap
        case Value.Struct.Full(vs, _, _, _) =>
          vs.flatMap { case (_, v) => gather(v) }.toMap

        case d: Value.Doc =>
          Map(d.id -> (() => {
            // NOTE: Legacy indexes cannot come from query arguments, so they aren't
            // handled here.
            CollectionConfig(ctx.scopeID, d.id.collID) map {
              _.fold(Type.AnyNullDoc: Type) { _.companionObject.docType.staticType }
            }
          }))

        case _ => Map.empty
      }

    val docQs = gather(v)
    val docQ = docQs.map { case (k, q) => q().map(v => k -> v) }.sequence

    docQ.map { docs =>
      findType0(v, docs.toMap)
    }
  }

  private def findType0(
    v: Value,
    docs: Map[DocID, Constraint.Value]): Constraint.Value = {
    v match {
      case Value.ID(_)   => Type.ID
      case Value.Null(_) => Type.Null
      case Value.Int(v)  => Constraint.Lit(Literal.Int(v), sp)
      case Value.Long(v) => Constraint.Lit(Literal.Int(v), sp)
      case Value.Double(v) =>
        if (v.isNaN || v.isInfinity) Type.Double(sp)
        else Constraint.Lit(Literal.Float(v), sp)
      case Value.Str(v)     => Constraint.Lit(Literal.Str(v), sp)
      case Value.Boolean(_) => Type.Boolean

      case Value.Time(_)         => Type.Time
      case Value.TransactionTime => Type.TransactionTime
      case Value.Date(_)         => Type.Date
      case Value.Bytes(_)        => Type.Bytes
      case Value.UUID(_)         => Type.UUID

      case Value.Array(elems) =>
        Constraint.Tup(
          elems.map { elem =>
            Constraint.Lazy(() => findType0(elem, docs))
          },
          sp)
      case Value.Struct.Full(fields, _, _, _) =>
        Constraint.Rec(
          SeqMap.from(fields.map { case (k, v) =>
            k -> Constraint.Lazy(() => findType0(v, docs))
          }),
          sp)

      case d: Value.Doc => docs(d.id)
      // Not strictly correct, but unless we read the whole set, we can't get more
      // accurate.
      case _: Value.Set         => Type.Set(Type.Any(sp), sp)
      case _: Value.EventSource => Type.EventSource(Type.Any(sp), sp)
      case _: Value.SetCursor   => Type.SetCursor

      case v: Value.SingletonObject => v.selfType.staticType

      // NOTE: This are never a hit in normal operation, as you cannot create a
      // NativeFunc or Partial using the wire protocol.
      case _: Value.NativeFunc | _: Value.Lambda | _: Value.Struct.Partial =>
        sys.error("unreacheable")
    }
  }

  implicit class StaticTypeOps(value: Value) {
    def staticType(ctx: FQLInterpCtx): Query[Constraint.Value] = findType(ctx, value)
  }
}
