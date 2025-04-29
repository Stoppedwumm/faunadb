package fauna.model.runtime.fql2

import fauna.lang.syntax._
import fauna.repo.query.Query
import fauna.repo.values.Value
import java.lang.{ Integer => JInteger }

object ValueCmp {
  private def listCmp[A](a: Iterable[A], b: Iterable[A])(
    cmp: (A, A) => Query[Int]): Query[Int] = {
    if (a.isEmpty) {
      Query.value(-1)
    } else if (b.isEmpty) {
      Query.value(1)
    } else {
      val v = a.head
      val v2 = b.head

      (cmp(v, v2)).flatMap { c =>
        if (c != 0) {
          Query.value(c)
        } else {
          listCmp(a.tail, b.tail)(cmp)
        }
      }
    }
  }

  private def mapCmp(
    ctx: FQLInterpCtx,
    a: Map[String, Value],
    b: Map[String, Value]): Query[Int] = {
    listCmp(a.toSeq.sortBy(_._1), b.toSeq.sortBy(_._1)) { case ((k1, a), (k2, b)) =>
      k1 compare k2 match {
        case n if n != 0 => Query.value(n)
        case _           => a.totalCmp(ctx, b)
      }
    }
  }

  implicit class ValueCmpOps(a: Value) {
    def cmp(ctx: FQLInterpCtx, b: Value): Query[Option[Int]] =
      (a, b) match {
        case _ if a eq b => Query.value(Some(0))

        case (p: Value.Struct.Partial, _: Value.Struct) =>
          ReadBroker.materializeValue(ctx, p).flatMap { a =>
            a.cmp(ctx, b)
          }
        case (_: Value.Struct, p: Value.Struct.Partial) =>
          ReadBroker.materializeValue(ctx, p).flatMap { b =>
            a.cmp(ctx, b)
          }

        case (Value.Struct.Full(v1, _, _, _), Value.Struct.Full(v2, _, _, _)) =>
          mapCmp(ctx, v1, v2).map(Some(_))

        case (Value.Array(v1), Value.Array(v2)) =>
          listCmp(v1, v2) { case (a, b) => a.totalCmp(ctx, b) }.map(Some(_))

        case (_: Value.Null, _: Value.Null) => Query.value(Some(0))

        case (Value.ID(v), Value.Str(v2)) =>
          Query.value(v2.toLongOption.map(v compare _))
        case (Value.Str(v), Value.ID(v2)) =>
          Query.value(v.toLongOption.map(_ compare v2))
        case (Value.ID(v), v2: Value.Number) =>
          Query.value(Some(Value.Number(v).compareNumbers(v2)))
        case (v: Value.Number, Value.ID(v2)) =>
          Query.value(Some(v.compareNumbers(Value.Number(v2))))

        case (Value.ID(v), Value.ID(v2)) => Query.value(Some(v compare v2))
        case (v1: Value.Number, v2: Value.Number) =>
          Query.value(Some(v1.compareNumbers(v2)))
        case (Value.Str(v), Value.Str(v2))         => Query.value(Some(v compare v2))
        case (Value.Boolean(v), Value.Boolean(v2)) => Query.value(Some(v compare v2))
        case (Value.Time(v), Value.Time(v2))       => Query.value(Some(v compare v2))
        case (Value.Date(v), Value.Date(v2)) => Query.value(Some(v compareTo v2))
        case (Value.Bytes(v1), Value.Bytes(v2)) =>
          val buf1 = v1.unsafeByteBuf
          val buf2 = v2.unsafeByteBuf
          Query.value(Some(buf1.compareTo(buf2)))
        case (Value.UUID(v), Value.UUID(v2)) => Query.value(Some(v compareTo v2))

        case (Value.Doc(id1, name1, _, _, _), Value.Doc(id2, name2, _, _, _)) =>
          Query.value(Some(id1 compare id2 match {
            case 0 =>
              (name1, name2) match {
                case (Some(n1), Some(n2)) => n1 compare n2
                case _ => id1.subID.toLong compare id2.subID.toLong
              }
            case n => n
          }))

        case (a: Value.SingletonObject, b: Value.SingletonObject) =>
          (a.parent, b.parent) match {
            case (Some(a0), Some(b0)) =>
              a0.cmp(ctx, b0).map {
                case Some(0) => Some(a.name compare b.name)
                case c       => c
              }
            case (None, None)    => Query.value(Some(a.name compare b.name))
            case (Some(_), None) => Query.value(Some(1))
            case (None, Some(_)) => Query.value(Some(-1))
          }

        case (a: Value.Set, b: Value.Set) =>
          Query.value(Some(a.hashCode compare b.hashCode))

        case (a: Value.SetCursor, b: Value.SetCursor) =>
          Query.value(Some(a.hashCode compare b.hashCode))

        case (a: Value.EventSource, b: Value.EventSource) =>
          Query.value(Some(a.hashCode compare b.hashCode))

        case (a: Value.Lambda, b: Value.Lambda) =>
          // Technically we don't need Query here but I'd rather not rewrite
          // listCmp just for this.
          listCmp(a.params, b.params) {
            case (Some(a), Some(b)) => Query.value(a compare b)
            case (None, Some(_))    => Query.value(-1)
            case (Some(_), None)    => Query.value(1)
            case _                  => Query.value(0)
          }
            .map {
              case 0 => Some(a.hashCode compare b.hashCode)
              case n => Some(n)
            }

        case (a: Value.NativeFunc, b: Value.NativeFunc) =>
          a.callee.totalCmp(ctx, b.callee).map {
            case 0 => Some(a.name compare b.name)
            case n => Some(n)
          }

        case _ => Query.value(None)
      }

    def totalCmp(ctx: FQLInterpCtx, b: Value): Query[Int] = {
      this.cmp(ctx, b).map { res =>
        res.getOrElse(
          JInteger.compare(a.totalOrderingPosition, b.totalOrderingPosition))
      }
    }

    def isEqual(ctx: FQLInterpCtx, b: Value): Query[Boolean] =
      (a, b) match {
        case _ if a eq b => Query.value(true)

        case (p: Value.Struct.Partial, _) =>
          ReadBroker.materializeValue(ctx, p).flatMap { a => a.isEqual(ctx, b) }
        case (_, p: Value.Struct.Partial) =>
          ReadBroker.materializeValue(ctx, p).flatMap { b => a.isEqual(ctx, b) }

        case (Value.Array(v), Value.Array(v2)) =>
          if (v.length == v2.length) {
            v.zip(v2)
              .map { case (a, b) => a.isEqual(ctx, b) }
              .accumulate(true) { case (acc, v) => acc && v }
          } else {
            Query.False
          }
        case (Value.Array(_), _) => Query.False

        case (Value.Struct.Full(v, _, _, _), Value.Struct.Full(v2, _, _, _)) =>
          if (v.size == v2.size) {
            v.map { case (k, a) =>
              v2.get(k) match {
                case Some(b) => a.isEqual(ctx, b)
                case None    => Query.value(false)
              }
            }.accumulate(true) { case (acc, v) => acc && v }
          } else {
            Query.False
          }
        case (Value.Struct.Full(_, _, _, _), _) => Query.False

        case (d: Value.Doc, _: Value.Null) => ReadBroker.exists(ctx, d).map(!_)
        case (_: Value.Null, d: Value.Doc) => ReadBroker.exists(ctx, d).map(!_)

        case (Value.TransactionTime, Value.TransactionTime) => Query.True

        case _ =>
          a.cmp(ctx, b).map {
            case Some(v) => v == 0
            case None =>
              assert(a.getClass != b.getClass)
              false
          }
      }
  }
}
