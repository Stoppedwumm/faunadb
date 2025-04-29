package fauna.model.runtime.fql2

import fauna.lang.syntax._
import fauna.model.runtime.fql2.serialization.MaterializedValue
import fauna.model.SchemaNames
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.util.Base64

object ToString {
  import Result._

  implicit class ToStringOps(v: Value) {
    def toDebugString(ctx: FQLInterpCtx)(
      implicit nested: Boolean = false): Query[String] = v match {
      case Value.ID(v)      => Query.value(s"ID(\"${v.toString}\")")
      case Value.Int(iv)    => Query.value(iv.toString)
      case Value.Long(lv)   => Query.value(lv.toString)
      case Value.Double(dv) => Query.value(dv.toString)
      case Value.True       => Query.value(true.toString)
      case Value.False      => Query.value(false.toString)
      case Value.Str(strv)  => Query.value(MaterializedValue.escape(strv))
      case v: Value.Bytes   => Query.value(s"Bytes(\"${v.toBase64.toString}\")")
      case Value.Time(time) => Query.value(s"Time(\"${time.toString}\")")
      case Value.Date(date) => Query.value(s"Date(\"${date.toString}\")")
      case Value.UUID(uuid) => Query.value(s"UUID(\"${uuid.toString}\")")
      case Value.Null(_)    => Query.value("null")

      case Value.Array(elems) =>
        elems.map(_.toDebugString(ctx)).sequence.map(_.mkString("[", ", ", "]"))

      case Value.Struct.Full(fields, _, _, _) =>
        if (fields.isEmpty) {
          Query.value("{}")
        } else {
          fields
            .map { case (k, v) =>
              v.toDebugString(ctx).map { vstr =>
                if (MaterializedValue.isIdent(k)) {
                  s"${k}: $vstr"
                } else {
                  s"${MaterializedValue.escape(k)}: $vstr"
                }
              }
            }
            .sequence
            .map(_.mkString("{ ", ", ", " }"))
        }

      case v: Value.Struct.Partial =>
        ReadBroker.materializeStruct(ctx, v).flatMap(_.toDebugString(ctx))

      case Value.SingletonObject((name, Some(parent))) =>
        parent.toDebugString(ctx).map(_ + "." + name)
      case Value.SingletonObject((name, None)) => Query.value(name)

      case Value.TransactionTime => Query.value("TransactionTime()")

      case c: Value.SetCursor =>
        c.toDisplayString(ctx).map(MaterializedValue.escape(_))

      // we don't want the user to blow up their query by calling toString on a set,
      // so we just put a placeholder.
      case _: Value.Set         => Query.value("[set]")
      case _: Value.EventSource => Query.value("[event source]")

      case v: Value.Doc =>
        if (nested) {
          // Prevent ref cycle kaboom by rendering the ref only.
          val nameQ = SchemaNames.lookupCachedName(ctx.scopeID, v.id.collID.toDocID)
          val popQ = ReadBroker.populateDoc(ctx, v)
          (nameQ, popQ) par { case (collNameOpt, res) =>
            val collName = collNameOpt.getOrElse("<deleted collection>")
            res match {
              case FieldTable.R.Val(Value.Doc(_, Some(name), _, _, _)) =>
                Query.value(s"[doc $name in $collName]")
              case FieldTable.R.Val(Value.Doc(id, None, _, _, _)) =>
                Query.value(s"[doc ${id.subID.toLong} in $collName]")
              case FieldTable.R.Null(cause) =>
                Value.Null(cause).toDebugString(ctx)
              case FieldTable.R.Error(e) =>
                Query.fail(new IllegalStateException(
                  s"doc (scope=${ctx.scopeID}, id=${v.id}) caused an error in ReadBroker.populateDoc: $e"))
            }
          }
        } else {
          ReadBroker
            .getAllFields(ctx, v)
            .flatMap {
              case Ok(value) => value.toDebugString(ctx)(nested = true)
              case Err(err)  => Query.value(s"[error: ${err.failureMessage}]")
            }
        }

      case Value.Lambda(_, _, _, _) => Query.value("[function <lambda>]")
      case fn: Value.NativeFunc     => Query.value(s"[function ${fn.name}()]")
    }

    def toDisplayString(ctx: FQLInterpCtx): Query[String] = v match {
      case Value.ID(v)      => Query.value(v.toString)
      case Value.Int(iv)    => Query.value(iv.toString)
      case Value.Long(lv)   => Query.value(lv.toString)
      case Value.Double(dv) =>
        // 2.0.toString() should render to "2", not "2.0"
        if (dv % 1 == 0) {
          Query.value(String.format("%.0f", dv))
        } else {
          Query.value(dv.toString)
        }
      case Value.True         => Query.value(true.toString)
      case Value.False        => Query.value(false.toString)
      case Value.Str(strv)    => Query.value(strv)
      case Value.Bytes(bytes) => Query.value(Base64.encodeStandard(bytes.toArray))
      case Value.Time(time)   => Query.value(time.toString)
      case Value.Date(date)   => Query.value(date.toString)
      case Value.UUID(uuid)   => Query.value(uuid.toString)
      case Value.Null(_)      => Query.value("null")

      case c: Value.SetCursor =>
        Query.value(Value.SetCursor.toBase64(c, None).toString)

      // TODO: we could support rendering the transaction time as a string,
      // but it would require changing the representation of FQL strings in
      // order to support transaction time placeholders. See further note on
      // Value.TransactionTime
      case Value.TransactionTime => Query.value("[transaction time]")

      case _ => v.toDebugString(ctx)
    }
  }
}
