package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.repo.query.Query
import fauna.repo.query.ReadCache.Fragment
import fauna.repo.values._
import fauna.repo.values.Value.Struct
import fql.ast.Name

object StructCompanion extends CompanionObject("Struct") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Struct => true
    case _               => false
  }
}

object StructPrototype
    extends Prototype(TypeTag.AnyStruct, isPersistable = true)
    with DynamicFieldTable[Value.Struct] {
  import FieldTable.R

  def dynHasField(
    ctx: FQLInterpCtx,
    self: Value.Struct,
    name: Name): Query[Boolean] = {
    getField0(ctx, self, name).map { _.isDefined }
  }

  def dynGet(ctx: FQLInterpCtx, self: Value.Struct, name: Name): Query[R[Value]] =
    getField0(ctx, self, name).map { v =>
      R.Val(v.getOrElse(Value.Null.missingField(self, name)))
    }

  defAccess(tt.Any)("key" -> tt.Str) { (ctx, self, key, span) =>
    dynGet(ctx, self, Name(key.value, span)).map { Result.Ok(_) }
  }

  def getField0(
    ctx: FQLInterpCtx,
    self: Value.Struct,
    name: Name): Query[Option[Value]] =
    self match {
      case Struct.Full(fields, srcHints, path, _) =>
        val fieldVal = fields.get(name.str).map {
          case sv: Struct.Full if ctx.performanceDiagnosticsEnabled =>
            sv.copy(
              srcHints = srcHints,
              path = path.appended(name.str),
              accessSpan = name.span)
          case v => v
        }

        ReadBroker
          .maybeEmitHint(ctx, path.appended(name.str), srcHints, name.span)
          .flatMap { _ =>
            Query.value(fieldVal)
          }
      case sp @ Struct.Partial(docID, prefix, path, fragment, _) =>
        fragment.project(List(name.str)) match {
          case None =>
            ReadBroker
              .maybeEmitHint(
                ctx,
                path.appended(name.str),
                sp.srcHints,
                name.span
              ) flatMap { _ =>
              ReadBroker
                /** Because we already emitted a hint in this scenario, we don't want to emit another
                  * hint when we materialize the struct.
                  */
                .materializeStruct(ctx.withPerformanceDiagnosticsDisabled, self)
                .map { struct =>
                  struct.fields.get(name.str)
                }
            }
          case Some(frag) =>
            frag match {
              case value: Fragment.Value => Query.value(Some(value.unwrap))
              case struct: Fragment.Struct =>
                val partial = Value.Struct.Partial(
                  docID,
                  prefix,
                  path.appended(name.str),
                  struct,
                  name.span
                )
                partial.srcHints = sp.srcHints
                Query.value(Some(partial))
            }
        }
    }
}
