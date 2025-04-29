package fauna.model.runtime.fql2.stdlib

import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.ToString._
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.Name
import scala.collection.immutable.{ ArraySeq, SeqMap }

object ObjectCompanion extends CompanionObject("Object") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Object => true
    case _               => false
  }

  private def extractKeys(s: Value.Struct.Full) =
    Value.Array(ArraySeq.from(s.fields.keys map { Value.Str(_) }))

  private def extractValues(s: Value.Struct.Full) =
    Value.Array(ArraySeq.from(s.fields.values))

  // Returns the (key, value) pairs of a struct as an array of
  // length 2 arrays. Compare with Javascript's Object.entries().
  private def extractEntries(s: Value.Struct.Full) =
    Value.Array(ArraySeq.from(s.fields map { case (k, v) =>
      Value.Array(ArraySeq(Value.Str(k), v))
    }))

  private def materializeStructOrDoc(
    ctx: FQLInterpCtx,
    obj: Value.Object,
    argName: String
  ): Query[Result[Value.Struct.Full]] = obj match {
    case s: Value.Struct => ReadBroker.materializeStruct(ctx, s).map(Result.Ok(_))
    case doc: Value.Doc =>
      ReadBroker.getAllFields(ctx, doc).flatMapT {
        case s: Value.Struct.Full => s.toQuery
        // No such doc.
        case Value.Null(cause) =>
          handleNullCause(ctx, argName, cause).toQuery

        case _ => sys.error("unreachable")
      }
    case _ =>
      QueryRuntimeFailure
        .InvalidArgument(
          argName,
          s"expected either an object or a document",
          ctx.stackTrace
        )
        .toQuery
  }

  private def handleNullCause(
    ctx: FQLInterpCtx,
    argName: String,
    cause: Value.Null.Cause
  ): QueryRuntimeFailure = {
    val err = FQLInterpreter.handleNullCause(cause, ctx.stackTrace)
    QueryRuntimeFailure
      .InvalidArgument(argName, uncapitalize(err.message), err.trace)
      .withHints(err.hints)
  }

  def uncapitalize(s: String): String =
    if (s.isEmpty) s else s"${s.head.toLower}${s.tail}"

  defStaticFunction("keys" -> tt.Array(tt.Str))("object" -> tt.AnyObject) {
    (ctx, obj) =>
      materializeStructOrDoc(ctx, obj, "object").mapT(extractKeys)
  }

  defStaticFunction("values" -> tt.Array(tt.A))("object" -> tt.WildObject(tt.A)) {
    (ctx, obj) =>
      materializeStructOrDoc(ctx, obj, "object").mapT(extractValues)
  }

  defStaticFunction("entries" -> tt.Array(tt.Tuple(tt.Str, tt.A)))(
    "object" -> tt.WildObject(tt.A)) { (ctx, obj) =>
    materializeStructOrDoc(ctx, obj, "object").mapT(extractEntries)
  }

  // This function is based on Javascript's Object.fromEntries, but it is stricter:
  // 1. The inner arrays must have length 2. JS ignores further elements.
  // 2. The first element of the inner array must be a string. JS coerces a
  //    non-string into a property name.
  defStaticFunction("fromEntries" -> tt.WildObject(tt.A))(
    "entries" -> tt.Array(tt.Tuple(tt.Str, tt.A))) { (ctx, fieldPairs) =>
    Result.guardM {
      val bld = SeqMap.newBuilder[String, Value]
      fieldPairs.elems.zipWithIndex foreach { case (value, i) =>
        // Validate each field pair looks like [string, *].
        val kv = value match {
          case Value.Array(kv) => kv
          case _ =>
            Result.fail(QueryRuntimeFailure.InvalidArgument(
              "entries",
              s"expected an array for each element of the outer array, but saw type ${value.dynamicType.displayString} at index $i",
              ctx.stackTrace
            ))
        }
        if (kv.size != 2) {
          Result.fail(QueryRuntimeFailure.InvalidArgument(
            "entries",
            s"expected inner array of length 2 but saw length ${kv.size} at index $i",
            ctx.stackTrace
          ))
        }
        val key = kv.head match {
          case Value.Str(k) => k
          case _ =>
            Result.fail(
              QueryRuntimeFailure.InvalidArgument(
                "entries",
                s"expected a string for the first element of each inner array, but saw type ${kv.head.dynamicType.displayString} at index $i",
                ctx.stackTrace
              ))
        }

        // Add it to the bag; the last occurrence of a key wins.
        bld += (key -> kv.last)
      }

      Value.Struct(bld.result()).toQuery
    }
  }

  defStaticFunction("assign" -> tt.WildStruct(tt.Union(tt.A, tt.B)))(
    "destination" -> tt.WildObject(tt.A),
    "source" -> tt.WildObject(tt.B)) { (ctx, dst, src) =>
    materializeStructOrDoc(ctx, dst, "destination").flatMapT { dst =>
      materializeStructOrDoc(ctx, src, "source").mapT { src =>
        Value.Struct(dst.fields ++ src.fields)
      }
    }
  }

  private def select0(
    ctx: FQLInterpCtx,
    curr: Value,
    ps: ArraySeq[Value]
  ): Query[Result[Option[Value]]] = {
    if (ps.isEmpty) {
      Result.Ok(Some(curr)).toQuery
    } else {
      ps.head match {
        case Value.Str(key) =>
          curr match {
            case s: Value.Struct =>
              StructPrototype.getField0(
                ctx,
                s,
                Name(key, ctx.stackTrace.currentStackFrame)) flatMap {
                case None =>
                  Result.Ok(None).toQuery
                case Some(next) =>
                  select0(ctx, next, ps.tail)
              }
            case _ =>
              Result.Ok(None).toQuery
          }
        case _ =>
          Result
            .Err(
              QueryRuntimeFailure
                .InvalidArgument(
                  "path",
                  "expected an array of strings",
                  ctx.stackTrace))
            .toQuery
      }
    }
  }

  defStaticFunction("select" -> tt.Any)(
    "object" -> tt.AnyStruct,
    "path" -> tt.Array(tt.Str)
  ) { case (ctx, obj, Value.Array(ps)) =>
    select0(ctx, obj, ps) mapT {
      _.getOrElse(
        Value.Null
          .noSuchElement(
            "no value in object found at path",
            ctx.stackTrace.currentStackFrame))
    }
  }

  defStaticFunction("hasPath" -> tt.Boolean)(
    "object" -> tt.AnyStruct,
    "path" -> tt.Array(tt.Str)) { case (ctx, obj, Value.Array(ps)) =>
    select0(ctx, obj, ps) mapT { v => Value.Boolean(v.isDefined) }
  }

  defStaticFunction("toString" -> tt.Str)("object" -> tt.Any) { (ctx, obj) =>
    obj.toDisplayString(ctx).map { str =>
      Value.Str(str).toResult
    }
  }
}
