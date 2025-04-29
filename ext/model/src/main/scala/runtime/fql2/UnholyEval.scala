package fauna.model.runtime.fql2

import fauna.ast.{
  Error => V4Error,
  EvalContext => V4Context,
  Literal => V4Literal,
  QueryParser => V4QueryParser
}
import fauna.atoms.{ APIVersion, ScopeID }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.serialization.MaterializedValue
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.Span
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.util.control.NoStackTrace

object UnholyEval {

  protected object NonConvertible extends Exception with NoStackTrace

  def ctxV4ToV10(ec: V4Context): Query[FQLInterpreter] =
    Query.snapshotTime.map { snapTS =>
      FQLInterpreter(
        auth = ec.auth,
        effectLimit = ec.effectLimit,
        userValidTime = ec.validTimeOverride,
        systemValidTime = Option.when(snapTS != ec.snapshotTime) { ec.snapshotTime },
        stackDepth = ec.stackDepth
      )
    }

  def ctxV10ToV4(intp: FQLInterpCtx) =
    Query.snapshotTime.map { snapTS =>
      V4Context(
        auth = intp.auth,
        snapshotTime = intp.systemValidTime.getOrElse(snapTS),
        validTimeOverride = intp.userValidTime,
        apiVers = APIVersion.V4,
        stackDepth = intp.stackDepth
      )
    }

  def evalV4(intp: FQLInterpCtx, v: Value.Struct): Query[Result[Value]] =
    (ctxV10ToV4(intp), dataV10ToV4(intp.scopeID, v)) par {
      case (_, None) =>
        QueryRuntimeFailure.V4InvalidArgument("expr", v, intp.stackTrace).toQuery
      case (ec, Some(in)) =>
        V4QueryParser.parse(intp.auth, in, APIVersion.V4).flatMap {
          case Left(errs) =>
            errV4ToV10(errs, APIVersion.V4, intp.stackTrace).toQuery
          case Right(expr) =>
            ec.eval(expr).map {
              case Left(errs) =>
                errV4ToV10(errs, APIVersion.V4, intp.stackTrace).toResult
              case Right(out) =>
                val span = intp.stackTrace.currentStackFrame
                dataV4ToV10(intp.scopeID, out, span, intp.userValidTime).toResult
            }
        }
    }

  def errV4ToV10(
    errs: List[V4Error],
    apiVersion: APIVersion,
    stackTrace: FQLInterpreter.StackTrace): QueryRuntimeFailure = {
    import fauna.ast._
    import fauna.model.RenderError

    def err0(errs: List[Error]): QueryRuntimeFailure = errs match {
      case List(FunctionCallError(_, _, cause)) => err0(cause)
      case List(TransactionAbort(msg, _)) =>
        QueryRuntimeFailure.Aborted(MaterializedValue(Value.Str(msg)), stackTrace)
      case _ =>
        val msgs = errs.map { RenderError.encodeErrors(_, apiVersion) }
        QueryRuntimeFailure.V4EvalError(msgs, stackTrace)
    }
    err0(errs)
  }

  def dataV4ToV10(
    scope: ScopeID,
    lit: V4Literal,
    span: Span,
    validTS: Option[Timestamp]): Value = {
    import fauna.ast._

    def go(lit: V4Literal): Value =
      lit match {
        // translate versions and refs to Docs
        case VersionL(v, _) if v.parentScopeID == scope =>
          Value.Doc(v.id, None, validTS)
        case RefL(s, id) if s == scope =>
          Value.Doc(id, None, validTS)
        // FIXME: out-of-scope documents aren't supported in v10 yet
        case _: VersionL | _: RefL => Value.Str("[db-external Document]")
        case _: UnresolvedRefL     => Value.Str("[unresolved Document]")

        // containers
        case ArrayL(es) =>
          Value.Array(es.view.map(go).to(ArraySeq))
        case ObjectL(fs) =>
          Value.Struct(
            fs.view
              .map { case (n, v) => n -> go(v) }
              .to(SeqMap))

        case LongL(v)   => Value.Number(v)
        case DoubleL(v) => Value.Number(v)
        case TrueL      => Value.True
        case FalseL     => Value.False
        case StringL(v) => Value.Str(v)
        case BytesL(v)  => Value.Bytes(v.toByteArraySeq)
        case TimeL(v)   => Value.Time(v)
        case DateL(v)   => Value.Date(v)
        case UUIDL(v)   => Value.UUID(v)
        case NullL      => Value.Null(span)

        case TransactionTimeL | TransactionTimeMicrosL => Value.TransactionTime

        case lit: ActionL => Value.Str(lit.toString)
        case ElemL(v, _)  => go(v)
        case CursorL(c)   => go(c.fold(identity, identity))
        case PageL(elems, _, before, after) =>
          val b = SeqMap.newBuilder[String, Value]
          before.foreach(v => b += ("before" -> go(v)))
          after.foreach(v => b += ("after" -> go(v)))
          b += ("data" -> go(ArrayL(elems)))
          Value.Struct(b.result())

        // Use IRValue for these, even though it's possibly bogus
        case _: SetL | _: LambdaL | _: EventL =>
          Value.fromIR(lit.irValue, span, validTS)
      }

    go(lit)
  }

  def dataV10ToV4(scope: ScopeID, v: Value): Query[Option[V4Literal]] = {
    import fauna.ast._

    def fieldsToObjectL(fs: SeqMap[String, Value]) =
      fs.view
        .map { case (n, v) => go(v) map { vv => (n, vv) } }
        .sequence
        .map { fs => ObjectL(fs: _*) }

    def go(v: Value): Query[Literal] = v match {
      case Value.Doc(id, _, _, None, _)   => Query.value(RefL(scope, id))
      case Value.Doc(_, _, _, Some(v), _) => Query.value(VersionL(v, false))

      case Value.Array(es) => es.view.map(go).sequence map { ls => ArrayL(ls: _*) }
      case Value.Struct.Full(fs, _, _, _) =>
        fieldsToObjectL(fs)
      case p: Value.Struct.Partial =>
        // see note on materializePartial
        @annotation.nowarn("cat=deprecation")
        val materializedQ = ReadBroker.materializePartial(scope, p)
        materializedQ.flatMap { case Value.Struct.Full(fs, _, _, _) =>
          fieldsToObjectL(fs)
        }

      case Value.ID(v)           => Query.value(LongL(v))
      case Value.Int(v)          => Query.value(LongL(v))
      case Value.Long(v)         => Query.value(LongL(v))
      case Value.Double(v)       => Query.value(DoubleL(v))
      case Value.True            => Query.value(TrueL)
      case Value.False           => Query.value(FalseL)
      case Value.Str(v)          => Query.value(StringL(v))
      case Value.Bytes(v)        => Query.value(BytesL(v.unsafeByteBuf))
      case Value.Time(v)         => Query.value(TimeL(v))
      case Value.TransactionTime => Query.value(TransactionTimeL)
      case Value.Date(v)         => Query.value(DateL(v))
      case Value.UUID(v)         => Query.value(UUIDL(v))
      case _: Value.Null         => Query.value(NullL)

      case _ => Query.fail(NonConvertible)
    }

    go(v).map(Some(_)).recover { case NonConvertible => None }
  }

}
