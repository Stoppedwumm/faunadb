package fauna.model.runtime.fql2

import fauna.atoms.ScopeID
import fauna.auth.Auth
import fauna.lang.{ CPUTiming, Timestamp }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model.{ Cache, Database, Key, RuntimeEnv }
import fauna.model.runtime.fql2.serialization._
import fauna.model.runtime.fql2.stdlib.StringPrototype
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.fql2.ValueStaticType._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.{ Expr, Literal, Name, Span, Src, TypeExpr }
import fql.ast.display._
import fql.ast.redact._
import fql.env.TypeEnv
import fql.error.{ Diagnostic, Hint, Warning }
import fql.parser.Parser
import fql.typer.{ TypeScheme, Typer }
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentLinkedQueue }
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** This contains any check diagnostics and runtime diagnostics. The check
  * diagnostics will already be sorted by span, and the runtime diagnostics
  * are in order of execution.
  */
final case class InfoWarns(check: Seq[Diagnostic], runtime: Seq[Diagnostic]) {
  def filterLogs(): InfoWarns =
    copy(
      check = check.filter(_.span.src.isQuery),
      runtime = runtime.filter(_.span.src.isQuery)
    )

  def allSrcs: Set[Src] =
    check.toSet[Diagnostic].flatMap { _.allSrcs } ++
      runtime.toSet[Diagnostic].flatMap { _.allSrcs }
}

object InfoWarns {
  def empty = InfoWarns(Seq.empty, Seq.empty)
}

final case class RedactedQuery(shape: String, redacted: String)

object RedactedQuery {
  def apply(expr: Expr): RedactedQuery = {
    def display(expr: Expr): String = {
      expr match {
        case block: Expr.Block => block.displayBody
        case _                 => expr.display
      }
    }

    RedactedQuery(
      shape = display(expr.redactStrict),
      redacted = display(expr.redact))
  }
}

final case class FQL2Output(
  result: Result[(MaterializedValue, Option[TypeExpr])],
  infoWarns: InfoWarns,
  redacted: Option[RedactedQuery])

object FQLInterpreter {

  sealed trait TypeMode
  object TypeMode {

    /** Disable type checking */
    case object Disabled extends TypeMode

    /** Check correctness only, but do not go as far as inferring a final type. */
    case object CheckOnly extends TypeMode

    /** Check correctness and generate a valid return type. */
    case object InferType extends TypeMode
  }

  private val DisabledTypeResult = TypeExpr.Any(Span.Null)

  val ResultMaterializationTimeMetric =
    "Query.FQL2.Result.Materialization.Time"

  // Static typing should take no more time than this.
  private val TypingTimeout = 5.seconds

  val TypingTimeMetric = "Query.FQL2.Typing.Time"
  val TypingCPUTimeMetric = "Query.FQL2.Typing.CPUTime"
  val EnvTypingTimeMetric = "Query.FQL2.EnvTyping.Time"
  val EnvTypingCPUTimeMetric = "Query.FQL2.EnvTyping.CPUTime"

  object StackTrace {
    val empty = StackTrace(Nil)
  }
  case class StackTrace(trace: Seq[Span]) extends AnyVal {
    def currentStackFrame: Span = if (trace.isEmpty) {
      getLogger().warn(s"Unexpected empty stack trace")
      Span.Null
    } else {
      trace.head
    }

    def +:(span: Span): StackTrace =
      StackTrace(span +: trace)

    def ++:(otherTrace: StackTrace): StackTrace =
      StackTrace(otherTrace.trace ++: trace)

    def filter(pred: Span => Boolean): StackTrace =
      StackTrace(trace.filter(pred))

    def depth: Int = trace.length
  }

  type VarCtx = Map[String, Value]

  def isEnvTypechecked(scope: ScopeID): Query[Boolean] =
    Database.isTypechecked(scope)

  /** Parses, evals, and then materializes a query into a value appropriate for
    * returning to the client using the given FQLInterpCtx.
    */
  def evalQuery(
    intp: FQLInterpCtx,
    query: FQL2Query,
    arguments: VarCtx,
    typecheck: Option[Boolean]): Query[FQL2Output] = {

    val parseQ: Query[(
      Result[(Value, Option[TypeExpr])],
      InfoWarns,
      Option[RedactedQuery])] =
      query.parse(intp).flatMap {
        case Result.Ok((vars, expr)) =>
          val typeModeQ = typecheck
            .fold(FQLInterpreter.isEnvTypechecked(intp.scopeID))(Query.value(_))
            .map(if (_) TypeMode.InferType else TypeMode.Disabled)

          val redacted = RedactedQuery(expr)

          typeModeQ.flatMap { typeMode =>
            intp.evalWithTypecheck(
              expr,
              arguments ++ vars,
              typeMode = typeMode) flatMap {
              case Result.Ok((value, tpe)) =>
                intp.runPostEvalHooks() flatMap { res =>
                  intp.infoWarns.map { infoWarns =>
                    res match {
                      case Result.Ok(_) =>
                        val tpeOpt = Option.when(typeMode != TypeMode.Disabled)(tpe)
                        (Result.Ok((value, tpeOpt)), infoWarns, Some(redacted))
                      case res @ Result.Err(_) =>
                        (res, infoWarns, Some(redacted))
                    }
                  }
                }
              case res @ Result.Err(_) =>
                intp.infoWarns.map { infoWarns =>
                  (res, infoWarns, Some(redacted))
                }
            }
          }
        case Result.Err(e) => Query.value((Result.Err(e), InfoWarns.empty, None))
      }

    parseQ flatMap {
      case (Result.Ok((value, tpe)), _, redacted) =>
        // Return a materialized value where all doc refs and sets have been
        // converted to concrete objects and pages, respectively.
        for {
          res <- Query.timing(ResultMaterializationTimeMetric) {
            FQL2ValueMaterializer.materialize(intp, value) mapT { (_, tpe) }
          }
          infoWarns <- intp.infoWarns
        } yield {
          FQL2Output(res, infoWarns, redacted)
        }
      case (res @ Result.Err(_), infoWarns, redacted) =>
        Query.value(FQL2Output(res, infoWarns, redacted))
    }
  }

  /** Evaluates the source code of the body of a lambda. The result
    * may be executed by supplying it with arguments.
    */
  def prepareUserFunc(
    src: Src,
    body: String,
    allowShortLambda: Boolean = true): Query[Result[Value.Lambda]] = {
    val lambda: Result[Expr.Lambda] = Parser.lambdaExpr(body, src)
    Query.value(lambda).flatMapT {
      case l: Expr.ShortLambda if !allowShortLambda =>
        Result
          .Err(QueryCheckFailure.InvalidShortLambda(l.span))
          .toQuery

      case l =>
        Value
          .Lambda(
            l.params.map(_.map(_.str)).to(ArraySeq),
            l.vari.mapT(_.str),
            l.body,
            Map.empty)
          .toQuery
    }
  }

  /** Parsed and returns a v10 UDF pre-checked type signature. */
  def prepareLambdaSig(sig: String): Result[TypeScheme] =
    Parser.typeSchemeExpr(sig, Src.Null).map(Typer.typeTSchemeUnchecked)

  def handleNullCause(
    cause: Value.Null.Cause,
    stackTrace: FQLInterpreter.StackTrace
  ): QueryRuntimeFailure =
    cause match {
      case Value.Null.Cause.InvalidNumber(_, nullSpan) =>
        QueryRuntimeFailure.NullValue(
          s"Null value, due to invalid number",
          stackTrace,
          nullSpan)

      case Value.Null.Cause.MissingField(_, field) =>
        QueryRuntimeFailure.NullValue(
          s"Null value, due to missing field `${field.str}`",
          stackTrace,
          field.span)
      case Value.Null.Cause.DocNotFound(doc, collName, nullSpan) =>
        val err = QueryRuntimeFailure.DocumentNotFound(collName, doc, stackTrace)
        if (!stackTrace.currentStackFrame.overlaps(nullSpan)) {
          err.withHint(Hint("Null value created here", nullSpan))
        } else {
          err
        }

      case Value.Null.Cause.DocDeleted(doc, collName, nullSpan) =>
        val err = QueryRuntimeFailure.DocumentNotFound(collName, doc, stackTrace)
        if (!stackTrace.currentStackFrame.overlaps(nullSpan)) {
          err.withHint(Hint("Null value created here", nullSpan))
        } else {
          err
        }

      case Value.Null.Cause.CollectionDeleted(doc, nullSpan) =>
        val err = QueryRuntimeFailure.CollectionDeleted(doc, stackTrace)
        if (!stackTrace.currentStackFrame.overlaps(nullSpan)) {
          err.withHint(Hint("Null value created here", nullSpan))
        } else {
          err
        }

      case Value.Null.Cause.NoSuchElement(reason, nullSpan) =>
        QueryRuntimeFailure.NullValue(
          s"Null value, due to $reason",
          stackTrace,
          nullSpan)

      case Value.Null.Cause.ReadPermissionDenied(_, nullSpan) =>
        QueryRuntimeFailure.NullValue(
          s"Null value, due to permission denied",
          stackTrace,
          nullSpan)

      case Value.Null.Cause.Lit(nullSpan) =>
        QueryRuntimeFailure.NullValue("Null literal value", stackTrace, nullSpan)
    }

}

sealed trait FQLInterpCtx {
  import FQLInterpreter._

  def auth: Auth
  def env: RuntimeEnv
  def infoWarns: Query[InfoWarns]

  def account = auth.database.account

  def effectLimit: Effect.Limit
  def withEffectLimit(limit: Effect.Limit): FQLInterpCtx

  // Whether interpretation is allowed to evaluate computed fields and
  // reference a doc's ts field (which isn't always known).
  // This flag relates specifically to special indexing situations and
  // probably shouldn't be used for anything else.
  def allowFieldComputation: Boolean
  def withAllowFieldComputation(allowed: Boolean): FQLInterpCtx

  def stackDepth: Int

  def stackTrace: StackTrace

  def performanceDiagnosticsEnabled: Boolean
  def withPerformanceDiagnosticsDisabled: FQLInterpCtx
  def emitDiagnostic(diag: Diagnostic): Query[Unit]

  def scopeID = auth.scopeID

  /** The user defined valid time for reads. Users may override valid time via the FQL
    * `at(..)` expression.
    */
  def userValidTime: Option[Timestamp]

  /** The system defined valid time for reads. Used by `Set.paginate(..)` to provide
    * stable cursors where the valid time of the `paginate` is set to the transaction
    * time of the query that produced the given cursor.
    */
  def systemValidTime: Option[Timestamp]

  /** The effective read valid time: user, or else, system defined. */
  def readValidTime: Timestamp

  /** Stores the given ts as the userValidTime.
    * - If given now, it will store None.
    * - If given something else, it will store Some(_).
    */
  def atUserValidTime(ts: Timestamp): Query[FQLInterpCtx]

  /** Similar to atUserValidTime, but will do one of three things:
    * - If given None, it will store None.
    * - If given Some(now), it will store None.
    * - If given Some(_), it will store Some(_).
    */
  def withUserValidTime(ts: Option[Timestamp]): Query[FQLInterpCtx]

  def withStackFrame(span: Span): FQLInterpreter

  def prependStackTrace(otherTrace: StackTrace): FQLInterpreter

  def withClearTrace: FQLInterpreter

  @inline final def atSystemValidTime(ts: Timestamp): Query[FQLInterpCtx] =
    withSystemValidTime(Some(ts))

  /** Equivalent to atUserValidTime, but will store the ts at the systemValidTime. */
  def withSystemValidTime(ts: Option[Timestamp]): Query[FQLInterpCtx]

  def withRole(role: Key.Role): Query[FQLInterpCtx]

  def addPostEvalHook(hook: PostEvalHook): Unit

  def runPostEvalHooks(): Query[Result[Unit]]

  def evalWithTypecheck(
    expr: Expr,
    closure: VarCtx,
    typeMode: TypeMode): Query[Result[(Value, TypeExpr)]]

  def evalApply(
    fn: Value.Func,
    args: IndexedSeq[Value]
  ): Query[Result[Value]]

  def evalLambda(
    fn: Value.Lambda,
    role: Option[Key.Role],
    name: String,
    args: IndexedSeq[Value]
  ): Query[Result[Value]]
}

class InterpreterState(
  var postEvalHooks: ConcurrentHashMap[(Class[_], AnyRef), PostEvalHook] =
    new ConcurrentHashMap[(Class[_], AnyRef), PostEvalHook],
  val checkDiagnostics: ConcurrentLinkedQueue[Diagnostic] =
    new ConcurrentLinkedQueue[Diagnostic]
)

/** An interpreter which can evaluate FQL expressions in a specific database's
  * environment.
  *
  * During eval, the variable environment is split into two maps, `globalCtx`
  * and `vctx`. The global context is constructed at the beginning of expression
  * evaluation and does not change throughout, whereas the var context contains
  * variables which are visible to a subexpression based on its lexical scope.
  * This distinction becomes important when serializing values which close over
  * expressions (lambdas and set cursors) where we do not want to serialize
  * parts of the global context.
  *
  * TODO: implement the at() form. Eliminate OCC checks for reads at a set
  * valid time.
  *
  * FIXME: this class must be made immutable since it interacts with the Query monad
  * which may decide to re-run any sub-query upon read/write conflicits.
  */
final case class FQLInterpreter(
  auth: Auth,
  effectLimit: Effect.Limit = Effect.Limit(Effect.Write, ""),
  env: RuntimeEnv = RuntimeEnv.Default,
  allowFieldComputation: Boolean = true,
  userValidTime: Option[Timestamp] = None,
  systemValidTime: Option[Timestamp] = None,
  stackTrace: FQLInterpreter.StackTrace = FQLInterpreter.StackTrace.empty,
  stackDepth: Int = 0,
  state: InterpreterState = new InterpreterState,
  performanceDiagnosticsEnabled: Boolean = false
) extends FQLInterpCtx
    with ExceptionLogging {
  import FQLInterpreter._
  import FieldTable.R
  import Hint.HintType

  def withEffectLimit(limit: Effect.Limit): FQLInterpCtx = copy(effectLimit = limit)

  def withAllowFieldComputation(allowed: Boolean): FQLInterpCtx =
    copy(allowFieldComputation = allowed)

  def infoWarns: Query[InfoWarns] =
    Query.state.map { qs =>
      InfoWarns(
        state.checkDiagnostics.asScala.toSeq.sortBy { _.span.start },
        qs.diagnostics)
    }

  def emitCheckDiagnostic(diag: Diagnostic): Unit = state.checkDiagnostics.add(diag)

  def withPerformanceDiagnosticsDisabled: FQLInterpCtx = if (
    performanceDiagnosticsEnabled
  ) {
    copy(performanceDiagnosticsEnabled = false)
  } else {
    this
  }

  def emitDiagnostic(diag: Diagnostic): Query[Unit] =
    diag match {
      case h: Hint
          if h.hintType == HintType.Performance && !performanceDiagnosticsEnabled =>
        Query.unit
      case _ => Query.updateState { _.addDiagnostic(diag) }.join
    }

  def readValidTime: Timestamp =
    userValidTime.orElse(systemValidTime).getOrElse(Timestamp.MaxMicros)

  def atUserValidTime(ts: Timestamp): Query[FQLInterpreter] =
    withUserValidTime(Some(ts))

  def withUserValidTime(ts: Option[Timestamp]): Query[FQLInterpreter] =
    setValidTime(ts, userValidTime) { ts => copy(userValidTime = ts) }

  // TODO: bound the valid time to the MVTOffset.
  def withSystemValidTime(ts: Option[Timestamp]): Query[FQLInterpreter] =
    setValidTime(ts, systemValidTime) { ts => copy(systemValidTime = ts) }

  private def setValidTime(ts: Option[Timestamp], get: => Option[Timestamp])(
    set: Option[Timestamp] => FQLInterpreter): Query[FQLInterpreter] =
    Query.snapshotTime map { snap =>
      val newValid = ts.filter { _ != snap }
      if (newValid == get) this else set(newValid)
    }

  def withStackFrame(span: Span): FQLInterpreter =
    copy(stackTrace = span +: stackTrace, stackDepth = stackDepth + 1)

  def prependStackTrace(otherStackTrace: StackTrace): FQLInterpreter =
    copy(
      stackTrace = otherStackTrace ++: stackTrace,
      stackDepth = stackDepth + otherStackTrace.depth)

  def withClearTrace: FQLInterpreter =
    copy(stackTrace = StackTrace(Nil), stackDepth = 0)

  def withRole(role: Key.Role): Query[FQLInterpreter] =
    Auth.changeRole(auth, role) map { newAuth => copy(auth = newAuth) }

  def addPostEvalHook(hook: PostEvalHook): Unit =
    state.postEvalHooks.compute(
      (hook.getClass, hook.key),
      {
        case (_, null) => hook
        case (_, prev) => prev.merge(hook.asInstanceOf[prev.Self])
      }
    )

  def runPostEvalHooks(): Query[Result[Unit]] =
    state.postEvalHooks.asScala.values.map(_.run()).joinT

  def evalWithTypecheck(
    expr: Expr,
    closure: VarCtx,
    typeMode: FQLInterpreter.TypeMode): Query[Result[(Value, TypeExpr)]] =
    lookupTypeEnv(expr, closure.keys, typeMode) flatMap {
      case Result.Ok(typeEnv) =>
        // We could have a stale schema missing collection methods (indexes that
        // we're recently added).
        // This can lead to a failed type check if a missing method is used.
        // We check for cache staleness here to account for this.
        Cache.guardFromStalenessIf(
          scopeID,
          typeExpr(typeEnv, expr, closure, typeMode)) {
          case Result.Ok(_)  => false
          case Result.Err(_) => true
        } flatMap {
          case Result.Ok(tpe) =>
            evalExpr(expr)(closure) map {
              case Result.Ok(value)    => Result.Ok((value, tpe))
              case res @ Result.Err(_) => res
            }
          case res @ Result.Err(_) => Query.value(res)
        }
      case res @ Result.Err(_) => Query.value(res)
    }

  private def lookupTypeEnv(
    expr: Expr,
    boundVars: Iterable[String],
    mode: TypeMode): Query[Result[TypeEnv]] =
    if (mode == TypeMode.Disabled) {
      Query.value(Result.Ok(env.staticTypeEnv))
    } else {
      try {
        val (vars, types) = expr.freeVarsAndTypeVars
        val vars0 = vars -- boundVars
        env.lookupTypeEnv(this, vars0, types).map(Result.Ok(_))
      } catch {
        // FIXME: freeVars shouldn't stack overflow, but this works for now.
        case _: StackOverflowError =>
          Query.value(
            Result.Err(QueryCheckFailure.TypecheckStackOverflow(expr.span)))
      }
    }

  private def typeExpr(
    typeEnv: TypeEnv,
    expr: Expr,
    closure: VarCtx,
    mode: TypeMode): Query[Result[TypeExpr]] = {
    if (mode == TypeMode.Disabled) {
      return Query.value(Result.Ok(DisabledTypeResult))
    }

    Query.timing(TypingTimeMetric) {

      val tclosureQ = closure
        .map { case (k, v) =>
          v.staticType(this).map { ty => k -> TypeScheme.Simple(ty) }
        }
        .sequence
        .map(_.toMap)

      (tclosureQ, Query.state, Query.stats) par { (tclosure, state, stats) =>
        val typer = typeEnv.newTyper()
        typer.logException = logException

        // Use Scala Deadline-- the typing module doesn't depend on fauna.lang,
        // where TimeBound lives.
        val qTimeLeft = state.deadline.timeLeft
        val timeout = if (qTimeLeft < TypingTimeout) qTimeLeft else TypingTimeout
        val cpuTiming = CPUTiming()

        val exprRes = cpuTiming.measure {
          val res = typer.typeExpr(expr, tclosure, timeout.fromNow)
          typer.warnings.foreach(emitCheckDiagnostic)

          res.map { ty =>
            if (mode == TypeMode.InferType) {
              val typeExpr = typer.valueToExpr(ty)
              (typeExpr, expr) match {
                case (te, Expr.Block(body, _)) =>
                  // This will miss any intersections which contain intersections
                  // of
                  // functions. If you've managed that as a user, you're already
                  // doing something advanced enough that this warning will have no
                  // use.
                  val warn = te match {
                    case TypeExpr.Lambda(_, _, _, _) => true
                    case TypeExpr.Intersect(tes, _) =>
                      tes.forall(_ match {
                        case TypeExpr.Lambda(_, _, _, _) => true
                        case _                           => false
                      })
                    case _ => false
                  }
                  if (warn) {
                    emitCheckDiagnostic(
                      Warning(
                        "Function is not called.",
                        body.last.span,
                        hints = Seq(
                          Hint(
                            "Call the function.",
                            body.last.span.copy(start = body.last.span.end),
                            Some("()")))))
                  }
                case _ => ()
              }
              typeExpr
            } else {
              DisabledTypeResult
            }
          }
        }

        stats.timing(TypingCPUTimeMetric, cpuTiming.elapsedMillis)

        Query.value(exprRes)
      }
    }
  }

  private def evalExpr(expr: Expr)(implicit vctx: VarCtx): Query[Result[Value]] =
    expr match {
      case id @ Expr.Id(name, _) =>
        vctx.get(name) match {
          case Some(v) => v.toQuery
          case None =>
            env.getGlobal(this, id.name).map {
              _.map(_.toResult).getOrElse {
                QueryRuntimeFailure
                  .UnboundVariable(name, expr.span +: this.stackTrace)
                  .toResult
              }
            }
        }

      case Expr.Lit(v, span) => evalLiteral(v, span)

      case Expr.StrTemplate(parts, span) =>
        evalStrTemplate(new StringBuilder, parts, span)

      case Expr.If(pred, ifThen, span) => evalIf(pred, ifThen, span)

      case Expr.IfElse(pred, ieThen, ieElse, _) => evalIfElse(pred, ieThen, ieElse)

      case Expr.At(ts, body, _) => evalAtExpr(ts, body)

      case Expr.Match(_, _, span) =>
        // FIXME: implement
        QueryCheckFailure.Unimplemented("match expr not supported", span).toQuery

      case Expr.Lambda(params, vari, body, _) =>
        Value
          .Lambda(
            params.view.map(_.map(_.str)).to(ArraySeq),
            vari.mapT(_.str),
            body,
            vctx)
          .toQuery

      case Expr.MethodChain(e, chain, _) =>
        evalMethodChain(e, chain)

      case Expr.OperatorCall(e, op, args, argsSpan) =>
        op.str match {
          case "??"  => evalGetOrElseOp(e, args.head)
          case "&&"  => evalBooleanOp(e, args.head, shortValue = Value.False)
          case "||"  => evalBooleanOp(e, args.head, shortValue = Value.True)
          case "isa" => evalIsAOp(e, args.head)
          case _     => evalExpr(e).flatMapT(callOp(_, op, args.toSeq, argsSpan))
        }

      case Expr.Project(e, bindings, span) =>
        evalExpr(e).flatMapT(
          this
            .withStackFrame(span)
            .evalProject(_, bindings))

      case Expr.ProjectAll(_, span) =>
        // FIXME: implement
        QueryCheckFailure.Unimplemented("project expr not supported", span).toQuery

      case Expr.Object(fields, _) => evalObject(fields)

      case Expr.Tuple(elems, _) =>
        if (elems.sizeIs == 1) evalExpr(elems.head) else evalArray(elems)

      case Expr.Array(elems, _) => evalArray(elems)

      case Expr.Block(body, _) => evalBlockBody(body)
    }

  private def evalArgs(args: Seq[Expr])(
    implicit vctx: VarCtx): Query[Result[ArraySeq[Value]]] =
    args.iterator.map(evalExpr(_)).to(Iterable).sequenceT.mapT(_.to(ArraySeq))

  private def evalMethodChain(e: Expr, chain: Seq[Expr.MethodChain.Component])(
    implicit vctx: VarCtx) =
    evalExpr(e).flatMapT {
      evalMethodChain0(e.span, _, chain.toList, false).mapT { case (v, _) => v }
    }

  private def callOp(value: Value, op: Name, args: Seq[Expr], span: Span)(
    implicit vctx: VarCtx): Query[Result[Value]] =
    FieldTable.get(value).getOp(op) match {
      case None =>
        val err = QueryRuntimeFailure.FunctionDoesNotExist(
          op.str,
          value.dynamicType,
          op.span +: this.stackTrace)
        (value match {
          case v: Value.Null =>
            err.withHint(Hint("Null value created here", v.cause.span))
          case _ => err
        }).toQuery
      case Some(m) =>
        evalArgs(args).flatMapT(
          this.withStackFrame(span).callNativeMethod(m, value, _))
    }

  private def callNativeMethod(
    method: NativeMethod[Value],
    value: Value,
    args: ArraySeq[Value]
  ) =
    if (!method.arity.accepts(args.size)) {
      QueryRuntimeFailure
        .InvalidFuncArity(method.arity, args.size, this.stackTrace)
        .toQuery
    } else {
      Query
        .incrCompute()
        .flatMap(_ => method(this, value, args))
    }

  /** Returns Ok(_, false) when successful, and Ok(_, true) when short-circuiting.
    */
  private def evalMethodChain0(
    cspan: Span, // callee span
    value: Value,
    chain: List[Expr.MethodChain.Component],
    needsShortCircuit: Boolean)(
    implicit vctx: VarCtx): Query[Result[(Value, Boolean)]] = {

    import Value.Null.{ Cause => NCause }

    def evalNullAccess(nullish: Value, field: Name, nc: NCause, opt: Boolean) =
      (nullish, nc) match {
        // if orig value was a doc, based on the null cause, return a direct
        // error.
        case (_: Value.Doc, nc @ NCause.DocNotFound(doc, coll, _)) =>
          if (opt) {
            Result.Ok((field.span, Value.Null(nc.copy(span = cspan)), true)).toQuery
          } else {
            QueryRuntimeFailure
              .DocumentNotFound(coll, doc, cspan +: this.stackTrace)
              .toQuery
          }
        case (_: Value.Doc, nc @ NCause.ReadPermissionDenied(_, _)) =>
          if (opt) {
            Result.Ok((field.span, Value.Null(nc.copy(span = cspan)), true)).toQuery
          } else {
            QueryRuntimeFailure.PermissionDenied(cspan +: this.stackTrace).toQuery
          }
        // else convert to null access
        case _ =>
          if (opt) {
            Result.Ok((field.span, Value.Null(nc), true)).toQuery
          } else {
            QueryRuntimeFailure
              .InvalidNullAccess(field.str, nc.span, field.span +: this.stackTrace)
              .toQuery
          }
      }

    def evalSel(value: Value, field: Name, optional: Boolean) =
      FieldTable
        .getField(this.withStackFrame(field.span), value, field)
        .flatMap {
          case R.Val(v)     => Result.Ok((field.span, v, false)).toQuery
          case R.Null(nc)   => evalNullAccess(value, field, nc, optional)
          case R.Error(err) => err.toQuery
        }

    def evalAp(
      value: Value,
      args: Seq[Expr],
      optional: Boolean,
      span: Span,
      invCtx: Option[Name]) =
      value match {
        // todo ?. do we need to switch up the cause to reflect the short
        // circuit
        case v: Value.Null if optional => Result.Ok((span, v, true)).toQuery

        case Value.Null(NCause.MissingField(value, field)) =>
          QueryRuntimeFailure
            .FunctionDoesNotExist(
              field.str,
              value.dynamicType,
              field.span +: this.stackTrace)
            .toQuery

        case v: Value.Func =>
          evalArgs(args).flatMapT(
            this
              .withStackFrame(span)
              .evalApply(v, _)
              .mapT { v =>
                (span, v, false)
              })

        case v =>
          FieldTable.get(v).getApplyImpl match {
            case None if invCtx.isDefined =>
              QueryRuntimeFailure
                .InvalidFieldFunctionInvalidType(
                  invCtx.get.str,
                  v.dynamicType,
                  invCtx.get.span +: this.stackTrace)
                .toQuery

            case None =>
              QueryRuntimeFailure
                .InvalidType(
                  ValueType.AnyFunctionType,
                  value,
                  span +: this.stackTrace)
                .toQuery

            case Some(m) =>
              evalArgs(args).flatMapT(
                this.withStackFrame(span).callNativeMethod(m, v, _).mapT { v =>
                  (span, v, false)
                })
          }
      }

    if (chain.isEmpty) {
      Result.Ok((value, true)).toQuery
    } else {
      import Expr.MethodChain._

      val resQ: Query[Result[(Span, Value, Boolean)]] = chain.head match {
        case Bang(bangSpan) =>
          val span = cspan.to(bangSpan)
          value match {
            case doc: Value.Doc =>
              ReadBroker
                .guardFromNull(
                  this.withStackFrame(span),
                  doc,
                  Effect.Action.Function("!"))
                .map {
                  case FieldTable.R.Val(_) =>
                    Result.Ok((span, value, needsShortCircuit))
                  case FieldTable.R.Null(cause) =>
                    handleNullCause(cause, span +: stackTrace).toResult
                  case FieldTable.R.Error(err) => Result.Err(err)
                }
            case Value.Null(nc) =>
              handleNullCause(nc, span +: stackTrace).toQuery
            case _ => Result.Ok((span, value, needsShortCircuit)).toQuery
          }

        // anything except bang is skipped when short-circuiting
        case mc if needsShortCircuit => Result.Ok((mc.span, value, true)).toQuery

        case Select(_, field, optional) => evalSel(value, field, optional)
        case Apply(args, optional, span) =>
          evalAp(value, args, optional.isDefined, span, None)
        case Access(args, optional, span) =>
          val op = Name("[]", span)
          FieldTable.get(value).getAccessImpl match {
            case None =>
              QueryRuntimeFailure
                .FunctionDoesNotExist(
                  op.str,
                  value.dynamicType,
                  op.span +: this.stackTrace)
                .toQuery
            case Some(impl) =>
              evalArgs(args)
                .flatMapT(impl(this.withStackFrame(span), value, _, span))
                .flatMapT {
                  case R.Val(v) => Result.Ok((span, v, false)).toQuery
                  case R.Null(nc) =>
                    evalNullAccess(value, op, nc, optional.isDefined)
                  case R.Error(err) => err.toQuery
                }
          }

        case MethodCall(_, field, args, selOpt, applyOpt, span) =>
          FieldTable.getMethod(this, value, field).flatMap {
            case R.Null(nc) => evalNullAccess(value, field, nc, selOpt)

            case R.Val(Some(m)) =>
              evalArgs(args).flatMapT {
                this
                  .withStackFrame(span)
                  .callNativeMethod(m, value, _)
                  .mapT((span, _, false))
              }

            // fall back to sel/apply path
            case R.Val(None) =>
              evalSel(value, field, selOpt).flatMapT {
                // if short-circuiting, then don't eval the apply
                case (sp, v, true) => Result.Ok((sp, v, true)).toQuery
                case (_, v, false) =>
                  evalAp(v, args, applyOpt.isDefined, span, Some(field))
              }

            case R.Error(err) => err.toQuery
          }
      }

      resQ.flatMapT { case (sp, res, needsShortCircuit) =>
        evalMethodChain0(sp, res, chain.tail, needsShortCircuit)
      }
    }
  }

  private def evalProject(v: Value, bindings: Seq[(Name, Expr)])(
    implicit vctx: VarCtx): Query[Result[Value]] = {
    val obj = Expr.Object(bindings, stackTrace.currentStackFrame)
    val fn = Value.Lambda(ArraySeq(Some(Expr.This.name)), None, obj, vctx)
    v match {
      case doc: Value.Doc =>
        ReadBroker.guardFromNull(this, doc, Effect.Action.Projection).flatMap {
          case FieldTable.R.Null(cause) => Value.Null(cause).toQuery
          case FieldTable.R.Val(_)      => evalApply(fn, IndexedSeq(v))
          case FieldTable.R.Error(err)  => err.toQuery
        }
      case v: Value.Object =>
        evalApply(fn, IndexedSeq(v))
      case v: Value.Iterable =>
        FieldTable
          .getMethod(this, v, Name("map", stackTrace.currentStackFrame))
          .flatMap {
            case R.Val(Some(m)) =>
              callNativeMethod(m, v, ArraySeq(fn))
            case _ => sys.error(s"Unexpected missing method `map` for $v")
          }
      case v => v.toQuery
    }
  }

  def evalApply(fn: Value.Func, args: IndexedSeq[Value]): Query[Result[Value]] =
    Query.incrCompute() flatMap { _ =>
      fn match {
        case fn: Value.Func if !fn.arity.accepts(args.size) =>
          QueryRuntimeFailure
            .InvalidFuncArity(fn.arity, args.size, this.stackTrace)
            .toQuery

        case Value.Lambda(params, variOpt, expr, closure) =>
          Query.repo flatMap { repo =>
            val limit = repo.fqlxMaxStackFrames

            /** We are currently pre-emptively adding the stack frame for a method call
              * to the stack before we invoke the method.  To account for this we
              * deduct one from the frame depth before comparing.
              */
            if (stackDepth - 1 > limit) {
              QueryRuntimeFailure.StackOverflow(limit, this.stackTrace).toQuery
            } else {
              // Bind non-variadic arguments to the leading args.
              val pctx = params.iterator.zip(args.iterator).collect {
                case (Some(p), a) => (p, a)
              }
              // Bind any variadic argument to an array of the rest of the args.
              val varictx = variOpt.flatten map { v =>
                (v, Value.Array(args.drop(params.size): _*))
              }
              this.evalExpr(expr)(closure ++ pctx ++ varictx)
            }
          }

        case fn: Value.NativeFunc => fn(this, args)
      }
    }

  def evalLambda(
    fn: Value.Lambda,
    role: Option[Key.Role],
    name: String,
    args: IndexedSeq[Value]
  ): Query[Result[Value]] =
    role.fold(Query.value(this))(this.withRole) flatMap {
      _.evalApply(fn, args)
    }

  private def evalStrTemplate(
    sb: StringBuilder,
    parts: Seq[Either[String, Expr]],
    span: Span)(implicit vctx: VarCtx): Query[Result[Value]] =
    if (parts.isEmpty) {
      Value.Str(sb.toString).toQuery
    } else {
      val limit = StringPrototype.MaxSize
      parts.head match {
        case Left(strv) =>
          val size = sb.size + strv.size
          if (size > limit) {
            QueryRuntimeFailure
              .ValueTooLarge(
                s"string size $size exceeds limit $limit",
                span +: this.stackTrace)
              .toQuery
          } else {
            evalStrTemplate(sb.append(strv), parts.tail, span)
          }
        case Right(expr) =>
          evalExpr(expr)
            .flatMapT { v =>
              v.toDisplayString(this).flatMap { str =>
                val size = sb.size + str.size
                if (size > limit) {
                  QueryRuntimeFailure
                    .ValueTooLarge(
                      s"string size $size exceeds limit $limit",
                      span +: this.stackTrace)
                    .toQuery
                } else {
                  evalStrTemplate(sb.append(str), parts.tail, span)
                }
              }
            }
      }
    }

  private def evalIf(pred: Expr, ifThen: Expr, span: Span)(implicit vctx: VarCtx) = {
    evalExpr(pred) flatMapT {
      case Value.True  => evalExpr(ifThen)
      case Value.False => Result.Ok(Value.Null(span)).toQuery
      case v =>
        QueryRuntimeFailure
          .InvalidType(ValueType.BooleanType, v, pred.span +: this.stackTrace)
          .toQuery
    }
  }

  private def evalIfElse(pred: Expr, ieThen: Expr, ieElse: Expr)(
    implicit vctx: VarCtx) = {
    evalExpr(pred) flatMapT {
      case Value.True  => evalExpr(ieThen)
      case Value.False => evalExpr(ieElse)
      case v =>
        QueryRuntimeFailure
          .InvalidType(ValueType.BooleanType, v, pred.span +: this.stackTrace)
          .toQuery
    }
  }

  private def evalAtExpr(tsExpr: Expr, body: Expr)(implicit vctx: VarCtx) =
    evalExpr(tsExpr) flatMapT {
      case Value.Time(ts) =>
        Query.snapshotTime flatMap { snap =>
          if (ts > snap) {
            QueryRuntimeFailure
              .InvalidArgument(
                "at_time",
                "cannot evaluate `at` in the future",
                tsExpr.span +: this.stackTrace)
              .toQuery
          } else {
            atUserValidTime(ts) flatMap { fqlctx => fqlctx.evalExpr(body) }
          }
        }
      case Value.TransactionTime =>
        Query.snapshotTime flatMap { snap =>
          atUserValidTime(snap) flatMap { fqlctx => fqlctx.evalExpr(body) }
        }
      case v =>
        QueryRuntimeFailure
          .InvalidType(ValueType.TimeType, v, tsExpr.span +: this.stackTrace)
          .toQuery
    }

  private def evalBooleanOp(lhs: Expr, rhs: Expr, shortValue: Value.Boolean)(
    implicit vctx: VarCtx) =
    evalExpr(lhs) flatMapT {
      case v: Value.Boolean if v == shortValue => Result.Ok(v).toQuery
      case _: Value.Boolean =>
        evalExpr(rhs).flatMapT {
          case v: Value.Boolean => Result.Ok(v).toQuery
          case v =>
            QueryRuntimeFailure
              .InvalidType(ValueType.BooleanType, v, rhs.span +: this.stackTrace)
              .toQuery
        }
      case v =>
        QueryRuntimeFailure
          .InvalidType(ValueType.BooleanType, v, lhs.span +: this.stackTrace)
          .toQuery
    }

  private def evalGetOrElseOp(lhs: Expr, rhs: Expr)(implicit vctx: VarCtx) =
    evalExpr(lhs) flatMapT {
      case _: Value.Null => evalExpr(rhs)
      case doc: Value.Doc =>
        ReadBroker
          .guardFromNull(
            this.withStackFrame(lhs.span),
            doc,
            Effect.Action.Function("??"))
          .flatMap {
            case FieldTable.R.Null(_)    => evalExpr(rhs)
            case FieldTable.R.Val(_)     => doc.toQuery
            case FieldTable.R.Error(err) => err.toQuery
          }
      case v => v.toQuery
    }

  private def evalIsAOp(lhs: Expr, rhs: Expr)(implicit vctx: VarCtx) =
    (evalExpr(lhs), evalExpr(rhs)) parT { case (v, t) =>
      t match {
        case sobj: Value.SingletonObject =>
          Value.Boolean(sobj.contains(v)).toQuery
        case _ =>
          QueryRuntimeFailure
            .InvalidArgument(
              "type_object",
              "expected a module object",
              rhs.span +: this.stackTrace)
            .toQuery
      }
    }

  private def evalObject(fields: Seq[(Name, Expr)])(
    implicit vctx: VarCtx): Query[Result[Value]] = {

    def evalFields(
      exprFields: Seq[(Name, Expr)],
      valueFields: SeqMap[String, Value]): Query[Result[Value]] = {
      if (exprFields.isEmpty) {
        Result.Ok(Value.Struct(valueFields)).toQuery
      } else {
        val (fieldName, fieldExpr) = exprFields.head
        evalExpr(fieldExpr) flatMapT { fieldV =>
          evalFields(exprFields.tail, valueFields + ((fieldName.str, fieldV)))
        }
      }
    }

    evalFields(fields, SeqMap.empty)
  }

  private def evalArray(elems: Seq[Expr])(
    implicit vctx: VarCtx): Query[Result[Value]] =
    elems.view.map { evalExpr(_) }.sequenceT mapT {
      Value.Array.fromSpecific(_)
    }

  private def evalLiteral(lit: Literal, span: Span) =
    lit match {
      case Literal.Null      => Value.Null(span).toQuery
      case Literal.True      => Value.True.toQuery
      case Literal.False     => Value.False.toQuery
      case Literal.Str(strv) => Value.Str(strv).toQuery
      case Literal.Float(bd) => Value.Number(bd).toQuery
      case Literal.Int(bi)   => Value.Number(bi).toQuery
    }

  private def evalBlockBody(body: Seq[Expr.Stmt])(
    implicit vctx: VarCtx): Query[Result[Value]] = {
    def go(body: Seq[Expr.Stmt])(implicit vctx: VarCtx): Query[Result[Value]] =
      if (body.isEmpty) {
        throw new IllegalStateException("cannot eval empty block")
      } else if (body.length == 1) {
        body.head match {
          case Expr.Stmt.Expr(expr) => evalExpr(expr)(vctx)
          case _ =>
            throw new IllegalStateException("cannot return let stmt from block")
        }
      } else {
        body.head match {
          case Expr.Stmt.Expr(e) =>
            evalExpr(e).flatMapT { _ => go(body.tail) }
          case Expr.Stmt.Let(name, _, e, _) =>
            evalExpr(e).flatMapT { v =>
              go(body.tail)(vctx + (name.str -> v))
            }
        }
      }

    go(body)
  }
}
