package fauna.ast

import fauna.atoms._
import fauna.auth.{ Auth, EvalAuth }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ RefParser, UserFunction }
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.trace.GlobalTracer
import scala.collection.immutable.Queue
import scala.util.{ Left, Right }

object EvalContext {

  val MaxStackDepth = 200

  def pure(scope: ScopeID, apiVers: APIVersion, reason: String) =
    EvalContext(
      EvalAuth(scope),
      Timestamp.MaxMicros,
      None,
      Effect.Limit(Effect.Pure, reason),
      apiVers)

  def read(
    auth: Auth,
    snapshotTime: Timestamp,
    apiVers: APIVersion,
    reason: String): EvalContext =
    EvalContext(auth, snapshotTime, None, Effect.Limit(Effect.Read, reason), apiVers)

  def write(auth: Auth, snapshotTime: Timestamp, apiVers: APIVersion): EvalContext =
    EvalContext(auth, snapshotTime, None, Effect.Limit(Effect.Write, ""), apiVers)
}

case class EvalContext(
  auth: Auth,
  snapshotTime: Timestamp,
  validTimeOverride: Option[Timestamp],
  effectLimit: Effect.Limit = Effect.Limit(Effect.Write, ""),
  apiVers: APIVersion,
  bindings: Map[String, Literal] = Map.empty,
  stackDepth: Int = 0
) {

  private[this] val tracer = GlobalTracer.instance

  // Query eval will enforce snapshot time, so if validTime is not
  // overridden we can use Timestamp.MaxMicros for reads. This will
  // also pull in all pending writes which have not been assigned a
  // valid time yet.
  def validTime = validTimeOverride getOrElse Timestamp.MaxMicros

  // Use snapshotTime as a sentinel here in order to allow the
  // construction Time("now") to opt in to reading the current state,
  // and allow writes.
  def atValidTime(ts: Timestamp): EvalContext =
    if (ts == snapshotTime) {
      if (validTimeOverride.isEmpty) this else copy(validTimeOverride = None)
    } else {
      if (ts == validTime) this else copy(validTimeOverride = Some(ts))
    }

  // interpret the validTime arg passed to read functions
  def getValidTime(tsOpt: Option[AbstractTimeL]) =
    tsOpt match {
      case None                   => validTime
      case Some(TransactionTimeL) => Timestamp.MaxMicros
      case Some(TimeL(ts))        => ts
    }

  // create a new ec based on a validTime arg
  def atValidTime(tsOpt: Option[AbstractTimeL]): EvalContext =
    tsOpt match {
      case None                   => this
      case Some(TransactionTimeL) => resetValidTime
      case Some(TimeL(ts))        => atValidTime(ts)
    }

  def resetValidTime =
    if (validTimeOverride.isEmpty) this else copy(validTimeOverride = None)

  def scopeID = auth.scopeID

  private def evalList(exprs: List[Expression]): Query[R[List[Literal]]] =
    (exprs map eval)
      .accumulateEitherT(Queue.empty[EvalError], Queue.empty[Literal]) {
        _ ++ _
      } { _ :+ _ }
      .map {
        case Right(rq) => Right(rq.toList)
        case Left(lq)  => Left(lq.toList)
      }

  private def evalAlist(
    exprs: List[(String, Expression)]): Query[R[List[(String, Literal)]]] =
    (exprs map { case (k, v) =>
      eval(v) mapT { k -> _ }
    }).accumulateEitherT(Queue.empty[EvalError], Queue.empty[(String, Literal)]) {
      _ ++ _
    } { _ :+ _ }
      .map {
        case Right(rq) => Right(rq.toList)
        case Left(lq)  => Left(lq.toList)
      }

  private def bindLetVar(binding: (String, Literal)) =
    this.copy(bindings = bindings + binding)

  private def genLambdaArgMap(pat: LambdaPat, arg: Literal, pos: => Position) = {
    val b = Map.newBuilder[String, Literal]
    val errs = List.newBuilder[EvalError]

    def bind0(p: LambdaPat, r: Literal, pos: => Position): Unit = (p, r) match {
      case (LambdaNullPat, _) => ()

      case (LambdaScalarPat(label), r) => b += (label -> r)

      case (LambdaArrayPat(pats), ArrayL(elems)) =>
        var i = 0
        val pIter = pats.iterator
        val eIter = elems.iterator

        while (pIter.hasNext && eIter.hasNext) {
          bind0(pIter.next(), eIter.next(), pos at i)
          i += 1
        }

        if (pIter.hasNext || eIter.hasNext)
          errs += InvalidLambdaArity(pats.size, elems.size, pos)

      case (LambdaArrayPat(List(pat)), r) =>
        bind0(pat, r, pos at 0)

      case (LambdaArrayPat(pats), _) =>
        errs += InvalidLambdaArity(pats.size, 1, pos)
    }

    bind0(pat, arg, pos)

    val errsResult = errs.result()
    if (errsResult.nonEmpty) {
      Left(errsResult)
    } else {
      Right(b.result())
    }
  }

  def evalLambdaApply(
    lambda: LambdaL,
    arg: Literal,
    pos: Position
  ): Query[R[Literal]] =
    if (stackDepth >= EvalContext.MaxStackDepth) {
      Query.value(Left(List(LambdaStackOverflowError(lambda, pos))))
    } else {
      withSpan("lambda", pos) {
        lambda.parsedState flatMap { case (expr, captured) =>
          genLambdaArgMap(expr.pattern, arg, pos at "lambda") match {
            case Right(args) =>
              val newBindings = Map.newBuilder[String, Literal]
              newBindings.addAll(bindings)
              newBindings.addAll(captured)
              newBindings.addAll(args)
              // Increase "stack" depth and bind closure and argument values.
              // once .eval() returns, the context copy, which acts more or less as a
              // frame, is dropped.
              val ec =
                copy(stackDepth = stackDepth + 1, bindings = newBindings.result())

              Query.incrCompute() flatMap { _ =>
                ec.eval(expr.body)
              }

            case Left(es) =>
              Query.value(Left(es))
          }
        }
      }
    }

  def evalLambdaApplyTopLevel(lambda: LambdaL, arg: Literal): Query[R[Literal]] =
    Query.timing("Query.Lambda.Eval.Time") {
      evalLambdaApply(lambda, arg, RootPosition)
    }

  def timedEvalTopLevel(expr: Expression): Query[R[Literal]] =
    Query.timing("Query.Eval.Time") { eval(expr) }

  def timedParseAndEvalTopLevel(expr: Literal): Query[Either[List[Error], Literal]] =
    Query.timing("Query.Eval.Time") { parseAndEvalTopLevel(expr) }

  def parseAndEvalTopLevel(expr: Literal): Query[Either[List[Error], Literal]] =
    (QueryParser.parse(auth, expr, apiVers): Query[
      Either[List[Error], Expression]]) flatMapT eval

  def evalOpt(expr: Option[Expression]): Query[R[Option[Literal]]] =
    expr match {
      case Some(expr) => eval(expr) mapT { Some(_) }
      case None       => Query(Right(None))
    }

  def eval(expr: Expression): Query[R[Literal]] = {
    (expr: @unchecked) match {
      case AtE(tsE, expr, pos) =>
        withSpan("at", pos) {
          eval(tsE) flatMapT { tsR =>
            Query(Casts.TransactionTimestamp(tsR, pos at "at")) flatMapT {
              case TransactionTimeL =>
                resetValidTime.eval(expr)
              case TimeL(ts) =>
                Query.stats flatMap { stats =>
                  Query.snapshotTime flatMap { snapTime =>
                    if (ts > snapTime) {
                      stats.incr("Query.AtExpr.ValidTimeInFuture")
                      getLogger.warn(
                        s"Query in ${scopeID} used At() with a valid time ($ts) greater than snapshot time $snapTime.")

                      // FIXME: eventually reject future valid times:
                      // Query.value(Left(List(InvalidValidReadTime(ts, pos at
                      // "at"))))
                      eval(expr)
                    } else {
                      atValidTime(ts).eval(expr)
                    }
                  }
                }
            }
          }
        }

      case Literal(value) => Query(Right(value))

      case NativeCall(thunk, _, _, eff, pos) =>
        withSpan("native_call", pos) {
          if (effectLimit.allows(eff)) {
            Query.incrCompute() flatMap { _ =>
              thunk(this)
            }
          } else {
            Query(Left(List(InvalidEffect(effectLimit, eff, pos))))
          }
        }

      case CallE(ref, args, pos) =>
        withSpan("call", pos) {
          (eval(ref), eval(args getOrElse ArrayE(Nil))) parT {
            // FIXME: Reenable.
            // case (lambda: LambdaL, args) =>
            //   evalLambdaApply(lambda, args, pos)

            case (ref, args) =>
              val udf = Casts.StringOrUserFunctionRef(ref, pos at "call") match {
                case Right(Right(ref @ RefL(scope, UserFunctionID(id)))) =>
                  tracer.activeSpan foreach { s =>
                    s.addAttribute("scope", scope.toLong)
                    s.addAttribute("id", id.toLong)
                  }

                  UserFunction.get(scope, id) map {
                    _ toRight List(InstanceNotFound(Right(ref), pos at "call"))
                  }

                case Right(Right(RefL(_, id))) =>
                  throw new IllegalArgumentException(
                    s"Expected UserFunctionID, got $id.")

                case Right(Left(StringL(name))) =>
                  tracer.activeSpan foreach { _.addAttribute("name", name) }

                  UserFunction.idByNameActive(scopeID, name) flatMapT { id =>
                    UserFunction.get(scopeID, id)
                  } map {
                    case Some(udf) => Right(udf)
                    case None =>
                      val ref = RefParser.RefScope.UserFunctionRef(name, None)
                      Left(List(UnresolvedRefError(ref, pos at "call")))
                  }
                case Left(es) => Query.value(Left(es))
              }

              udf flatMapT { udf =>
                udf.lambda(this, args, udf.name, udf.role, udf.id, pos)
              }
          }
        }

      case e: LambdaE =>
        Query.value(Right(LambdaL(e, bindings)))

      case VarE(name) =>
        Query.value(Right(bindings(name)))

      case ArrayE(values) =>
        evalList(values) mapT { ArrayL(_) }

      case ObjectE(bindings) =>
        evalAlist(bindings) mapT { ObjectL(_) }

      case LetE(bindings, body) =>
        // XXX: compute data dependencies among binding
        // expressions, and execute independent expressions
        // concurrently.
        bindings match {
          case Nil => eval(body)
          case (k, v) :: Nil =>
            eval(v) flatMapT { v =>
              bindLetVar(k -> v).eval(body)
            }
          case (k, v) :: rest =>
            eval(v) flatMapT { v =>
              bindLetVar(k -> v).eval(LetE(rest, body))
            }
        }

      case IfE(condExpr, thenExpr, elseExpr, pos) =>
        withSpan("if", pos) {
          eval(condExpr) flatMapT { cond =>
            Query(Casts.Boolean(cond, pos at "if")) flatMapT {
              if (_) eval(thenExpr) else eval(elseExpr)
            }
          }
        }

      case SelectE(name, pathExpr, fromExpr, allExpr, defaultExpr, pos) =>
        withSpan("select", pos) {
          (eval(pathExpr), eval(fromExpr), eval(allExpr getOrElse FalseL)) parT {
            (path, from, all) =>
              Query.value(Casts.Path(path, pos at name)) flatMapT { path =>
                Query.value(Casts.Boolean(all, pos at "all")) flatMapT { all =>
                  from match {
                    case UnresolvedRefL(orig) =>
                      Query.value(
                        Left(List(UnresolvedRefError(orig, pos at "from"))))

                    case _ =>
                      ReadAdaptor.select(
                        this,
                        path,
                        all,
                        from,
                        None,
                        pos at "from") flatMap {
                        case Left(errors @ List(ValueNotFound(_, _))) =>
                          defaultExpr map {
                            eval(_)
                          } getOrElse {
                            Query.value(Left(errors))
                          }

                        case errs @ Left(_) => Query.value(errs)

                        case Right(value) =>
                          Query.value(Right(value))
                      }
                  }
                }
              }
          }
        }

      case DoE(expr, exprs) =>
        ((expr :: exprs) map eval).accumulateT(NullL: Literal) { (_, r) =>
          r
        }

      case AndE(exprs, pos) =>
        def and0(acc: Boolean, tail: List[Expression], idx: Int): Query[R[BoolL]] = {
          if (tail.isEmpty) {
            Query.value(Right(BoolL(acc)))
          } else {
            if (acc) {
              eval(tail.head) flatMapT { value =>
                Query.value(Casts.Boolean(value, pos at "and" at idx)) flatMapT {
                  b =>
                    and0(b, tail.tail, idx + 1)
                }
              }
            } else {
              Query.value(Right(FalseL))
            }
          }
        }

        withSpan("and", pos) {
          and0(true, exprs, 0)
        }

      case OrE(exprs, pos) =>
        def or0(acc: Boolean, tail: List[Expression], idx: Int): Query[R[BoolL]] = {
          if (tail.isEmpty) {
            Query.value(Right(BoolL(acc)))
          } else {
            if (acc) {
              Query.value(Right(TrueL))
            } else {
              eval(tail.head) flatMapT { value =>
                Query.value(Casts.Boolean(value, pos at "or" at idx)) flatMapT { b =>
                  or0(b, tail.tail, idx + 1)
                }
              }
            }
          }
        }

        withSpan("or", pos) {
          or0(false, exprs, 0)
        }
    }
  } recover { case EvalErrorException(errs) =>
    Left(errs)
  }

  private def withSpan(expr: String, pos: Position)(f: => Query[R[Literal]]) =
    Query.withSpan("eval") {
      tracer.activeSpan foreach { s =>
        s.setOperation(s"eval.$expr")
        s.addAttribute("position", pos.toString)
      }
      f
    }
}
