package fauna.model

import fauna.ast._
import fauna.atoms.{ APIVersion, DocID, ScopeID, UserFunctionID }
import fauna.auth.{ Auth, EvalAuth, LoginSource }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.storage.ir.QueryV
import fql.ast.{ Span, Src, TypeExpr }
import fql.typer.{ Type, TypeScheme }
import scala.util.{ Left, Right }

object LambdaWrapper {
  private val anyTypeScheme = Type.Any.typescheme

  val PredEffectLimit = Effect.Limit(Effect.Read, "auth predicates")

  case class FQL4(lambda: LambdaL) extends LambdaWrapper {
    def userSig = None
    def internalSig = anyTypeScheme
    def arity = Value.Func.Arity(lambda.arity)
    def numRequiredArgs = lambda.arity
    def apiVersion = lambda.apiVersion
    def estimatedCallMaxEffect = lambda.estimatedCallMaxEffect
    def source = None
  }

  case class FQLX(
    lambda: Value.Lambda,
    userSig: Option[TypeExpr.Lambda],
    internalSig: TypeScheme,
    src: String)
      extends LambdaWrapper {
    def arity = lambda.arity
    def apiVersion = APIVersion.Default
    def estimatedCallMaxEffect = Effect.Write
    def source = Some(src)
  }

  def apply(
    lambda: Value.Lambda,
    userSig: Option[TypeExpr.Lambda],
    internalSig: TypeScheme,
    src: String): LambdaWrapper =
    FQLX(lambda, userSig, internalSig, src)

  def apply(lambda: LambdaL): LambdaWrapper = FQL4(lambda)

  // FIXME: these next two seems sorta out of place here

  type Src = Either[String, QueryV]

  def parsePredicate(
    scope: ScopeID,
    lambda: Src,
    srcName: Option[String] = None): Query[LambdaWrapper] =
    lambda match {
      case Left(src) =>
        // FIXME: As the name says, this function is for UDFs. This needs to
        // be replaced to make a `Src` for the predicate.
        FQLInterpreter.prepareUserFunc(Src.Predicate(srcName), src) flatMap {
          case Result.Ok(lambda) =>
            // FIXME: validate predicate signature conformance
            val sig = Type.Any.typescheme
            Query.value(LambdaWrapper(lambda, None, sig, src))
          case Result.Err(_) =>
            Query.fail(new IllegalStateException("invalid lambda"))
        }
      case Right(query) => Query.value(LambdaWrapper(LambdaL(scope, query)))
    }
}

sealed trait LambdaWrapper {
  def userSig: Option[TypeExpr.Lambda]
  def internalSig: TypeScheme
  def arity: Value.Func.Arity
  def apiVersion: APIVersion
  def estimatedCallMaxEffect: Effect
  def source: Option[String]

  // shared impl
  import LambdaWrapper.{ FQL4, FQLX, PredEffectLimit }

  /**  Evaluates the lambda from a predicate,
    *  avoid check call permission.
    *
    *  Always evaluates the lambda in a Read only context.
    */
  def evalPredicate(
    scope: ScopeID,
    args: Either[Literal, IndexedSeq[Value]],
    source: LoginSource,
    id: Option[DocID] = None,
    /** This is currently used for v10 streaming remove events. When an event is a remove in streaming, we want to
      * check the permissions against the prior version of the document, since that is the data being returned, not the
      * most recently updated version of the document. This allows us to provide the role read predicate with the state
      * of the document prior to its removal from the set.
      */
    snapshotTime: Option[Timestamp] = None
  ): Query[R[Literal]] =
    Query.snapshotTime.flatMap { time =>
      val auth = EvalAuth.read(scope, source, id)

      this match {
        case FQL4(lambda) =>
          val v4Args = args match {
            case Left(lit) => Query.value(Right(lit))
            case Right(args) =>
              v10ToV4Args(scope, args, lambda, FQLInterpreter.StackTrace.empty) map {
                case Result.Ok(v)    => Right(v)
                case Result.Err(err) => Left(err)
              }
          }

          v4Args flatMap {
            case Right(args) =>
              EvalContext(auth, time, None, PredEffectLimit, lambda.apiVersion)
                .evalLambdaApply(lambda, args, RootPosition)
            case Left(e: QueryRuntimeFailure) =>
              Query.value(Left(List(TransactionAbort(e.message, RootPosition))))
            case Left(QueryCheckFailure(errors)) =>
              val errMsg = errors map {
                _.messageLine
              } mkString "\n"
              Query.fail(
                new IllegalStateException(s"Error evaluating predicate.\n$errMsg"))
          }

        case FQLX(lambda, _, _, _) =>
          val v10Args = args match {
            case Left(lit)   => v4ToV10Args(scope, lit, lambda.params, None)
            case Right(args) => args
          }

          val ctx =
            FQLInterpreter(auth, PredEffectLimit, systemValidTime = snapshotTime)

          ctx.evalLambda(lambda, None, "predicate", v10Args) flatMap {
            case Result.Ok(v) =>
              UnholyEval.dataV10ToV4(auth.scopeID, v) flatMap {
                case Some(lit) => Query.value(Right(lit))
                case None =>
                  v.toDisplayString(ctx).map { str => Right(StringL(str)) }
              }

            case Result.Err(e: QueryRuntimeFailure) =>
              Query.value(Left(List(TransactionAbort(e.message, RootPosition))))

            case Result.Err(QueryCheckFailure(errors)) =>
              val errMsg = errors map {
                _.messageLine
              } mkString "\n"
              Query.fail(
                new IllegalStateException(s"Error evaluating predicate.\n$errMsg"))
          }
      }
    }

  /** Evaluates the lambda from FQL4 context */
  def apply(
    ec: EvalContext,
    args: Literal,
    name: String,
    role: Option[Key.Role],
    funcID: UserFunctionID,
    pos: Position): Query[R[Literal]] = {

    ec.auth.checkCallPermission(ec.scopeID, funcID, Left(args)) flatMap { allow =>
      if (allow) {
        this match {
          case FQL4(lambda) =>
            val evalContextQ = role match {
              case Some(role) =>
                Auth.changeRole(ec.auth, role) map { newAuth =>
                  ec.copy(auth = newAuth)
                }
              case None =>
                Query.value(ec)
            }

            evalContextQ flatMap { ec =>
              ec.evalLambdaApply(lambda, args, pos)
            } map {
              case Right(r) => Right(r)

              case Left(List(e @ LambdaStackOverflowError(l, _))) =>
                // Map this function's lambda object to a Function overflow
                // error.
                Left(
                  List(if (l eq lambda) FunctionStackOverflowError(funcID, pos)
                  else e))

              case Left(List(e: FunctionStackOverflowError)) =>
                Left(List(e))

              case Left(es) =>
                Left(List(FunctionCallError(funcID, pos, es)))
            }

          case FQLX(lambda, _, _, _) =>
            UnholyEval.ctxV4ToV10(ec).flatMap { intp =>
              val args0 =
                v4ToV10Args(ec.scopeID, args, lambda.params, ec.validTimeOverride)
              intp.evalLambda(lambda, role, name, args0).flatMap {
                case Result.Ok(v) =>
                  UnholyEval.dataV10ToV4(ec.scopeID, v) map {
                    _.toRight(
                      List(InvalidFQL4Value(v.dynamicType.displayString, pos)))
                  }

                case Result.Err(e: QueryRuntimeFailure) =>
                  e.abortReturn match {
                    case Some(v) =>
                      v.value.toDisplayString(intp).map { message =>
                        Left(List(TransactionAbort(message, pos)))
                      }
                    case None =>
                      Query.value(Left(List(TransactionAbort(e.message, pos))))
                  }
                case Result.Err(QueryCheckFailure(errors)) =>
                  val errMsg = errors map { _.messageLine } mkString "\n"
                  throw new IllegalStateException(
                    s"Error calling FQL v10 UDF `$name` from v4.\n$errMsg")
              }
            }
        }
      } else {
        Query.value(
          Left(List(PermissionDenied(Right(RefL(ec.scopeID, funcID)), pos))))
      }
    }
  }

  /** Evaluates the lambda from FQLX context */
  def apply(
    ctx: FQLInterpCtx,
    args: IndexedSeq[Value],
    name: String,
    role: Option[Key.Role],
    funcID: UserFunctionID
  ): Query[Result[Value]] = Result.guardM {

    this match {
      case FQLX(fn, _, _, _) =>
        ctx.auth.checkCallPermission(ctx.scopeID, funcID, Right(args)) flatMap {
          allow =>
            if (allow) {
              ctx.evalLambda(fn, role, name, args)
            } else {
              Result.fail(QueryRuntimeFailure.PermissionDenied(ctx.stackTrace))
            }
        }

      case FQL4(lam) =>
        val argLQ = v10ToV4Args(ctx.scopeID, args, lam, ctx.stackTrace) map {
          _.getOrFail
        }

        argLQ flatMap { argL =>
          ctx.auth.checkCallPermission(ctx.scopeID, funcID, Left(argL)) flatMap {
            case true =>
              UnholyEval.ctxV10ToV4(ctx).flatMap { v4ec =>
                val authQ = role match {
                  case Some(role) => Auth.changeRole(ctx.auth, role)
                  case None       => Query.value(ctx.auth)
                }

                authQ flatMap { auth =>
                  v4ec
                    .copy(auth = auth, apiVers = lam.apiVersion)
                    .evalLambdaApply(lam, argL, RootPosition)
                }
              } map {
                case Right(v) =>
                  UnholyEval
                    .dataV4ToV10(
                      ctx.scopeID,
                      v,
                      ctx.stackTrace.currentStackFrame,
                      ctx.userValidTime)
                    .toResult
                case Left(errs) =>
                  UnholyEval
                    .errV4ToV10(errs, lam.apiVersion, ctx.stackTrace)
                    .toResult
              }
            case false =>
              Result.fail(QueryRuntimeFailure.PermissionDenied(ctx.stackTrace))
          }
        }
    }
  }

  private def v4ToV10Args(
    scope: ScopeID,
    args: Literal,
    params: Seq[_],
    validTime: Option[Timestamp]): IndexedSeq[Value] = {
    UnholyEval.dataV4ToV10(scope, args, Span.Null, validTime) match {
      // () => {}
      case Value.Array(Seq()) if params.isEmpty => IndexedSeq.empty

      // (x, y, z) => {}
      case Value.Array(elems) if params.nonEmpty => elems

      case v => IndexedSeq(v)
    }
  }

  private def v10ToV4Args(
    scope: ScopeID,
    args: IndexedSeq[Value],
    lam: LambdaL,
    stackTrace: FQLInterpreter.StackTrace): Query[Result[Literal]] = {
    Result.guardM {
      val litsQ = lam.paramNames
        .zip(args)
        .map { case (name, arg) =>
          UnholyEval.dataV10ToV4(scope, arg) map {
            _.getOrElse {
              Result.fail(
                QueryRuntimeFailure.V4InvalidArgument(name, arg, stackTrace))
            }
          }
        }
        .sequence

      litsQ map {
        // Lambda(x => ...) & Lambda(_ => ...)
        case Seq(arg) if !lam.isArrayPat => Result.Ok(arg)

        // Lambda([x, y, z] => ...)
        case args => Result.Ok(ArrayL(args: _*))
      }
    }
  }
}
