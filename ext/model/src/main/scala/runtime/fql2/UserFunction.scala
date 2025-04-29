package fauna.model.runtime.fql2

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model
import fauna.model.LambdaWrapper
import fauna.repo.query.Query
import fauna.repo.values._
import fauna.repo.PagedQuery
import fql.ast.Span
import fql.typer.TypeScheme

/** A user-defined function. */
case class UserFunction(
  name: String,
  funcID: UserFunctionID,
  lambda: LambdaWrapper,
  role: Option[model.Key.Role] = None,
  calleeOpt: Option[Value] = None)
    extends NativeFunction {

  /** Return the source body, if a v10 user function */
  def source: Option[String] = lambda.source

  // NativeFunction impl

  lazy val callee = calleeOpt.getOrElse(Value.Null(Span.Null))

  lazy val arity = lambda.arity

  def accepts(args: IndexedSeq[Value]) = arity.accepts(args.size)

  lazy val signature: TypeScheme = lambda.internalSig

  def apply(
    ctx: FQLInterpCtx,
    args: IndexedSeq[Value]
  ): Query[Result[Value]] =
    lambda(ctx, args, name, role, funcID)
}

object UserFunction {
  def lookup(
    ctx: FQLInterpCtx,
    nameOrAlias: String,
    parent: Option[Value] = None): Query[Option[UserFunction]] =
    model.UserFunction
      .idByIdentifier(ctx, nameOrAlias)
      .flatMapT { id =>
        model.UserFunction.get(ctx.scopeID, id) mapT { udf =>
          UserFunction(nameOrAlias, id, udf.lambda, udf.role, parent)
        }
      }

  def getAll(
    scope: ScopeID
  ): PagedQuery[Iterable[UserFunction]] = {
    model.UserFunction
      .getAll(scope)
      .flatMapValuesT { udf =>
        Query.value(
          Seq(UserFunction(udf.name, udf.id, udf.lambda, udf.role)) ++ udf.alias
            .map { alias =>
              UserFunction(alias, udf.id, udf.lambda, udf.role)
            }
        )
      }
  }
}
