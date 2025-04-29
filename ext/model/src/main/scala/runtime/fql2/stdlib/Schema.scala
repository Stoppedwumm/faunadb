package fauna.model.runtime.fql2.stdlib

import fauna.lang.syntax._
import fauna.model.{ Collection, UserFunction }
import fauna.model.runtime.fql2._
import fauna.repo.values.Value

object Schema extends ModuleObject("Schema") {

  // HACK: namespace in FQL
  override val selfType = TypeTag.Named("FQL.SchemaModule")

  // FIXME: this could be better once we revise the doc type hierarchy
  private lazy val optDocType = tt.Any

  // FIXME: this ignores ctx.userValidTime because `idByIdentiferUncached` does
  // not take
  // a valid time.
  defStaticFunction("defForIdentifier" -> optDocType)("ident" -> tt.Str) {
    (ctx, identVal) =>
      val ident = identVal.value
      // We bias to collections by name or alias and then functions by name or alias
      // see GlobalContext and RuntimeEnv
      val collQ =
        Collection.idByIdentifierUncachedStaged(ctx.scopeID, ident).mapT(_.toDocID)
      val funcQ =
        UserFunction.idByIdentifierUncached(ctx.scopeID, ident).mapT(_.toDocID)

      collQ.orElseT(funcQ).map {
        case Some(id) => Value.Doc(id).toResult
        // Just using a "literal null" type here because we would need a new null
        // cause otherwise.
        case None => Value.Null(ctx.stackTrace.currentStackFrame).toResult
      }
  }
}
