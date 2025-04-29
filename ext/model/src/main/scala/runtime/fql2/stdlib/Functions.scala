package fauna.model.runtime.fql2.stdlib

import fauna.model
import fauna.model.runtime.fql2._
import fauna.model.schema.SchemaCollection
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.error.Hint
import fql.typer.Type

object FunctionPrototype
    extends Prototype(TypeTag.AnyFunction, isPersistable = false)

object UserFunctionPrototype
    extends Prototype(TypeTag.NamedUserFunction(TypeTag.A), isPersistable = false) {
  defField("definition" -> FunctionDefCompanion.docType) { (_, self) =>
    Query.value(Value.Doc(self.funcID.toDocID))
  }

  override def getTypeShape = {
    val shape = super.getTypeShape
    shape.copy(apply = Some(TypeTag.A.staticType.typescheme))
  }
}

object FunctionDefCompanion
    extends SchemaCollectionCompanion(
      SchemaCollection.UserFunction,
      NativeDocPrototype(
        SchemaCollection.UserFunction.name,
        docType = TypeTag.NamedDoc(s"${SchemaCollection.UserFunction.name}Def"),
        nullDocType = TypeTag.NullDoc(s"${SchemaCollection.UserFunction.name}Def")
      ),
      NamedNullDocPrototype
    ) {

  override val selfType = TypeTag.Named(s"${coll.name}Collection")

  override lazy val docImplType = Type
    .Record(
      "name" -> Type.Str,
      "alias" -> Type.Optional(Type.Str),
      "signature" -> Type.Optional(Type.Str),
      "ts" -> Type.Time,
      "coll" -> selfType.staticType,
      "role" -> Type.Optional(Type.Str),
      // FIXME: needs to union with ~something~ for v4 functions.
      "body" -> Type.Str,
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme
  override lazy val docCreateType = tt.Struct(
    "name" -> tt.Str,
    "alias" -> tt.Optional(tt.Str),
    "signature" -> tt.Optional(tt.Str),
    "role" -> tt.Optional(tt.Str),
    "body" -> tt.Str,
    "data" -> tt.Optional(tt.AnyStruct)
  )

  override def contains(v: Value): Boolean = v match {
    case _: Value.Lambda     => true
    case _: Value.NativeFunc => true
    case doc: Value.Doc      => doc.id.collID == collID
    case _                   => false
  }

  defApply(tt.Any)("function" -> tt.Str) { (ctx, function) =>
    UserFunction.lookup(ctx, function.value).flatMap {
      case Some(fn) => fn.toQuery
      case None     =>
        // Check if they attempted to create that function in this transaction.
        val hintsQ =
          model.UserFunction.idByNameUncached(ctx.scopeID, function.value).map {
            case Some(_) =>
              Seq(
                Hint(
                  "A function cannot be created and used in the same query.",
                  ctx.stackTrace.currentStackFrame))
            case None =>
              Seq.empty
          }

        hintsQ.map { hints =>
          Result.Err(
            QueryRuntimeFailure.InvalidArgument(
              "function",
              s"No such user function `${function.value}`.",
              ctx.stackTrace,
              hints))
        }
    }
  }
}
