package fauna.model.runtime.fql2.stdlib

import fauna.flags.EvalV4FromV10
import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.serialization._
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.{ Name, Span }
import fql.env.TypeEnv
import fql.error.Log
import fql.typer.{ Type, TypeShape }
import scala.collection.immutable.ArraySeq

/** Static set of top level values available in every FQL eval context. NB this
  * object is _not_ scoped. DB-specific objects like Collections are loaded
  * dynamically.
  *
  * Global is named "FQL" so that reification of global functions finds them on
  * the FQL module. for global methods.
  */
object Global {
  object Default extends Base with UserGlobalResolvers

  object Static extends Base

  sealed abstract class Base extends ModuleObject("FQL") with StaticGlobalResolvers {
    import GlobalResolvers.FQL
    defField(FQL.name -> FQL.selfType)((_, _) => Query.value(FQL))
  }

  lazy val StaticEnv = TypeEnv(Static.typeShape.fields, GlobalResolvers.typeShapes)
}

object GlobalResolvers {

  /** Top-level FQL object. */
  object FQL extends ModuleObject("FQL") with StaticGlobalResolvers {
    // add in future top-level singletons
    (GlobalResolvers.futureSingletons).foreach { case (n, s) =>
      defField(n -> s.selfType)((_, _) => Query.value(s))
    }

    defStaticFunction("evalV4" -> tt.Any)("expr" -> tt.AnyStruct) { (ctx, v4in) =>
      ctx.account.flatMap { account =>
        if (account.flags.get(EvalV4FromV10)) {
          UnholyEval.evalV4(ctx, v4in)
        } else {
          QueryRuntimeFailure
            .FunctionDoesNotExist("evalV4", FQL.dynamicType, ctx.stackTrace)
            .toQuery
        }
      }
    }
  }

  private val nativeColls =
    Seq(
      CollectionDefCompanion,
      FunctionDefCompanion,
      RoleCompanion,
      AccessProviderCompanion,
      DatabaseDefCompanion,
      KeyCompanion,
      CredentialCompanion,
      TokenCompanion
    )

  private val prototypes = Seq(
    stdlib.NullPrototype,
    stdlib.IDPrototype,
    stdlib.BooleanPrototype,
    stdlib.NumberPrototype,
    stdlib.IntPrototype,
    stdlib.LongPrototype,
    stdlib.DoublePrototype,
    stdlib.FloatPrototype,
    stdlib.StringPrototype,
    stdlib.StructPrototype,
    stdlib.BytesPrototype,
    stdlib.TimePrototype,
    stdlib.DatePrototype,
    stdlib.UUIDPrototype,
    stdlib.ArrayPrototype,
    stdlib.SetPrototype,
    stdlib.SetCursorPrototype,
    stdlib.EventSourcePrototype,
    stdlib.TransactionTimePrototype,
    stdlib.UserFunctionPrototype
  )

  private val modules = Seq(
    stdlib.ArrayCompanion,
    stdlib.BooleanCompanion,
    stdlib.BytesCompanion,
    stdlib.DateCompanion,
    stdlib.IDCompanion,
    stdlib.DocCompanion,
    stdlib.Math,
    stdlib.NullCompanion,
    stdlib.NumberCompanion,
    stdlib.IntCompanion,
    stdlib.LongCompanion,
    stdlib.DoubleCompanion,
    stdlib.FloatCompanion,
    stdlib.ObjectCompanion,
    stdlib.SetCompanion,
    stdlib.SetCursorCompanion,
    stdlib.EventSourceCompanion,
    stdlib.StringCompanion,
    stdlib.StructCompanion,
    stdlib.TimeCompanion,
    stdlib.TransactionTimeCompanion,
    stdlib.UUIDCompanion,
    stdlib.Query
  )

  // Top-level modules which cannot be introduced immediately go here. They
  // will be promoted to the `modules` member above in a future FQL version.
  private val futureModules = Seq(stdlib.Schema)

  def soMap(sos: Iterable[SingletonObject]) =
    sos.map(so => so.name -> so).toMap

  lazy val singletons: Map[String, SingletonObject] =
    soMap(modules ++ nativeColls :+ FQL) +
      ("Credentials" -> CredentialCompanion) // Adding this alias for early v10 users.

  // Top-level modules which cannot be introduced immediately go here. They
  // will be promoted to the `singletons` member above in a future FQL version.
  lazy val futureSingletons: Map[String, SingletonObject] =
    soMap(futureModules)

  lazy val staticShapes = Seq[(String, TypeShape)](
    {
      // EmptyRef<Doc>.
      val emptyRefType = TypeTag.EmptyRef(TypeTag.A)
      val emptyRefShape = TypeShape(
        emptyRefType.typescheme,
        // Add in fields common to null docs.
        fields = Map(
          "id" -> Type.ID.typescheme,
          "coll" -> Type.Any.typescheme,
          "exists" -> Type
            .Function(ArraySeq.empty, None, TypeTag.False.staticType, Span.Null)
            .typescheme
        ),
        isPersistable = false,
        alias = Some(Type.Null.typescheme)
      )
      emptyRefType.name -> emptyRefShape
    }, {
      // Ref<Doc> (== Doc | EmptyRef<Doc>).
      val refType = TypeTag.Ref(TypeTag.A)
      val refShape = TypeShape(
        refType.typescheme,
        isPersistable = true,
        alias = Some(
          Type
            .Union(TypeTag.A.staticType, Type.EmptyRef(TypeTag.A.staticType))
            .typescheme)
      )
      refType.name -> refShape
    }, {
      // EmptyNamedRef<Doc>.
      val emptyNamedRefType = TypeTag.EmptyNamedRef(TypeTag.A)
      val emptyNamedRefShape = TypeShape(
        emptyNamedRefType.typescheme,
        // Add in fields common to null docs.
        fields = Map(
          "name" -> Type.Str.typescheme,
          "coll" -> Type.Any.typescheme,
          "exists" -> Type
            .Function(ArraySeq.empty, None, TypeTag.False.staticType, Span.Null)
            .typescheme
        ),
        isPersistable = false,
        alias = Some(Type.Null.typescheme)
      )
      emptyNamedRefType.name -> emptyNamedRefShape
    }, {
      // NamedRef<Doc> (== Doc | EmptyNamedRef<Doc>).
      val namedRefType = TypeTag.NamedRef(TypeTag.A)
      val namedRefShape = TypeShape(
        namedRefType.typescheme,
        isPersistable = false, // v10 doesn't let you store (refs to) named docs.
        alias = Some(
          Type
            .Union(TypeTag.A.staticType, Type.EmptyNamedRef(TypeTag.A.staticType))
            .typescheme)
      )
      namedRefType.name -> namedRefShape
    }, {
      // Stream<A>.
      val streamType = Type.Named("Stream", Type.Param.A)
      val streamShape = TypeShape(
        streamType.typescheme,
        alias = Some(Type.EventSource(TypeTag.A).typescheme)
      )
      streamType.name.str -> streamShape
    }
  )

  lazy val typeShapes: Map[String, TypeShape] = {
    (prototypes ++ modules ++ futureModules :+ FQL)
      .map(so => so.selfType.name -> so.typeShape)
      .toMap ++
      nativeColls.flatMap(_.typeShapes) ++ staticShapes
  }
}

trait UserGlobalResolvers extends DynamicFieldTable[Value] {
  import FieldTable.R

  // dynamic lookup of user-defined db environment

  protected def dynHasField(
    ctx: FQLInterpCtx,
    self: Value,
    name: Name): Query[Boolean] =
    GlobalContext
      .lookupUserValue(ctx, name.str)
      .map(_.isDefined)

  protected def dynGet(ctx: FQLInterpCtx, self: Value, name: Name): Query[R[Value]] =
    GlobalContext
      .lookupUserValue(ctx, name.str)
      .getOrElseT(Value.Null.missingField(self, name))
      .map(R.Val(_))
}

trait StaticGlobalResolvers extends BaseFieldTable[Value] {

  // Static members

  // must subtract FQL here, since it should not be a member of itself.
  (GlobalResolvers.singletons - "FQL").foreach { case (n, s) =>
    defField(n -> s.selfType)((_, _) => Query.value(s))
  }

  defStaticFunction("log" -> tt.Null).vararg("args")((ctx, messages) =>
    messages.map { msg => msg.toDisplayString(ctx) }.sequence.flatMap { messages =>
      ctx
        .emitDiagnostic(
          Log(messages mkString " ", ctx.stackTrace.currentStackFrame, long = false))
        .map { _ =>
          Value.Null(ctx.stackTrace.currentStackFrame).toResult
        }
    })

  defStaticFunction("dbg" -> tt.A)("value" -> tt.A) { (ctx, value) =>
    value.toDebugString(ctx).flatMap { message =>
      ctx
        .emitDiagnostic(Log(message, ctx.stackTrace.currentStackFrame, long = true))
        .map { _ =>
          value.toResult
        }
    }
  }

  /** Only call within a guard block */
  private def checkArgArity(
    argName: String,
    accept: Int,
    fn: Value.Func,
    stackTrace: FQLInterpreter.StackTrace) =
    if (!fn.arity.accepts(accept)) {
      Result.unsafeFail(
        QueryRuntimeFailure
          .InvalidFuncParamArity(argName, accept, fn.arity, stackTrace))
    }

  import SetCompanion.{ accessorType, ascType, descType }

  defStaticFunction("asc" -> ascType)("accessor" -> accessorType) {
    (ctx, accessor) =>
      Result.guardM {
        checkArgArity("accessor", 1, accessor, ctx.stackTrace)
        Value.Struct("asc" -> accessor).toQuery
      }
  }

  defStaticFunction("desc" -> descType)("accessor" -> accessorType) {
    (ctx, accessor) =>
      Result.guardM {
        checkArgArity("accessor", 1, accessor, ctx.stackTrace)
        Value.Struct("desc" -> accessor).toQuery
      }
  }

  defStaticFunction("newId" -> tt.ID)() { (ctx) =>
    Effect.Action.Function("newId").check(ctx, Effect.Observation).flatMapT { _ =>
      Query.nextID.map { id => Result.Ok(Value.ID(id)) }
    }
  }

  // Globally-scoped standard library functions.

  // Cf. evalQuery materialization.
  private val AbortMaterializationTimeMetric =
    "Query.FQL2.Abort.Materialization.Time"

  defStaticFunction("abort" -> tt.Never)("return" -> tt.Any) { case (ctx, v) =>
    Query.timing(AbortMaterializationTimeMetric) {
      FQL2ValueMaterializer.materialize(ctx, v) flatMap {
        case Result.Ok(mv) =>
          QueryRuntimeFailure.Aborted(mv, ctx.stackTrace).toQuery
        case err @ Result.Err(_) => err.toQuery
      }
    }
  }
}
