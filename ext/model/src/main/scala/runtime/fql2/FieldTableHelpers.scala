package fauna.model.runtime.fql2

import fauna.lang.syntax._
import fauna.model.runtime.fql2.NativeFunction.NameWithType
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.Span
import fql.typer.{ Type, TypeShape }

/** Defines an embedded DSL for defining static fields on categories objects. */
trait FieldTableHelpers[Self <: Value] {
  import FieldTable.R

  def selfType: TypeTag[Self]
  def tt = TypeTag

  def typeHint: Option[TypeShape.TypeHint] = None

  // Holds the implementation of self application
  private[this] var _applyImpl = Option.empty[NativeMethod[Self]]
  def applyImpl = _applyImpl

  // Holds the implementation of [] access. components below is used to generate a
  // TS-style sig in tests. See FQL2StdlibSpec
  private[this] var _accessImpl =
    Option
      .empty[(FQLInterpCtx, Self, Seq[Value], Span) => Query[Result[R[Value]]]]
  def accessImpl = _accessImpl
  private[this] var _accessSigComponents =
    Option.empty[(NameWithType[_], TypeTag[_])]
  def accessSigComponents = _accessSigComponents
  def accessSig = accessSigComponents.map { case (p, ret) =>
    Type.Function(p.ty -> ret.staticType).typescheme
  }

  // Used to build up a set of field resolver definitions.
  private[this] var _fields = Map.empty[String, FieldTable.Resolver[Self]]
  protected def fields = _fields

  // Used to build up a set of method definitions.
  private[this] var _methods = Map.empty[String, NativeMethod[Self]]
  protected def methods = _methods

  // Used to build up a set of operator implementations.
  private[this] var _ops = Map.empty[String, NativeMethod[Self]]
  protected def ops = _ops

  private def attachMethod(m: NativeMethod[Self]) = {
    require(
      !_fields.contains(m.name),
      s"Cannot redefine ${m.name}, already defined as field.")
    _methods = _methods.updatedWith(m.name)(e => Some(e.fold(m)(_.overload(m))))
  }

  private def reattachMethod(m: NativeMethod[Self]) = {
    require(
      _methods.contains(m.name),
      s"Cannot override ${m.name}, not already defined.")
    _methods = _methods.updated(m.name, m)
  }

  private def attachOp(m: NativeMethod[Self]) = {
    _ops = _ops.updatedWith(m.name)(e => Some(e.fold(m)(_.overload(m))))
  }

  /** Defines a field as a raw resolver */
  protected def defResolver(name: String, res: FieldTable.Resolver[Self]) = {
    require(
      !_fields.contains(name),
      s"Cannot redefine $name, already defined as field.")
    require(
      !_methods.contains(name),
      s"Cannot redefine $name, already defined as function member.")
    _fields += (name -> res)
  }

  /** Defines implementation of [] access */
  protected def defAccess[VR <: Value, V1 <: Value](ret: TypeTag[VR])(
    p1: NameWithType[V1])(
    impl: (FQLInterpCtx, Self, V1, Span) => Query[Result[R[Value]]]): Unit = {
    _accessImpl = Some((ctx, self, args, span) => {
      if (args.sizeIs != 1) {
        QueryRuntimeFailure
          .InvalidFuncArity(Value.Func.Arity(1), args.size, ctx.stackTrace)
          .toQuery
      } else {
        p1(args.head, ctx.stackTrace)(0).flatMapT(impl(ctx, self, _, span))
      }
    })
    _accessSigComponents = Some((p1, ret))
  }

  /** Defines a static field in this field table, which resolves using the given
    * resolver function. Example usage:
    * ```
    * defField("myField") { (ctx, self) =>
    *   // function implementation
    * }
    * ```
    * A type parameter may optionally be provided. If it is not provided, it
    * will be inferred to be `TypeTag.Any`.
    * ```
    * defField("myString" -> tt.Str) { (ctx, self) =>
    *   //function implementation that returns a Value.Str
    * }
    * ```
    */
  protected def defField[V <: Value](nameAndType: NameWithType[V])(
    impl: (FQLInterpCtx, Self) => Query[Value]): Unit = {
    val NameWithType(name, t) = nameAndType
    val sch = t.staticType.typescheme
    defResolver(name, FieldTable.Resolver((c, s) => impl(c, s).map(R.Val(_)), sch))
  }

  /** Defines a method with the given name. Example usage:
    * ```
    * defMethod("myMethod")("param") { (ctx, self, param) =>
    *   // function implementation
    * }
    * ```
    * Define an overloaded method by calling multiple times with different
    * numbers of parameters:
    * ```
    * defMethod("myOverload")() { (ctx, self) =>
    *   // function implementation
    * }
    * defMethod("myOverload")("param") { (ctx, self, param) =>
    *   // function implementation
    * }
    * ```
    * Use `.vararg` to define a method with variable number of parameters:
    * ```
    * defMethod("myVarargMethod").vararg("params") { (ctx, self, params) =>
    *     // function implementation
    * }
    * ```
    * Types may optionally be provided for parameters and the return type.
    * ```
    * defMethod("myIntToString" -> tt.Str)("myInt" -> tt.Int) {
    *   (ctx, self, myInt) =>
    *     // function implementation that returns a Value.Str
    * }
    * ```
    */
  protected def defMethod[VR <: Value](nameAndReturnType: NameWithType[VR]) =
    new MethodBuilder(nameAndReturnType, attachMethod)

  /** Override the method with the given name. All overloads are replaced. See
    * defMethod for example usage.
    */
  protected def overrideMethod[VR <: Value](nameAndReturnType: NameWithType[VR]) =
    new MethodBuilder(nameAndReturnType, reattachMethod)

  /** Defines an operator with the given name. See defMethod for example usage. */
  protected def defOp[VR <: Value](nameAndReturnType: NameWithType[VR]) =
    new MethodBuilder(nameAndReturnType, attachOp)

  protected class MethodBuilder[VR <: Value](
    nameAndReturnType: NameWithType[VR],
    attach: NativeMethod[Self] => Unit) {

    /** Defines a method with no parameters. */
    def apply()(impl: (FQLInterpCtx, Self) => Query[Result[VR]]): Unit =
      attach(NativeMethod[Self, VR](selfType, nameAndReturnType)(impl))

    /** Defines a method with 1 parameter. */
    def apply[V1 <: Value](p1: NameWithType[V1])(
      impl: (FQLInterpCtx, Self, V1) => Query[Result[VR]]
    ): Unit =
      attach(NativeMethod[Self, V1, VR](selfType, nameAndReturnType, p1)(impl))

    /** Defines a method with 2 parameters. */
    def apply[V1 <: Value, V2 <: Value](p1: NameWithType[V1], p2: NameWithType[V2])(
      impl: (FQLInterpCtx, Self, V1, V2) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, V2, VR](selfType, nameAndReturnType, p1, p2)(impl))

    /** Defines a method with 3 parameters. */
    def apply[V1 <: Value, V2 <: Value, V3 <: Value](
      p1: NameWithType[V1],
      p2: NameWithType[V2],
      p3: NameWithType[V3])(
      impl: (FQLInterpCtx, Self, V1, V2, V3) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, V2, V3, VR](selfType, nameAndReturnType, p1, p2, p3)(
          impl))

    /** Defines a method with 4 parameters. */
    def apply[V1 <: Value, V2 <: Value, V3 <: Value, V4 <: Value](
      p1: NameWithType[V1],
      p2: NameWithType[V2],
      p3: NameWithType[V3],
      p4: NameWithType[V4])(
      impl: (FQLInterpCtx, Self, V1, V2, V3, V4) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, V2, V3, V4, VR](
          selfType,
          nameAndReturnType,
          p1,
          p2,
          p3,
          p4)(impl))

    /** Defines a method with 5 parameters. */
    def apply[V1 <: Value, V2 <: Value, V3 <: Value, V4 <: Value, V5 <: Value](
      p1: NameWithType[V1],
      p2: NameWithType[V2],
      p3: NameWithType[V3],
      p4: NameWithType[V4],
      p5: NameWithType[V5])(
      impl: (FQLInterpCtx, Self, V1, V2, V3, V4, V5) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, V2, V3, V4, V5, VR](
          selfType,
          nameAndReturnType,
          p1,
          p2,
          p3,
          p4,
          p5)(impl))

    /** Defines a method with a variable number of parameters. */
    def vararg[V <: Value](ps: NameWithType[V])(
      impl: (FQLInterpCtx, Self, IndexedSeq[V]) => Query[Result[VR]]
    ): Unit =
      attach(NativeMethod.vararg[Self, V, VR](selfType, nameAndReturnType, ps)(impl))
  }

  /** Defines a static function with the given name. Example usage:
    * ```
    * defStaticFunction("myFn")("param") { (ctx, param) =>
    *   // function implementation
    * }
    * ```
    * Define an overloaded static function by calling multiple times with different
    * numbers of parameters:
    * ```
    * defStaticFunction("myOverload")() { (ctx) =>
    *   // function implementation
    * }
    * defStaticFunction("myOverload")("param") { (ctx, param) =>
    *   // function implementation
    * }
    * ```
    * Use `.vararg` to define a static function with variable number of parameters:
    * ```
    * defStaticFunction("myVarargFn").vararg("params") { (ctx, params) =>
    *     // function implementation
    * }
    * ```
    * Types may optionally be provided for parameters and the return type.
    * ```
    * defStaticFunction("myIntToString" -> tt.Str)("myInt" -> tt.Int) {
    *   (ctx, myInt) =>
    *     // function implementation that returns a Value.Str
    * }
    * ```
    */
  protected def defStaticFunction[VR <: Value](nameAndReturnType: NameWithType[VR]) =
    new MemberFunctionBuilder(nameAndReturnType, attachMethod)

  /** Override the static function with the given name. All overloads are
    * replaced. See defMethod for example usage.
    */
  protected def overrideStaticFunction[VR <: Value](
    nameAndReturnType: NameWithType[VR]) =
    new MemberFunctionBuilder(nameAndReturnType, reattachMethod)

  /** Defines the apply behavior of this singleton. That is, what happens when
    * the user calls `SingletonObj(args)` as a function. This will generally be
    * used to define constructor-like behavior. Example usage:
    * ```
    * defApply(tt.Any)("param") { (ctx, param) =>
    *   // function implementation
    * }
    * ```
    * Define an overloaded constructor by calling multiple times with different
    * numbers of parameters:
    * ```
    * defApply(tt.Any)() { ctx =>
    *   // function implementation
    * }
    * defApply(tt.Any)("param") { (ctx, param) =>
    *   // function implementation
    * }
    * ```
    * Use .varargs to define a constructor with variable number of parameters:
    * ```
    * defApply(tt.Any).vararg("params") { (ctx, params) =>
    *     // function implementation
    * }
    * ```
    * Types may optionally be provided for parameters.
    * ```
    * defApply(tt.Str)("myInt" -> tt.Int) { (ctx, myInt) =>
    *   // function implementation that returns a Value.Str
    * }
    * ```
    */
  protected def defApply[VR <: Value](returnType: TypeTag[VR]) =
    new MemberFunctionBuilder(
      // parent type and name are not used, as there's no way to retrieve the
      // apply impl as a value directly.
      "apply" -> returnType,
      { m => _applyImpl = Some(_applyImpl.fold(m)(_.overload(m))) }
    )

  class MemberFunctionBuilder[VR <: Value](
    nameAndReturnType: NameWithType[VR],
    attach: NativeMethod[Self] => Unit) {

    /** Defines a member function with no parameters. */
    def apply()(impl: (FQLInterpCtx) => Query[Result[VR]]): Unit =
      attach(NativeMethod[Self, VR](selfType, nameAndReturnType)((c, _) => impl(c)))

    /** Defines a member function with 1 parameter. */
    def apply[V1 <: Value](p1: NameWithType[V1])(
      impl: (FQLInterpCtx, V1) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, VR](selfType, nameAndReturnType, p1)((c, _, v1) =>
          impl(c, v1)))

    /** Defines a member function with 2 parameters. */
    def apply[V1 <: Value, V2 <: Value](p1: NameWithType[V1], p2: NameWithType[V2])(
      impl: (FQLInterpCtx, V1, V2) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, V2, VR](selfType, nameAndReturnType, p1, p2)(
          (c, _, v1, v2) => impl(c, v1, v2)))

    /** Defines a member function with 3 parameters. */
    def apply[V1 <: Value, V2 <: Value, V3 <: Value](
      p1: NameWithType[V1],
      p2: NameWithType[V2],
      p3: NameWithType[V3])(
      impl: (FQLInterpCtx, V1, V2, V3) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod[Self, V1, V2, V3, VR](selfType, nameAndReturnType, p1, p2, p3)(
          (c, _, v1, v2, v3) => impl(c, v1, v2, v3)))

    /** Defines a method with N parameters. */
    def params(params: IndexedSeq[NameWithType[_]])(
      impl: (FQLInterpCtx, IndexedSeq[Value]) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod.params[Self, VR](selfType, nameAndReturnType, params)(
          (c, _, vs) => impl(c, vs)))

    /** Defines a member function with a variable number of parameters. */
    def vararg[V <: Value](ps: NameWithType[V])(
      impl: (FQLInterpCtx, IndexedSeq[V]) => Query[Result[VR]]
    ): Unit =
      attach(
        NativeMethod.vararg[Self, V, VR](selfType, nameAndReturnType, ps)(
          (c, _, vs) => impl(c, vs)))
  }
}
