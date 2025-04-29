package fauna.model.runtime.fql2

import fauna.lang.syntax._
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.{ Name, Span }
import fql.typer.{ Type, TypeScheme }
import scala.collection.immutable.ArraySeq
import scala.language.implicitConversions

/** Base trait for all built-in functions. */
trait NativeFunction extends Value.NativeFunc {
  def arity: Value.Func.Arity
  def signature: TypeScheme

  /** Calls this function with the provided arguments. */
  def apply(
    ctx: FQLInterpCtx,
    args: IndexedSeq[Value]
  ): Query[Result[Value]]

  protected def `Only fauna.model.runtime.fql2.NativeFunction may extend Value.NativeFunc`() = {}
}

object NativeFunction {

  /** Convenience/helper class to enable defining functions & methods using the
    * notation: `"myParam" -> tt.Str`. The type is optional; default is
    * `TypeTag.Any`.
    */
  final case class NameWithType[V <: Value](n: String, t: TypeTag[V]) {
    def ty: Type = t.staticType
    def expected: ValueType = t.valueType
    type Repr = t.valueType.Repr

    def name(implicit idx: Int): String = if (idx >= 0) s"${n}$idx" else n
    def displayString: String = expected.displayString

    def accepts(arg: Value): Boolean = t.cast(arg).isDefined
    def apply(arg: Value, stackTrace: FQLInterpreter.StackTrace)(
      implicit idx: Int): Query[Result[V]] =
      Result.guardM {
        t.cast(arg)
          .getOrElse(Result.fail(invalidArg(arg, stackTrace)))
          .toQuery
      }

    private def invalidArg(actual: Value, stackTrace: FQLInterpreter.StackTrace)(
      implicit idx: Int) =
      QueryRuntimeFailure.InvalidArgumentType(
        name,
        expected,
        actual.dynamicType,
        stackTrace)
  }

  object NameWithType {
    implicit def apply(name: String): NameWithType[Value] =
      NameWithType(name, TypeTag.Any)
    implicit def apply[V <: Value](t: (String, TypeTag[V])): NameWithType[V] =
      NameWithType(t._1, t._2)
  }
}

/** A built-in method. It is 'unbound', in the sense that it is not attached to
  * a specific callee value.
  */
sealed trait NativeMethod[Self <: Value] {
  import FieldTable.R

  def name: String
  def arity: Value.Func.Arity
  def accepts(args: IndexedSeq[Value]): Boolean
  def signature: TypeScheme
  def selfType: TypeTag[Self]

  /** Creates a 'bound' `NativeFunction` that represents the instance of this
    * method on the callee `self`.
    */
  def bind(self: Self): NativeFunction = new NativeMethod.Bound(this, self)

  /** Returns a resolver which binds `self` and returns a NativeFunction */
  lazy val resolver: FieldTable.Resolver[Self] =
    FieldTable.Resolver((_, self) => R.Val(bind(self)).toQuery, signature)

  def overload(other: NativeMethod[Self]): NativeMethod[Self] = {
    import NativeMethod.Impl._
    if (name != other.name)
      throw new IllegalArgumentException(
        s"Incompatible method names: $name, ${other.name}")
    if (selfType != other.selfType)
      throw new IllegalArgumentException(
        s"Incompatible self type: ${selfType.displayString}, ${other.selfType.displayString}")
    (this, other) match {
      case (Overloaded(m1s), Overloaded(m2s))      => Overloaded(m1s ++ m2s)
      case (ths: FixedArg[Self], Overloaded(ms))   => Overloaded(ms :+ ths)
      case (Overloaded(ms), other: FixedArg[Self]) => Overloaded(ms :+ other)
      case (ths: FixedArg[Self], other: FixedArg[Self]) =>
        Overloaded(ArraySeq(ths, other))
      case _ => throw new IllegalArgumentException("Unsupported overload")
    }
  }

  /** Calls this method with the given arguments, and `self` as the callee. */
  def apply(
    ctx: FQLInterpCtx,
    self: Self,
    args: IndexedSeq[Value]
  ): Query[Result[Value]]
}

object NativeMethod {
  import NativeFunction.NameWithType

  /** A `Bound` method has been attached to a `self` value, and so is a function. */
  class Bound[Self <: Value](method: NativeMethod[Self], self: Self)
      extends NativeFunction {
    def callee: Value = self
    def name: String = method.name
    def arity: Value.Func.Arity = method.arity
    def signature: TypeScheme = method.signature

    def apply(
      ctx: FQLInterpCtx,
      args: IndexedSeq[Value]
    ): Query[Result[Value]] =
      method(ctx, self, args)
  }

  object Impl {

    /** An overloaded method that can be called with different numbers of
      * parameters.
      */
    case class Overloaded[Self <: Value](methods: ArraySeq[FixedArg[Self]])
        extends NativeMethod[Self] {
      def name = methods.head.name
      def selfType = methods.head.selfType

      val arity = Value.Func.Arity(methods.map { _.params.size }: _*)

      def accepts(args: IndexedSeq[Value]) =
        arity.accepts(args.size) && methods.exists(m => m.accepts(args))

      def expects(numArgs: Int) = methods
        .filter { _.arity.accepts(numArgs) }
        .map { _.expects }
        .mkString(" | ")

      val signature = {
        val tys = methods.map(_.signature).map {
          case TypeScheme.Skolemized(raw)   => raw
          case TypeScheme.Simple(raw: Type) => raw
          case sig => sys.error(s"invalid method signature for $name: $sig")
        }
        Type.Intersect(tys, Span.Null).typescheme
      }

      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ): Query[Result[Value]] = Result.guardM {
        methods
          .find { _.accepts(args) }
          .getOrElse(Result.fail(invalidArgs(args, ctx.stackTrace)))
          .apply(ctx, self, args)
      }

      private def invalidArgs(
        args: IndexedSeq[Value],
        stackTrace: FQLInterpreter.StackTrace) = {
        val actual =
          args.map { _.dynamicType.displayString }.mkString("(", ", ", ")")
        QueryRuntimeFailure.InvalidArgumentTypes(
          name,
          expects(args.size),
          actual,
          stackTrace)
      }
    }

    sealed abstract class FixedArg[Self <: Value](
      val selfType: TypeTag[Self],
      val name: String,
      val params: IndexedSeq[NameWithType[_]],
      val ret: TypeTag[_])
        extends NativeMethod[Self] {
      implicit val NoIdx: Int = -1
      def arity = Value.Func.Arity(params.size)
      val signature =
        Type
          .Function(
            params.map(p => Some(Name(p.n, Span.Null)) -> p.ty).to(ArraySeq),
            None,
            ret.staticType,
            Span.Null)
          .typescheme
      def accepts(args: IndexedSeq[Value]) = args.size == params.size &&
        params.zip(args).forall { case (param, arg) => param.accepts(arg) }
      def expects: String = params.map { _.displayString }.mkString("(", ", ", ")")
      def displayString: String = s"$name$expects"
    }

    sealed abstract class VarArg[Self <: Value](
      val selfType: TypeTag[Self],
      val name: String,
      val param: NameWithType[_],
      val ret: TypeTag[_])
        extends NativeMethod[Self] {
      val arity = Value.Func.Arity.Variable(0)
      val signature =
        Type
          .Function(
            ArraySeq.empty,
            Some(Some(Name(param.n, Span.Null)) -> param.ty),
            ret.staticType,
            Span.Null)
          .typescheme
      def accepts(args: IndexedSeq[Value]) = args.forall { param.accepts(_) }
    }
  }

  /** Creates a NativeMethod with no parameters. */
  def apply[Self <: Value, R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R])(
    fn: (FQLInterpCtx, Self) => Query[Result[R]]
  ): NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg(selfType, name, IndexedSeq(), ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) =
        fn(ctx, self)
    }
  }

  /** Creates a NativeMethod with 1 parameter. */
  def apply[Self <: Value, V1 <: Value, R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    p1: NameWithType[V1])(
    fn: (FQLInterpCtx, Self, V1) => Query[Result[R]]): NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg[Self](selfType, name, IndexedSeq(p1), ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) = {
        val IndexedSeq(a1) = args
        p1(a1, ctx.stackTrace) flatMapT { v1 => fn(ctx, self, v1) }
      }
    }
  }

  /** Creates a NativeMethod with 2 parameters. */
  def apply[Self <: Value, V1 <: Value, V2 <: Value, R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    p1: NameWithType[V1],
    p2: NameWithType[V2])(
    fn: (FQLInterpCtx, Self, V1, V2) => Query[Result[R]]): NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg[Self](selfType, name, IndexedSeq(p1, p2), ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) = {
        val IndexedSeq(a1, a2) = args
        p1(a1, ctx.stackTrace) flatMapT { v1 =>
          p2(a2, ctx.stackTrace) flatMapT { v2 =>
            fn(ctx, self, v1, v2)
          }
        }
      }
    }
  }

  /** Creates a NativeMethod with 3 parameters. */
  def apply[Self <: Value, V1 <: Value, V2 <: Value, V3 <: Value, R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    p1: NameWithType[V1],
    p2: NameWithType[V2],
    p3: NameWithType[V3]
  )(fn: (FQLInterpCtx, Self, V1, V2, V3) => Query[Result[R]]): NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg[Self](selfType, name, IndexedSeq(p1, p2, p3), ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) = {
        val IndexedSeq(a1, a2, a3) = args
        p1(a1, ctx.stackTrace) flatMapT { v1 =>
          p2(a2, ctx.stackTrace) flatMapT { v2 =>
            p3(a3, ctx.stackTrace) flatMapT { v3 =>
              fn(ctx, self, v1, v2, v3)
            }
          }
        }
      }
    }
  }

  /** Creates a NativeMethod with 4 parameters. */
  def apply[
    Self <: Value,
    V1 <: Value,
    V2 <: Value,
    V3 <: Value,
    V4 <: Value,
    R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    p1: NameWithType[V1],
    p2: NameWithType[V2],
    p3: NameWithType[V3],
    p4: NameWithType[V4]
  )(fn: (FQLInterpCtx, Self, V1, V2, V3, V4) => Query[Result[R]])
    : NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg[Self](selfType, name, IndexedSeq(p1, p2, p3, p4), ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) = {
        val IndexedSeq(a1, a2, a3, a4) = args
        p1(a1, ctx.stackTrace) flatMapT { v1 =>
          p2(a2, ctx.stackTrace) flatMapT { v2 =>
            p3(a3, ctx.stackTrace) flatMapT { v3 =>
              p4(a4, ctx.stackTrace) flatMapT { v4 =>
                fn(ctx, self, v1, v2, v3, v4)
              }
            }
          }
        }
      }
    }
  }

  /** Creates a NativeMethod with 4 parameters. */
  def apply[
    Self <: Value,
    V1 <: Value,
    V2 <: Value,
    V3 <: Value,
    V4 <: Value,
    V5 <: Value,
    R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    p1: NameWithType[V1],
    p2: NameWithType[V2],
    p3: NameWithType[V3],
    p4: NameWithType[V4],
    p5: NameWithType[V5]
  )(fn: (FQLInterpCtx, Self, V1, V2, V3, V4, V5) => Query[Result[R]])
    : NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg[Self](selfType, name, IndexedSeq(p1, p2, p3, p4, p5), ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) = {
        val IndexedSeq(a1, a2, a3, a4, a5) = args
        p1(a1, ctx.stackTrace) flatMapT { v1 =>
          p2(a2, ctx.stackTrace) flatMapT { v2 =>
            p3(a3, ctx.stackTrace) flatMapT { v3 =>
              p4(a4, ctx.stackTrace) flatMapT { v4 =>
                p5(a5, ctx.stackTrace) flatMapT { v5 =>
                  fn(ctx, self, v1, v2, v3, v4, v5)
                }
              }
            }
          }
        }
      }
    }
  }

  /** Creates a NativeMethod with N parameters. */
  def params[Self <: Value, R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    params: IndexedSeq[NameWithType[_]]
  )(fn: (FQLInterpCtx, Self, IndexedSeq[Value]) => Query[Result[R]])
    : NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.FixedArg[Self](selfType, name, params, ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) =
        fn(ctx, self, args)
    }
  }

  /** Creates a NativeMethod with a variable number of parameters. */
  def vararg[Self <: Value, V <: Value, R <: Value](
    selfType: TypeTag[Self],
    nameAndReturnType: NameWithType[R],
    p: NameWithType[V])(fn: (FQLInterpCtx, Self, IndexedSeq[V]) => Query[Result[R]])
    : NativeMethod[Self] = {
    val NameWithType(name, ret) = nameAndReturnType
    new Impl.VarArg[Self](selfType, name, p, ret) {
      def apply(
        ctx: FQLInterpCtx,
        self: Self,
        args: IndexedSeq[Value]
      ) =
        args.iterator.zipWithIndex
          .map { case (a, idx) => p(a, ctx.stackTrace)(idx) }
          .to(Seq)
          .sequenceT
          .mapT { _.to(IndexedSeq) }
          .flatMapT { fn(ctx, self, _) }
    }
  }
}
