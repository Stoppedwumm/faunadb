package fauna.model.runtime.fql2

import fauna.atoms.{ CredentialsID, DocID, KeyID, TokenID, UserCollectionID }
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.fql2.ValueCmp._
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.{ Name, Span }
import fql.typer.{ TypeScheme, TypeShape }

trait FieldTable[V <: Value] {
  import FieldTable.R
  // dynamic APIs with context
  def hasField(ctx: FQLInterpCtx, self: V, name: Name): Query[Boolean]
  def getField(ctx: FQLInterpCtx, self: V, name: Name): Query[R[Value]]
  def getMethod(
    ctx: FQLInterpCtx,
    self: V,
    name: Name): Query[R[Option[NativeMethod[V]]]]

  // static
  def selfType: TypeTag[V]
  def typeShape: TypeShape
  def getApplyImpl: Option[NativeMethod[V]]
  def getAccessImpl
    : Option[(FQLInterpCtx, V, Seq[Value], Span) => Query[Result[R[Value]]]]
  def getOp(name: Name): Option[NativeMethod[V]]
  def hasResolver(name: String): Boolean
  def getResolver(name: String): Option[FieldTable.Resolver[V]]
}

object FieldTable {

  object R {

    /** A field/resolver return value. */
    final case class Val[A](value: A) extends R[A]

    /** The null return caused by a null receiver. */
    final case class Null(cause: Value.Null.Cause) extends R[Nothing]

    /** A failure to compute a field/resolver result value. */
    final case class Error(qf: QueryFailure) extends R[Nothing]
  }

  /** The field's table result type. Note that type covers specific variants that in
    * system behavior that are otherwise too broad to be expressed by the `Result`
    * type.
    */
  sealed trait R[+A] {

    final def toQuery: Query[R[A]] = Query.value(this)
    final def toResult: Result[R[A]] = Result.Ok(this)
    final def toResultQ: Query[Result[R[A]]] = toResult.toQuery

    final def isVal: Boolean =
      this match {
        case R.Val(_) => true
        case _        => false
      }

    final def isNull(implicit ev: A <:< Value): Boolean =
      this match {
        case R.Null(_) | R.Val(_: Value.Null) => true
        case _                                => false
      }

    final def nullReceiver: Option[Value.Null.Cause] =
      this match {
        case R.Null(nc) => Some(nc)
        case _          => None
      }

    final def lift: Option[A] =
      this match {
        case R.Val(v) => Some(v)
        case _        => None
      }

    final def liftValue(implicit ev: A <:< Value): Option[Value] =
      lift match {
        case None | Some(_: Value.Null) => None
        case Some(other)                => Some(other)
      }

    final def unsafeGet: A =
      this match {
        case R.Val(value) => value
        case R.Null(nc) =>
          throw new IllegalStateException(s"unexpected null found: $nc")
        case R.Error(qf) =>
          throw new IllegalStateException(s"unexpected error found: $qf")
      }
  }

  final case class Resolver[-V](
    impl: (FQLInterpCtx, V) => Query[R[Value]],
    typescheme: TypeScheme)

  def get(value: Value): FieldTable[Value] = {
    val table = value match {
      case Value.Doc(DocID(_, UserCollectionID(_)), _, _, _, _) =>
        stdlib.UserDocPrototype.Any
      case Value.Doc(DocID(_, KeyID.collID), _, _, _, _) =>
        stdlib.KeyPrototype
      case Value.Doc(DocID(_, TokenID.collID), _, _, _, _) =>
        stdlib.TokenPrototype
      case Value.Doc(DocID(_, CredentialsID.collID), _, _, _, _) =>
        stdlib.CredentialPrototype
      case _: Value.Doc => stdlib.NativeDocPrototype.Any

      case _: Value.Int             => stdlib.IntPrototype
      case _: Value.Long            => stdlib.LongPrototype
      case _: Value.Double          => stdlib.DoublePrototype
      case _: Value.Boolean         => stdlib.BooleanPrototype
      case _: Value.Str             => stdlib.StringPrototype
      case _: Value.Null            => stdlib.NullPrototype
      case _: Value.Array           => stdlib.ArrayPrototype
      case _: Value.EventSource     => stdlib.EventSourcePrototype
      case _: Value.Set             => stdlib.SetPrototype
      case _: Value.ID              => stdlib.IDPrototype
      case _: Value.Time            => stdlib.TimePrototype
      case _: Value.Date            => stdlib.DatePrototype
      case _: Value.Struct          => stdlib.StructPrototype
      case _: Value.Bytes           => stdlib.BytesPrototype
      case _: Value.UUID            => stdlib.UUIDPrototype
      case _: Value.SetCursor       => stdlib.SetCursorPrototype
      case Value.TransactionTime    => stdlib.TransactionTimePrototype
      case v: Value.SingletonObject => v
      case _: UserFunction          => stdlib.UserFunctionPrototype
      case _: Value.Lambda          => stdlib.FunctionPrototype
      case _: Value.NativeFunc      => stdlib.FunctionPrototype
    }
    table.asInstanceOf[FieldTable[Value]]
  }

  /** Returns true if the value has a field `field`. This is equivalent but
    * possibly more efficient than getField(...) != null
    */
  def hasField(ctx: FQLInterpCtx, value: Value, field: Name) =
    get(value).hasField(ctx, value, field)

  def getField(ctx: FQLInterpCtx, value: Value, field: Name) =
    get(value).getField(ctx, value, field)

  def getMethod(ctx: FQLInterpCtx, value: Value, field: Name) =
    get(value).getMethod(ctx, value, field)
}

/** Base implementation of field table, which mixes in DSL */
trait BaseFieldTable[V <: Value] extends FieldTable[V] with FieldTableHelpers[V] {
  import FieldTable.R

  // dynamic APIs with context

  def hasField(ctx: FQLInterpCtx, self: V, name: Name) =
    Query.value(hasResolver(name.str))

  def getField(ctx: FQLInterpCtx, self: V, name: Name): Query[R[Value]] =
    getResolver(name.str) match {
      case Some(resolver) => resolver.impl(ctx, self)
      case None           => R.Val(Value.Null.missingField(self, name)).toQuery
    }

  def getMethod(
    ctx: FQLInterpCtx,
    self: V,
    name: Name): Query[R[Option[NativeMethod[V]]]] =
    R.Val(methods.get(name.str)).toQuery

  // static

  def getApplyImpl = applyImpl

  def getAccessImpl = accessImpl

  def getOp(name: Name) = ops.get(name.str)

  def hasResolver(name: String) =
    fields.contains(name) || methods.contains(name)

  def getResolver(name: String) =
    fields.get(name).orElse(methods.get(name).map(_.resolver))

  // FIXME: Once we have parameter names in typeschemes, this can be replaced
  // with `getResolver`
  def getFieldOrMethod(
    name: String): Option[Either[NativeMethod[V], FieldTable.Resolver[V]]] =
    fields.get(name).map(v => Right(v)).orElse(methods.get(name).map(v => Left(v)))

  lazy val typeShape: TypeShape = getTypeShape

  // Allows for overriding, as you can't get a super lazy val.
  protected def getTypeShape = {
    val fieldShapes = fields.view
      .mapValues(_.typescheme)
      .iterator
      .concat(methods.view.mapValues(_.signature).iterator)
      .toMap
    val opShapes = ops.view.mapValues(_.signature).toMap
    val accShape = accessSig
    val apShape = applyImpl.map(_.signature)
    TypeShape(
      self = selfType.typescheme,
      fields = fieldShapes,
      ops = opShapes,
      access = accShape,
      apply = apShape,
      typeHint = typeHint
    )
  }

  // universal equality/comparison impl

  defOp("==" -> tt.Boolean)("other" -> tt.Any) { (ctx, self, other) =>
    self.isEqual(ctx, other).map(Value.Boolean(_).toResult)
  }
  defOp("!=" -> tt.Boolean)("other" -> tt.Any) { (ctx, self, other) =>
    self.isEqual(ctx, other).map(v => Value.Boolean(!v).toResult)
  }
  defOp("<" -> tt.Boolean)("other" -> tt.Any) { (ctx, self, other) =>
    self.cmp(ctx, other).flatMap {
      case Some(v) => Value.Boolean(v < 0).toQuery
      case None    => unrelatedTypesComparison
    }
  }
  defOp("<=" -> tt.Boolean)("other" -> tt.Any) { (ctx, self, other) =>
    self.cmp(ctx, other).flatMap {
      case Some(v) => Value.Boolean(v <= 0).toQuery
      case None    => unrelatedTypesComparison
    }
  }
  defOp(">" -> tt.Boolean)("other" -> tt.Any) { (ctx, self, other) =>
    self.cmp(ctx, other).flatMap {
      case Some(v) => Value.Boolean(v > 0).toQuery
      case None    => unrelatedTypesComparison
    }
  }
  defOp(">=" -> tt.Boolean)("other" -> tt.Any) { (ctx, self, other) =>
    self.cmp(ctx, other).flatMap {
      case Some(v) => Value.Boolean(v >= 0).toQuery
      case None    => unrelatedTypesComparison
    }
  }

  private val unrelatedTypesComparison = Value.False.toQuery
}

/** Implementation and type signatures of a named type.
  * FIXME: no longer really a prototype...
  */
class Prototype[V <: Value](
  val selfType: TypeTag[V],
  private val isPersistable: Boolean)
    extends BaseFieldTable[V] {
  if (
    selfType != TypeTag.AnyStruct &&
    selfType != TypeTag.AnyDoc &&
    !selfType.isInstanceOf[TypeTag.NamedDoc]
  ) {
    defMethod("toString" -> tt.Str)() { (ctx, self) =>
      self.toDisplayString(ctx).map { str =>
        Value.Str(str).toResult
      }
    }
  }

  override def getTypeShape = {
    val ts = super.getTypeShape
    if (isPersistable) {
      ts.copy(isPersistable = isPersistable)
    } else {
      ts
    }
  }
}

/** Implementation and type signatures of a named singleton object */
abstract class SingletonObject(val name: String, val parent: Option[Value] = None)
    extends BaseFieldTable[Value]
    with Value.SingletonObject {

  protected def `Only fauna.model.runtime.fql2.SingletonObject may extend Value.SingletonObject`() = {}

  val selfType = TypeTag.Named(s"${name}Module")

  override val typeHint: Option[TypeShape.TypeHint] = Some(TypeShape.TypeHint.Module)

  // Is `v` a member of this type?
  def contains(v: Value): scala.Boolean
}

abstract class ModuleObject(name: String, parent: Option[Value] = None)
    extends SingletonObject(name, parent) {
  def contains(v: Value): scala.Boolean = v == this
}

abstract class CompanionObject(name: String, parent: Option[Value] = None)
    extends SingletonObject(name, parent)

/** Mix-in for field tables with dynamic fallbacks. defined resolvers take
  * precedent.
  */
trait DynamicFieldTable[V <: Value] extends BaseFieldTable[V] {
  import FieldTable.R

  protected def dynHasField(ctx: FQLInterpCtx, self: V, name: Name): Query[Boolean]
  protected def dynGet(ctx: FQLInterpCtx, self: V, name: Name): Query[R[Value]]

  override def hasField(ctx: FQLInterpCtx, self: V, name: Name): Query[Boolean] =
    if (hasResolver(name.str)) Query.value(true) else dynHasField(ctx, self, name)

  override def getField(ctx: FQLInterpCtx, self: V, name: Name): Query[R[Value]] =
    if (hasResolver(name.str)) super.getField(ctx, self, name)
    else dynGet(ctx, self, name)
}
