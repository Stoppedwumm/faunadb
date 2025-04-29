package fql.ast

import fql.ast.{ Expr, Name, Span, TypeExpr }

sealed trait GroupedItem {

  /** A unique, opaque identifier that allows diffing to determine when two items are equal. */
  private[fql] def id: AnyRef

  /** Returns if this item is the same as the default value for this item.
    */
  private[fql] def isDefault: Boolean = false
}

sealed trait Annotation extends GroupedItem {
  def kind: Annotation.Kind
  def config: Config.Scalar
  def span: Span
  private[fql] def id: AnyRef = kind
}

object Annotation {
  sealed abstract class Kind(val keyword: String) {
    override def toString = keyword
  }
  object Kind {
    case object Alias extends Kind("alias")
    case object Role extends Kind("role")
  }

  final case class Typed[C <: Config.Scalar](kind: Kind, config: C, span: Span)
      extends Annotation {
    def configValue = config.unwrap
  }

  def apply[C <: Config.Scalar](kind: Kind, config: C, span: Span): Typed[C] =
    new Typed(kind, config, span)

  type Id = Typed[Config.Id]
}

sealed trait Field extends GroupedItem {
  def kind: FSL.FieldKind
  def name: Name
  def ty: Option[TypeExpr]
  def value: Option[Config.Scalar]
  def span: Span

  private[fql] def id: AnyRef = (kind, name.str)
}

object Field {
  final case class Defined(
    name: Name,
    schemaType: SchemaTypeExpr,
    value: Option[Config.Expression],
    span: Span)
      extends Field {
    val kind = FSL.FieldKind.Defined
    def ty = Some(schemaType.asTypeExpr)
  }

  final case class Computed(
    kwSpan: Span,
    name: Name,
    ty: Option[TypeExpr],
    _value: Config.Lambda,
    span: Span)
      extends Field {
    def value = Some(_value)
    val kind = FSL.FieldKind.Compute
  }

  final case class Wildcard(
    _ty: TypeExpr,
    span: Span
  ) extends Field {
    val name = Name("*", Span.Null)
    def value = None
    val kind = FSL.FieldKind.Wildcard
    def ty = Some(_ty)
  }
}

case class MigrationBlock(items: Seq[MigrationItem], span: Span)
    extends Member
    with Config.Scalar {
  def kind = Member.Kind.Migrations
  def config = this
  private[ast] type V = this.type
  private[ast] def unwrap = this
}

sealed trait MigrationItem extends WithNullSpanMigrationItem

object MigrationItem {
  final case class Backfill(field: Path, value: FSL.Lit, span: Span)
      extends MigrationItem

  final case class Drop(field: Path, span: Span) extends MigrationItem

  final case class Split(from: Path, to: Seq[Path], span: Span) extends MigrationItem

  final case class Move(field: Path, to: Path, span: Span) extends MigrationItem

  final case class Add(field: Path, span: Span) extends MigrationItem

  final case class MoveWildcardConflicts(into: Path, span: Span)
      extends MigrationItem

  final case class MoveWildcard(into: Path, span: Span) extends MigrationItem

  final case class AddWildcard(span: Span) extends MigrationItem
}

sealed trait Member extends GroupedItem {
  def kind: Member.Kind
  def config: Config
  def span: Span

  private[fql] def id: AnyRef

  private[fql] final def nameOpt: Option[Name] =
    this match {
      case named: Member.Named => Some(named.name)
      case _                   => None
    }
}

object Member {
  sealed abstract class Kind(val keyword: String) {
    final def unapply(name: Name) = Option.when(name.str == keyword) { name.span }
    override def toString = keyword
  }
  object Kind {
    case object Index extends Kind("index")
    case object Terms extends Kind("terms")
    case object Values extends Kind("values")
    case object Unique extends Kind("unique")
    case object Check extends Kind("check")
    case object HistoryDays extends Kind("history_days")
    case object TTLDays extends Kind("ttl_days")
    case object DocumentTTLs extends Kind("document_ttls")
    case object Migrations extends Kind("migrations")
    case object Privileges extends Kind("privileges")
    case object Membership extends Kind("membership")
    case object Create extends Kind("create")
    case object CreateWithId extends Kind("create_with_id")
    case object Delete extends Kind("delete")
    case object Read extends Kind("read")
    case object Write extends Kind("write")
    case object HistoryRead extends Kind("history_read")
    case object HistoryWrite extends Kind("history_write")
    case object UnrestrictedRead extends Kind("unrestricted_read")
    case object Call extends Kind("call")
    case object Issuer extends Kind("issuer")
    case object JWKSURI extends Kind("jwks_uri")
    case object Role extends Kind("role")
  }

  final case class Typed[+C <: Config](kind: Kind, config: C, span: Span)
      extends Member {
    def configValue = config.unwrap
    private[fql] def id: AnyRef = kind
  }

  def apply[C <: Config](kind: Kind, config: C, span: Span): Typed[C] =
    new Typed(kind, config, span)

  /** Define a member that has a system default value associated with it.
    *
    * When set by the user, the `Set` variant is provided. When the FSL declaring the
    * `SchemaItem` lacks the member definition, the `Implicit` variant must be
    * provided.
    *
    * NOTE: When diffing items without the member's associated keyword, an implicit
    * variant must be loaded from disk, if any. See `SchemaValidate.scala`.
    */
  sealed trait Default[C <: Config.Scalar] extends Member {
    def config: C
    def configValue = config.unwrap
  }
  object Default {
    final case class Set[C <: Config.Scalar](
      member: Member.Typed[C],
      override val isDefault: Boolean)
        extends Default[C] {

      def kind = member.kind
      def config = member.config
      def span = member.span

      private[fql] def id = {
        // This assert is necessary since the `Implicit` variant relies on its kind
        // in order to match the `Set` variant when diffing.
        assert(member.id == member.kind, "default member id should equal its kind")
        member.id
      }
    }

    final case class Implicit[C <: Config.Scalar](
      kind: Kind,
      default: C,
      override val isDefault: Boolean)
        extends Default[C] {
      def config = default
      def span = Span.Null
      private[fql] def id = kind
    }
  }

  sealed trait Wrapper {
    def member: Member

    final def kind = member.kind
    final def config = member.config
    final def span = member.span
  }

  sealed trait Named extends Member with Wrapper {
    def name: Name
    private[fql] final def id: AnyRef = (member.id, name.str)
  }
  object Named {
    final case class Typed[C <: Config](name: Name, member: Member.Typed[C])
        extends Named {
      def configValue = member.configValue
    }

    def apply[C <: Config](name: Name, member: Member.Typed[C]): Typed[C] =
      new Typed(name, member)
  }

  sealed trait Repeated extends Member with Wrapper {
    override private[fql] final def id: AnyRef = (member.id, config.id)
  }
  object Repeated {
    final case class Typed[C <: Config](member: Member.Typed[C]) extends Repeated {
      def configValue = member.configValue
    }
    def apply[C <: Config](member: Member.Typed[C]): Typed[C] = new Typed(member)
  }

  type BoolT = Typed[Config.Bool]
  type StrT = Typed[Config.Str]
  type LongT = Typed[Config.Long]

  type SeqT[C <: Config.Scalar] = Typed[Config.SeqT[C]]
  type OptT[C <: Config] = Typed[Config.OptT[C]]

  type NamedT[C <: Config] = Named.Typed[C]
  type RepeatedT[C <: Config] = Repeated.Typed[C]
}

sealed trait Config extends GroupedItem {
  def span: Span

  private[ast] type V
  private[ast] def unwrap: V

  private[fql] final def id: AnyRef =
    this match {
      case c: Config.Id             => c.value.str
      case c: Config.Bool           => java.lang.Boolean.valueOf(c.value)
      case c: Config.Str            => c.value
      case c: Config.Long           => java.lang.Long.valueOf(c.value)
      case c: Config.Expression     => c.value.withNullSpan
      case c: Config.IndexTerm      => (c.path.withNullSpan, c.mva)
      case c: Config.IndexValue     => (c.path.withNullSpan, c.mva, c.asc)
      case c: Config.CheckPredicate => c.expr.withNullSpan
      case c: Config.Predicate      => c.expr.withNullSpan
      case c: Config.Lambda         => c.expr.withNullSpan
      case c: Config.Seq            => c.configs map { _.id }
      case c: Config.Opt            => c.config map { _.id }
      case c: Config.Block          => c.members map { _.id }
      case c: MigrationBlock        => c.items.map(_.withNullSpan)
    }
}

object Config {
  sealed trait Scalar extends Config
  final case class Id(value: Name) extends Scalar {
    def span = value.span
    private[ast] type V = Name
    private[ast] def unwrap = value
  }

  final case class Bool(value: scala.Boolean, span: Span) extends Scalar {
    private[ast] type V = scala.Boolean
    private[ast] def unwrap = value
  }

  final case class Str(value: String, span: Span) extends Scalar {
    private[ast] type V = String
    private[ast] def unwrap = value
  }

  final case class Long(value: scala.Long, span: Span) extends Scalar {
    private[ast] type V = scala.Long
    private[ast] def unwrap = value
  }

  final case class Expression(value: Expr) extends Scalar {
    private[ast] type V = Expr
    private[ast] def unwrap = value
    def span = value.span
  }

  final case class CheckPredicate(expr: Expr.Lambda, span: Span) extends Scalar {
    private[ast] type V = Expr.Lambda
    private[ast] def unwrap = expr
  }

  final case class Predicate(expr: Expr.Lambda, span: Span) extends Scalar {
    private[ast] type V = Expr.Lambda
    private[ast] def unwrap = expr
  }

  final case class Lambda(expr: Expr.Lambda, span: Span) extends Scalar {
    private[ast] type V = Expr.Lambda
    private[ast] def unwrap = expr
  }

  final case class IndexTerm(path: fql.ast.Path, mva: Boolean, span: Span)
      extends Scalar {
    private[ast] type V = this.type
    private[ast] def unwrap = this
  }

  final case class IndexValue(
    path: fql.ast.Path,
    mva: Boolean,
    asc: Boolean,
    span: Span)
      extends Scalar {
    private[ast] type V = this.type
    private[ast] def unwrap = this
  }

  sealed trait Seq extends Config {
    def configs: scala.Seq[Config.Scalar]
  }

  object Seq {
    final case class Typed[C <: Config.Scalar](configs: scala.Seq[C], span: Span)
        extends Seq {
      private[ast] type V = scala.Seq[C#V]
      private[ast] def unwrap = configs map { _.unwrap }
    }

    def apply[C <: Config.Scalar](configs: scala.Seq[C], span: Span): Typed[C] =
      new Typed(configs, span)
  }

  sealed trait Opt extends Config {
    def config: Option[Config]
    final def span = config.fold(Span.Null) { _.span }
  }

  object Opt {
    final case class Typed[C <: Config](config: Option[C]) extends Opt {
      private[ast] type V = Option[C#V]
      private[ast] def unwrap = config map { _.unwrap }
    }

    def apply[C <: Config](c: Option[C]): Typed[C] = new Typed(c)
    def some[C <: Config](c: C): Typed[C] = apply(Some(c))
    def empty[C <: Config]: Typed[C] = apply(None)
  }

  sealed trait Block extends Config {
    def members: scala.Seq[Member]
    private[ast] type V = this.type
    private[ast] def unwrap = this
  }

  type SeqT[C <: Config.Scalar] = Seq.Typed[C]
  type OptT[C <: Config] = Opt.Typed[C]
}

sealed abstract class SchemaItem(val kind: SchemaItem.Kind) extends GroupedItem {
  def name: Name
  def span: Span
  def docComment: Option[Span]
  def annotations: Seq[Annotation]
  private[fql] final def id: AnyRef = (kind, name.str)
}

object SchemaItem {

  sealed abstract class ItemConfig(kind: Kind) extends SchemaItem(kind) {
    def fields: Seq[Field]
    def members: Seq[Member]
  }

  sealed abstract class Kind(private val ord: Byte, val keyword: String)
      extends Ordered[Kind] {
    final def compare(that: Kind) = ord.compare(that.ord)
    final def unapply(name: Name) = Option.when(name.str == keyword) { name.span }
    override def toString = keyword
  }
  object Kind {
    case object Collection extends Kind(0, "collection")
    case object Function extends Kind(1, "function")
    case object Role extends Kind(2, "role")
    case object AccessProvider extends Kind(3, "access provider")
  }

  final case class Collection(
    name: Name,
    alias: Option[Collection.Alias] = None,
    fields: Seq[Field] = Nil,
    migrations: Option[Member.Typed[MigrationBlock]] = None,
    indexes: Seq[Collection.Index] = Nil,
    uniques: Seq[Collection.Unique] = Nil,
    checks: Seq[Collection.CheckConstraint] = Nil,
    historyDays: Collection.HistoryDays = Collection.DefaultHistoryDays,
    ttlDays: Option[Collection.TTLDays] = None,
    documentTTLs: Option[Collection.DocumentTTLs] = None,
    span: Span,
    docComment: Option[Span] = None)
      extends ItemConfig(Kind.Collection) {
    def annotations = alias.toSeq
    def members =
      Seq
        .newBuilder[Member]
        .addAll(indexes)
        .addAll(uniques)
        .addAll(checks)
        .addOne(historyDays)
        .addAll(ttlDays)
        .addAll(documentTTLs)
        .addAll(migrations)
        .result()
  }

  object Collection {

    type Alias = Annotation.Id
    type Index = Member.NamedT[Collection.IndexConfig]
    type Unique = Member.RepeatedT[Config.SeqT[Config.IndexTerm]]
    type CheckConstraint = Member.NamedT[Config.CheckPredicate]
    type HistoryDays = Member.Default[Config.Long]
    type TTLDays = Member.LongT
    type DocumentTTLs = Member.BoolT

    def DefaultHistoryDays: HistoryDays = {
      val days =
        System
          .getProperty("fauna.model.default-history_days", "0")
          .toLongOption
          .getOrElse(0L)

      Member.Default.Implicit(
        Member.Kind.HistoryDays,
        Config.Long(days, Span.Null),
        isDefault = true
      )
    }

    final case class IndexConfig(
      terms: Option[Index.Terms],
      values: Option[Index.Values],
      span: Span)
        extends Config.Block {
      def members =
        Seq
          .newBuilder[Member]
          .addAll(terms)
          .addAll(values)
          .result()
    }

    object Index {
      type Terms = Member.SeqT[Config.IndexTerm]
      type Values = Member.SeqT[Config.IndexValue]
    }
  }

  final case class Role(
    name: Name,
    privileges: Seq[Role.Privilege] = Nil,
    membership: Seq[Role.Membership] = Nil,
    span: Span,
    docComment: Option[Span] = None)
      extends ItemConfig(Kind.Role) {
    def annotations = Seq.empty
    def fields = Seq.empty
    def members =
      Seq
        .newBuilder[Member]
        .addAll(privileges)
        .addAll(membership)
        .result()
  }

  object Role {

    type Privilege = Member.NamedT[PrivilegeConfig]
    type Action = Member.OptT[Config.Predicate]
    type Membership = Member.NamedT[Config.OptT[Config.Predicate]]

    final case class PrivilegeConfig(actions: Seq[Action], span: Span)
        extends Config.Block {
      def members = actions
    }
  }

  final case class AccessProvider(
    name: Name,
    issuer: AccessProvider.Issuer,
    jwksURI: AccessProvider.JWKSURI,
    roles: Seq[AccessProvider.Role] = Nil,
    span: Span,
    docComment: Option[Span] = None)
      extends ItemConfig(Kind.AccessProvider) {
    def annotations = Seq.empty
    def fields = Seq.empty
    def members =
      Seq
        .newBuilder[Member]
        .addOne(issuer)
        .addOne(jwksURI)
        .addAll(roles)
        .result()
  }

  object AccessProvider {
    type Issuer = Member.StrT
    type JWKSURI = Member.StrT
    type Role = Member.NamedT[Config.OptT[Config.Predicate]]
  }

  final case class Function(
    name: Name,
    alias: Option[Function.Alias] = None,
    role: Option[Function.Role] = None,
    sig: Function.Sig,
    body: Expr.Block,
    span: Span,
    docComment: Option[Span] = None)
      extends SchemaItem(Kind.Function) {
    def annotations =
      Seq.newBuilder
        .addAll(alias)
        .addAll(role)
        .result()
  }

  object Function {
    type Alias = Annotation.Id
    type Role = Annotation.Id

    final case class Sig(args: Seq[Arg], ret: Option[TypeExpr], span: Span)
    final case class Arg(name: Name, ty: Option[TypeExpr], variadic: Boolean)
  }
}
