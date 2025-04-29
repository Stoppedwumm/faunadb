package fql.ast

final case class Name(str: String, span: Span) extends WithNullSpanName

sealed trait Node extends NodeVisit {
  def span: Span

  /** visit each node in a tree with a `visitor`. The visitor is called with each
    * node, and a callback to continue visiting its children. If the callback is
    * skipped, visiting will not recurse.
    */
  def visit(visitor: PartialFunction[(Node, () => Unit), Unit]): Unit

  def freeVars: FreeVars

  def freeTypeVars: FreeVars

  def freeVarsAndTypeVars: (FreeVars, FreeVars)

  /** Walks value name occurrences in the node in depth-first, left-right order
    * (in other words, evaluation order.)
    */
  def foreachVarName(f: Name => Unit, bound: Set[String] = Set.empty): Unit

  /** Walks type name occurrences in the node in depth-first, left-right order (in
    * other words, evaluation order.)
    */
  def foreachTypeName(f: Name => Unit, bound: Set[String] = Set.empty): Unit

  /** Walks name occurrences in the node in depth-first, left-right order (in
    * other words, evaluation order.)
    *
    * a Left[Name] is a type identifier. A Right[Name] is a value identifier.
    */
  def foreachName(
    f: Either[Name, Name] => Unit,
    boundTypes: Set[String] = Set.empty,
    boundVars: Set[String] = Set.empty): Unit
}

sealed trait Expr extends Node with WithNullSpanExpr

final object Expr {
  // does not use name, since the span would be equal
  final case class Id(str: String, span: Span) extends Expr {
    def name = Name(str, span)
  }
  final case class Lit(value: Literal, span: Span) extends Expr
  final case class StrTemplate(parts: Seq[Either[String, Expr]], span: Span)
      extends Expr

  // FIXME: decide if/where general ascriptions make sense
  // final case class Ascription(expr: Expr, tpe: TypeExpr, span: Span) extends Expr

  final case class If(pred: Expr, `then`: Expr, span: Span) extends Expr
  final case class At(ts: Expr, body: Expr, span: Span) extends Expr
  final case class IfElse(pred: Expr, `then`: Expr, `else`: Expr, span: Span)
      extends Expr

  final case class Match(e: Expr, branches: Seq[(PatExpr, Expr)], span: Span)
      extends Expr

  sealed trait Lambda extends Expr {
    def params: Seq[Option[Name]]
    def vari: Option[Option[Name]]
    def body: Expr
  }
  object Lambda {
    def unapply(l: Expr.Lambda)
      : Some[(Seq[Option[Name]], Option[Option[Name]], Expr, Span)] = {
      Some((l.params, l.vari, l.body, l.span))
    }
  }
  final case class LongLambda(
    params: Seq[Option[Name]],
    vari: Option[Option[Name]],
    body: Expr,
    span: Span)
      extends Lambda
  // A wrapper which indicates the body is a lambda which contains one or more
  // shorthand references to an unnamed parameter.
  final case class ShortLambda(body: Expr) extends Lambda {
    def params = Seq(Some(Name(Expr.This.name, Span.Null)))
    def vari = None
    def span = body.span
  }

  object This {
    val name = s"$$this"
    def apply(span: Span) = Id(name, span)
  }

  object Underscore {
    val name = "_"
    def apply(span: Span) = Id(name, span)
  }

  final case class OperatorCall(
    e: Expr,
    field: Name,
    args: Option[Expr],
    argsSpan: Span)
      extends Expr {
    val span = e.span.copy(end = argsSpan.end)
  }

  final case class MethodChain(
    e: Expr,
    chain: Seq[MethodChain.Component],
    span: Span)
      extends Expr
  final object MethodChain {
    sealed trait Component extends WithNullSpanMethodChain {
      def span: Span
    }

    // dotSpan contains the `.` and optional `?` before `field`.
    final case class Select(dotSpan: Span, field: Name, optional: Boolean)
        extends Component {
      def span = dotSpan.to(field.span)
    }

    // `span` contains the span of the `!`
    final case class Bang(span: Span) extends Component

    // `argsSpan` contains the span of the parens around the args.
    // `optional` contains the `?.` if present.
    final case class Apply(args: Seq[Expr], optional: Option[Span], applySpan: Span)
        extends Component {
      def span = optional match {
        case Some(o) => o.to(applySpan)
        case None    => applySpan
      }
    }

    // `dotSpan` is the `.` and optional `?` before the field.
    // `applySpan` is the span of the parens surrounding `args`.
    // `applyOptional` contains the optional `?.` before `applySpan`.
    final case class MethodCall(
      dotSpan: Span,
      field: Name,
      args: Seq[Expr],
      selectOptional: Boolean,
      applyOptional: Option[Span],
      applySpan: Span)
        extends Component {
      def span = dotSpan.to(applySpan)
    }

    // `optional` stores the optional `?.` before the `[]`.
    // `accSpan` stores the span of the square brackets surrounding `args`.
    final case class Access(args: Seq[Expr], optional: Option[Span], accSpan: Span)
        extends Component {
      def span = optional match {
        case Some(o) => o.to(accSpan)
        case None    => accSpan
      }
    }
  }

  final case class Project(e: Expr, bindings: Seq[(Name, Expr)], span: Span)
      extends Expr
  final case class ProjectAll(e: Expr, span: Span) extends Expr

  final case class Object(fields: Seq[(Name, Expr)], span: Span) extends Expr

  // A tuple literal evaluates to its single element if its length is one, or an
  // array otherwise.
  // TODO: fix the parser so that this expr AST variant can be eliminated.
  final case class Tuple(elems: Seq[Expr], span: Span) extends Expr
  final case class Array(elems: Seq[Expr], span: Span) extends Expr

  final case class Block(body: Seq[Stmt], span: Span) extends Expr

  sealed trait Stmt extends Node with WithNullSpanStmt
  final object Stmt {
    final case class Let(
      name: Name,
      tpe: Option[TypeExpr],
      value: fql.ast.Expr,
      span: Span)
        extends Stmt
    final case class Expr(expr: fql.ast.Expr) extends Stmt { def span = expr.span }
  }
}

sealed trait TypeExpr extends Node with WithNullSpanTypeExpr

final object TypeExpr {
  final case class Scheme(params: Seq[String], expr: TypeExpr)

  final case class Hole(span: Span) extends TypeExpr
  final case class Any(span: Span) extends TypeExpr
  final case class Never(span: Span) extends TypeExpr
  final case class Singleton(value: Literal, span: Span) extends TypeExpr

  // does not use name, since the span would be equal
  final case class Id(str: String, span: Span) extends TypeExpr {
    def name = Name(str, span)
  }
  final case class Cons(name: Name, targs: Seq[TypeExpr], span: Span)
      extends TypeExpr

  final case class Object(
    fields: Seq[(Name, TypeExpr)],
    wildcard: Option[TypeExpr],
    span: Span)
      extends TypeExpr
  final case class Interface(fields: Seq[(Name, TypeExpr)], span: Span)
      extends TypeExpr
  final case class Projection(proj: TypeExpr, ret: TypeExpr, span: Span)
      extends TypeExpr

  final case class Tuple(elems: Seq[TypeExpr], span: Span) extends TypeExpr

  final case class Lambda(
    params: Seq[(Option[Name], TypeExpr)],
    variadic: Option[(Option[Name], TypeExpr)],
    ret: TypeExpr,
    span: Span)
      extends TypeExpr {}

  final case class Union(members: Seq[TypeExpr], span: Span) extends TypeExpr
  final case class Intersect(members: Seq[TypeExpr], span: Span) extends TypeExpr

  final case class Difference(elem: TypeExpr, sub: TypeExpr, span: Span)
      extends TypeExpr

  final case class Recursive(name: Name, in: TypeExpr, span: Span) extends TypeExpr

  final case class Nullable(base: TypeExpr, qmarkSpan: Span, span: Span)
      extends TypeExpr
}

/** This represents a parsed type expression tree. Default values are allowed
  * everywhere, and in general this is designed to be as permissive as possible.
  *
  * Then, in a second pass, this is converted to a `SchemaTypeExpr` for field
  * definitions, and a `TypeExpr` for everything else.
  */
sealed trait PartialTypeExpr extends Node

object PartialTypeExpr {
  final case class Hole(span: Span) extends PartialTypeExpr
  final case class Any(span: Span) extends PartialTypeExpr
  final case class Never(span: Span) extends PartialTypeExpr
  final case class Singleton(value: Literal, span: Span) extends PartialTypeExpr

  // does not use name, since the span would be equal
  final case class Id(str: String, span: Span) extends PartialTypeExpr {
    def name = Name(str, span)
  }
  final case class Cons(name: Name, targs: Seq[PartialTypeExpr], span: Span)
      extends PartialTypeExpr

  final case class Object(fields: Seq[ObjectField], span: Span)
      extends PartialTypeExpr

  sealed trait ObjectField {
    def span: Span
  }

  final case class LiteralField(
    name: Name,
    ty: PartialTypeExpr,
    default: Option[Expr])
      extends ObjectField {
    def span = name.span.to(default.fold(ty.span)(_.span))
  }
  final case class WildcardField(star: Span, ty: PartialTypeExpr)
      extends ObjectField {
    def span = star.to(ty.span)
  }
  final case class InterfaceField(dotdotdot: Span) extends ObjectField {
    def span = dotdotdot
  }

  final case class Tuple(elems: Seq[PartialTypeExpr], span: Span)
      extends PartialTypeExpr

  final case class Lambda(
    params: Seq[(Option[Name], PartialTypeExpr)],
    variadic: Option[(Option[Name], PartialTypeExpr)],
    ret: PartialTypeExpr,
    span: Span)
      extends PartialTypeExpr {}

  final case class Union(members: Seq[PartialTypeExpr], span: Span)
      extends PartialTypeExpr
  final case class Intersect(members: Seq[PartialTypeExpr], span: Span)
      extends PartialTypeExpr

  final case class Difference(
    elem: PartialTypeExpr,
    sub: PartialTypeExpr,
    span: Span)
      extends PartialTypeExpr

  final case class Recursive(name: Name, in: PartialTypeExpr, span: Span)
      extends PartialTypeExpr

  final case class Nullable(base: PartialTypeExpr, qmarkSpan: Span, span: Span)
      extends PartialTypeExpr
}

/** A fully parsed and validated type expression for a field in schema.
  */
sealed trait SchemaTypeExpr extends Node with WithNullSpanSchemaTypeExpr {
  def asTypeExpr: TypeExpr
}

object SchemaTypeExpr {
  final case class Simple(te: TypeExpr) extends SchemaTypeExpr {
    def span = te.span
    def asTypeExpr = te
  }

  final case class Object(
    fields: Seq[(Name, SchemaTypeExpr, Option[Expr])],
    wildcard: Option[TypeExpr],
    span: Span)
      extends SchemaTypeExpr {
    def asTypeExpr =
      TypeExpr.Object(
        fields.map { case (n, t, _) => (n, t.asTypeExpr) },
        wildcard,
        span)
  }
}

sealed trait PatExpr extends Node with WithNullSpanPatExpr {
  import PatExpr._

  def boundVars: Set[String] =
    this match {
      case Hole(i, _)       => i.fold(Set.empty[String])(_.boundVars)
      case Bind(n, i, _)    => i.fold(Set.empty[String])(_.boundVars) + n.str
      case _: Type | _: Lit => Set.empty
      case Object(fields, _) =>
        fields.foldLeft(Set.empty[String]) { _ | _._2.boundVars }
      case Tuple(elems, _) =>
        elems.foldLeft(Set.empty[String]) { _ | _.boundVars }
      case Array(elems, rest, _) =>
        val r = rest.fold(Set.empty[String])(_.boundVars)
        elems.foldLeft(r) { _ | _.boundVars }
    }
}

final object PatExpr {
  final case class Hole(inner: Option[PatExpr], span: Span) extends PatExpr
  final case class Bind(name: Name, inner: Option[PatExpr], span: Span)
      extends PatExpr

  final case class Type(tpe: TypeExpr, span: Span) extends PatExpr
  final case class Lit(value: Literal, span: Span) extends PatExpr

  final case class Object(fields: Seq[(Name, PatExpr)], span: Span) extends PatExpr
  final case class Tuple(elems: Seq[PatExpr], span: Span) extends PatExpr
  final case class Array(elems: Seq[PatExpr], rest: Option[PatExpr], span: Span)
      extends PatExpr
}

sealed trait FSL extends Node

object FSL {

  final case class Annotation(keyword: Name, value: Name, span: Span) extends FSL

  final case class Node(
    annotations: Seq[Annotation],
    keyword: Name,
    name: Option[Name],
    body: Option[Value],
    span: Span,
    docComment: Option[Span] = None
  ) extends FSL

  final case class ColonType(colon: Span, ty: TypeExpr) extends FSL {
    def span = colon.to(ty.span)
  }

  final case class ColonSchemaType(colon: Span, ty: SchemaTypeExpr) extends FSL {
    def span = colon.to(ty.span)
  }

  final case class ColonPartialType(colon: Span, ty: PartialTypeExpr) extends FSL {
    def span = colon.to(ty.span)
  }

  final case class Arg(name: Name, ty: Option[ColonType]) extends FSL {
    def span = ty.fold(name.span) { ty => name.span.to(ty.span) }
  }

  final case class VarArg(dotdotdot: Span, name: Name, ty: Option[ColonType])
      extends FSL {
    def toArg = Arg(name, ty)
    def span = {
      val res = dotdotdot.to(name.span)
      ty.fold(res) { ty => res.to(ty.span) }
    }
  }

  sealed trait Value extends FSL

  final case class Block(openBrace: Span, items: Seq[Node], closeBrace: Span)
      extends Value {
    def span = openBrace.copy(end = closeBrace.end)
  }

  final case class Paths(openBracket: Span, paths: Seq[Expr], closeBracket: Span)
      extends Value {
    def span = openBracket.copy(end = closeBracket.end)
  }

  final case class Function(
    openParen: Span,
    args: Seq[Arg],
    variadic: Option[VarArg],
    closeParen: Span,
    ret: Option[ColonType],
    body: Expr.Block)
      extends Value {
    def span = openParen.copy(end = body.span.end)
  }

  sealed trait FieldKind
  object FieldKind {
    final object Defined extends FieldKind
    final object Compute extends FieldKind
    final object Wildcard extends FieldKind
  }

  final case class Field(
    kind: FieldKind,
    ty: Option[ColonPartialType],
    value: Option[FSL.Value])
      extends Value {
    def span = (ty, value) match {
      case (None, None)       => Span.Null
      case (Some(t), None)    => t.span
      case (None, Some(v))    => v.span
      case (Some(t), Some(v)) => t.span.to(v.span)
    }
  }

  object Field {
    val name = "field"

    def Keyword(sp: Span) = Name(name, sp)

    case object Keyword {
      final def unapply(f: Name) = Option.when(f.str == name) { f.span }
    }

    def Wildcard(ty: ColonPartialType) =
      Field(FieldKind.Wildcard, Some(ty), None)
  }

  sealed trait Migration extends Value

  object Migration {
    final case class Backfill(fp: Path, value: FSL.Lit, span: Span) extends Migration

    final case class Drop(path: Path, span: Span) extends Migration

    final case class Split(from: Path, to: Seq[Path], span: Span) extends Migration

    final case class Move(field: Path, to: Path, span: Span) extends Migration

    final case class Add(path: Path, span: Span) extends Migration

    final case class MoveWildcardConflicts(into: Path, span: Span) extends Migration

    final case class MoveWildcard(into: Path, span: Span) extends Migration

    final case class AddWildcard(span: Span) extends Migration
  }

  final case class Lit(expr: Expr) extends Value {
    def span = expr.span
  }

  final case class Invalid(span: Span) extends Value
}
