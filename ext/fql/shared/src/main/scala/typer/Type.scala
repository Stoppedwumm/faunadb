package fql.typer

import fql.ast.{ Literal, Name, Span }
import fql.ast.display._
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.collection.mutable.Buffer
import scala.language.implicitConversions

/** A type which contains universally quantified type variables, which can be
  * instantiated to a given level.
  */
sealed trait TypeScheme { def raw: Constraint.Value }

object TypeScheme {

  /** A type without universally quantified variables */
  final case class Simple(raw: Constraint.Value) extends TypeScheme

  /** A type with universally quantified variables. Variables of level greater
    * than `level` are considered quantified.
    */
  final case class Polymorphic(raw: Constraint.Value, level: Type.Level)
      extends TypeScheme

  /** A type with opaque placeholders for quantified variables which needs to be
    * "deskolemized" before use.
    */
  final case class Skolemized(raw: Type) extends TypeScheme
}

sealed trait Constraint extends ConstraintHelpers {
  def level: Type.Level
  def span: Span
  def move(to: Span): Constraint

  def vars: Iterable[Either[Type.VarId, String]] = {
    val b = Buffer.empty[Either[Type.VarId, String]]
    forallVars { e =>
      e match {
        case Left(v)  => b += Left(v.id)
        case Right(s) => b += Right(s.name.str)
      }
      true
    }
    b
  }

  def hasVar = {
    val allSkolems = forallVars(_.isRight)
    !allSkolems
  }

  def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean): Boolean

  override final def equals(o: Any): Boolean = equalsImpl(o)
  override final def hashCode() = hashCodeImpl()
}

object Constraint {
  sealed trait Value extends Constraint with ValueHelpers {
    // Defined separately to have more narrow types
    def move(to: Span): Value
  }
  sealed trait Use extends Constraint {
    // Defined separately to have more narrow types
    def move(to: Span): Use
  }

  sealed trait Unleveled extends Constraint {
    def level = Type.Level.Zero
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) = true
  }

  // A lazy type, which will resolve itself when needed.
  final case class Lazy(private[typer] val thnk: () => Value) extends Value {
    lazy val self = thnk()

    def span = self.span
    def move(to: Span) = self.move(to)
    def level = self.level
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) = self.forallVars(f)

    override def toString = s"Lazy($self)"
  }

  object Lazy {
    def unapply(ty: Lazy) = Some(ty.self)
  }

  final case class Lit(lit: Literal, span: Span) extends Value with Unleveled {
    def move(to: Span) = copy(span = to)
    override def toString = lit.display
  }
  object Lit {
    def unapply(v: Value): Option[(Literal, Span)] =
      v match {
        case l: Lit            => Some((l.lit, l.span))
        case l: Type.Singleton => Some((l.lit, l.span))
        case _                 => None
      }
  }

  // Requires a record which has exactly the following fields. This will not allow
  // other types with the given field (different from Interface).
  final case class Rec(fields: SeqMap[String, Value], span: Span) extends Value {
    def move(to: Span) = copy(span = to)
    lazy val level = fields.foldLeft(Type.Level.Zero)(_ max _._2.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      fields.forall { case (_, v) => v.forallVars(f) }

    override def toString =
      fields.iterator.map { case (l, v) => s"$l: $v" }.mkString("{ ", ", ", " }")
  }
  object Rec {
    def unapply(v: Value): Option[(SeqMap[String, Value], Option[Type], Span)] =
      v match {
        case r: Rec         => Some((r.fields, None, r.span))
        case r: Type.Record => Some((r.fields, r.wildcard, r.span))
        case _              => None
      }
  }

  // Requires _any_ type which has the field. The `dotSpan` is the span of the
  // `.` This is used to produce a suggestion on the left of the dot, to add a
  // `!` operator.
  //
  // The `span` is the span of the field that is being selected.
  final case class Interface(field: String, ret: Use, dotSpan: Span, span: Span)
      extends Use {
    def move(to: Span) = copy(span = to)
    def level = ret.level
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      ret.forallVars(f)

    override def toString = s".$field: $ret"
  }
  object Interface {
    def unapply(u: Use): Option[(String, Use, Span, Span)] = u match {
      case i: Interface      => Some((i.field, i.ret, i.dotSpan, i.span))
      case i: Type.Interface => Some((i.field, i.ret, Span.Null, i.span))
      case _                 => None
    }
  }

  final case class Tup(elems: ArraySeq[Value], span: Span) extends Value {
    def move(to: Span) = copy(span = to)
    lazy val level = elems.foldLeft(Type.Level.Zero)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      elems.forall(_.forallVars(f))

    override def toString = elems.mkString("[", ", ", "]")
  }
  object Tup {
    def unapply(v: Value): Option[(ArraySeq[Value], Span)] = v match {
      case t: Tup        => Some((t.elems, t.span))
      case t: Type.Tuple => Some((t.elems, t.span))
      case _             => None
    }
  }

  // Constraint variant for getting the proj shape
  final case class Proj(proj: Value, ret: Use, span: Span) extends Use {
    def move(to: Span) = copy(span = to)
    def level = proj.level max ret.level
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      proj.forallVars(f) && ret.forallVars(f)
  }

  final case class Func(
    params: ArraySeq[(Option[Name], Use)],
    variadic: Option[(Option[Name], Use)],
    ret: Value,
    span: Span)
      extends Value {
    def move(to: Span) = this
    lazy val level = params.foldLeft(ret.level)(_ max _._2.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      params.forall(_._2.forallVars(f)) &&
        variadic.forall(_._2.forallVars(f)) &&
        ret.forallVars(f)

    override def toString = {
      val vstr = variadic.map {
        case (Some(name), ty) => s"...$name: Array<$ty>"
        case (None, ty)       => s"...Array<$ty>"
      }

      val pstr = params.iterator.map {
        case (Some(n), ty) => s"$n: $ty"
        case (None, ty)    => ty.toString
      }

      (pstr ++ vstr).mkString("(", ",", s") => $ret")
    }
  }
  object Func {
    def unapply(v: Value): Option[(ArraySeq[Use], Option[Use], Value)] = v match {
      case f: Func => Some((f.params.map(_._2), f.variadic.map(_._2), f.ret))
      case f: Type.Function =>
        Some((f.params.map(_._2), f.variadic.map(_._2), f.ret))
      case _ => None
    }
  }
  final case class Apply(args: ArraySeq[Value], ret: Use, span: Span) extends Use {
    def move(to: Span) = copy(span = to)
    lazy val level = args.foldLeft(ret.level)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      args.forall(_.forallVars(f)) && ret.forallVars(f)

    override def toString = args.mkString("(", ", ", s") => $ret")
  }
  object Apply {
    def unapply(u: Use): Option[(ArraySeq[Value], Use, Span)] = u match {
      case a: Apply         => Some((a.args, a.ret, a.span))
      case f: Type.Function => Some((f.params.map(_._2), f.ret, f.span))
      case _                => None
    }
  }

  final case class Access(args: ArraySeq[Value], ret: Use, span: Span) extends Use {
    def move(to: Span) = copy(span = to)
    lazy val level = args.foldLeft(ret.level)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      args.forall(_.forallVars(f)) && ret.forallVars(f)

    override def toString = args.mkString("[", ", ", s"] => $ret")
  }
  object Access {
    def unapply(u: Use): Option[(ArraySeq[Value], Use, Span)] = u match {
      case a: Access => Some((a.args, a.ret, a.span))
      case _         => None
    }
  }

  final case class Intersect(variants: ArraySeq[(Alt, Value)]) extends Value {
    def move(to: Span) = this
    def span = Span.Null
    lazy val level = variants.foldLeft(Type.Level.Zero) { case (lvl, (a, v)) =>
      lvl max a.vlevel max v.level
    }
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      variants.forall { case (_, v) => v.forallVars(f) }

    override def toString = variants.mkString("(", " & ", ")")
  }
  object Intersect {
    def unapply(v: Value): Option[ArraySeq[(Alt, Value)]] = v match {
      case i: Intersect => Some(i.variants)
      case _            => None
    }
  }

  final case class Union(variants: ArraySeq[(Alt, Use)]) extends Use {
    def move(to: Span) = this
    def span = Span.Null
    lazy val level = variants.foldLeft(Type.Level.Zero) { case (lvl, (a, v)) =>
      lvl max a.vlevel max v.level
    }
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      variants.forall { case (_, v) => v.forallVars(f) }

    override def toString = variants.mkString("(", " | ", ")")
  }
  object Union {
    def unapply(u: Use): Option[ArraySeq[(Alt, Use)]] = u match {
      case u: Union => Some(u.variants)
      case _        => None
    }
  }

  final case class Diff(value: Value, sub: Type) extends Value {
    def move(to: Span) = this
    def span = Span.Null
    lazy val level = value.level max sub.level
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) = {
      value.forallVars(f)
      sub.forallVars(f)
    }

    override def toString = s"($value - $sub)"
  }

  final case class DiffGuard(sub: Type, use: Use) extends Use {
    def move(to: Span) = this
    def span = Span.Null
    lazy val level = use.level max sub.level
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) = {
      sub.forallVars(f)
      use.forallVars(f)
    }

    override def toString = s"($sub | ($use - $sub))"
  }
}

sealed trait Type extends Constraint.Value with Constraint.Use {
  def move(to: Span): Type
  def span: Span

  /** For types with the static parameter convention, convert to a proper simple
    * or polymorphic typescheme.
    */
  def typescheme: TypeScheme = {
    val ids = vars
    if (ids.isEmpty) {
      TypeScheme.Simple(this)
    } else if (ids.forall(_.isRight)) {
      TypeScheme.Skolemized(this)
    } else {
      throw new IllegalArgumentException(
        s"invalid call to .typescheme on non-statically defined type $this")
    }
  }
}

object Type extends CoreTypes with StdTypes {

  final case class Level(toInt: Int) extends AnyVal with Ordered[Level] {
    def compare(o: Level) = java.lang.Integer.compare(toInt, o.toInt)
    def max(o: Level) = if (this >= o) this else o
    def incr = Level(toInt + 1)
    def decr = Level(toInt - 1)
  }
  object Level {
    val Zero = Level(0)
  }

  // Variables

  final case class VarId(toInt: Int) extends AnyVal {
    override def toString = s"v$toInt"
  }

  object Var {
    type Bounds =
      (ArraySeq[(Constraint.Value, Alt)], ArraySeq[(Constraint.Use, Alt)])

    final case class State(
      values: ArraySeq[(Constraint.Value, Alt)],
      uses: ArraySeq[(Constraint.Use, Alt)],
      // Collects the value/use checks that ths variable acts as a conduit for.
      // Constraint checks accumulated by variables are semantically a union of
      // intersections, based on the combination of Alt groups involved. See
      // TypeConstraintCheck for details.
      //
      // FIXME: verify that we do not need to handle chks in extrude and
      // freshen. It's possible we don't need to do anything here if the original
      // var's checks are accounted for elsewhere, but it's also probably safe to
      // propagate them since the goal is to keep track of all valid combinations of
      // alts.
      chk: TypeConstraintCheck,
      poisoned: Boolean)
    object State {
      val Empty =
        State(ArraySeq.empty, ArraySeq.empty, TypeConstraintCheck.Empty, false)
    }
  }

  /** A Var with a negative varID is a type parameter occurrence, and so adding
    * bounds is disallowed below.
    */
  final case class Var(id: VarId, level: Level) extends Type {
    import Var._

    var _state = State.Empty
    def bounds = Option.when(_state.values.nonEmpty || _state.uses.nonEmpty) {
      (_state.values, _state.uses)
    }
    def update(f: State => State) = _state = f(_state)

    private var _span = Span.Null
    def span = _span
    // we only move after instantiate, so we can just mutate the Var here.
    def move(to: Span) = {
      _span = to
      this
    }

    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) = f(Left(this))

    def values = _state.values.filterNot(_._2.isPruned)
    def values_=(vs: ArraySeq[(Constraint.Value, Alt)]) =
      update(_.copy(values = vs))
    def uses = _state.uses.filterNot(_._2.isPruned)
    def uses_=(us: ArraySeq[(Constraint.Use, Alt)]) =
      update(_.copy(uses = us))
    def check = TypeConstraintCheck.Var(this)
    def poisoned = _state.poisoned
    def poison() = update(_.copy(poisoned = true))

    def addValue(v: Constraint.Value, valt: Alt) = {
      require(id.toInt >= 0, "cannot add bound to negative ID variable")
      require(
        level >= valt.vlevel,
        s"level $level must be greater than valt.vlevel ${valt.vlevel}")
      update(s => s.copy(values = s.values :+ ((v, valt))))
    }

    def addUse(u: Constraint.Use, ualt: Alt) = {
      require(id.toInt >= 0, "cannot add bound to negative ID variable")
      require(
        level >= ualt.vlevel,
        s"level $level must be greater than ualt.vlevel ${ualt.vlevel}")
      update(s => s.copy(uses = s.uses :+ ((u, ualt))))
    }

    def addCheck(c: TypeConstraintCheck) = {
      require(id.toInt >= 0, "cannot add bound to negative ID variable")
      if (c != TypeConstraintCheck.Empty) {
        update(s => s.copy(chk = s.chk combine c))
      }
    }

    override def toString = s"${id}${"'" * level.toInt}"
  }

  // Types of types

  /** A "rigid" variable that cannot have bounds, occuring in an explicit type
    * signature/annotation
    */
  final case class Skolem(name: Name, level: Level) extends Type {

    private[this] var _invalid = false
    def isInvalid = _invalid
    def markInvalid(): Unit = _invalid = true

    def move(to: Span) = copy(name = name.copy(span = to))
    def span = name.span
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) = f(Right(this))

    override def toString =
      if (isInvalid) s"InvalidSkolem!${name.str}" else s"Skolem!${name.str}"
  }

  sealed trait UnleveledType extends Type with Constraint.Unleveled

  case class Any(span: Span) extends UnleveledType {
    def move(to: Span) = copy(span = to)
    override def toString = "Any"
  }
  case class Top(span: Span) extends UnleveledType {
    def move(to: Span) = copy(span = to)
    override def toString = "Top"
  }
  case class Never(span: Span) extends UnleveledType {
    def move(to: Span) = copy(span = to)
    override def toString = "Never"
  }
  object Any extends Any(Span.Null)
  object Top extends Top(Span.Null)
  object Never extends Never(Span.Null)

  final case class Singleton(lit: Literal, span: Span) extends UnleveledType {
    def move(to: Span) = copy(span = to)
    override def toString = lit.display
  }

  object Singleton {
    def apply(l: Literal): Type.Singleton = Type.Singleton(l, Span.Null)
    def apply(i: Int): Type.Singleton = Type.Singleton(Literal.Int(i), Span.Null)
    def apply(s: String): Type.Singleton = Type.Singleton(Literal.Str(s), Span.Null)
  }

  final case class Named(
    name: Name,
    span: Span,
    args: ArraySeq[Type] = ArraySeq.empty)
      extends Type {
    def move(to: Span) = copy(span = to)
    lazy val level = args.foldLeft(Level.Zero)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      args.forall(_.forallVars(f))

    override def toString =
      if (args.isEmpty) name.str else args.mkString(s"${name.str}<", ", ", ">")
  }

  object Named {
    def apply(name: String, args: Type*): Named =
      Named(Name(name, Span.Null), Span.Null, ArraySeq.from(args))
  }

  final case class Record(
    fields: SeqMap[String, Type],
    wildcard: Option[Type],
    span: Span = Span.Null)
      extends Type {
    def move(to: Span) = copy(span = to)
    lazy val level =
      fields.foldLeft(wildcard.fold(Level.Zero)(_.level))(_ max _._2.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      fields.forall(_._2.forallVars(f)) &&
        wildcard.forall(_.forallVars(f))

    override def toString = {
      val fs = fields.iterator.map { case (l, v) => s"$l: $v" }
      val w = wildcard.map(w => s"*: $w")
      fs.concat(w).mkString("{ ", ", ", " }")
    }
  }
  object Record {
    def apply(fields: (String, Type)*): Record =
      Record(fields.to(SeqMap), None, Span.Null)

    def Wild(wc: Type, fields: (String, Type)*): Record =
      Record(fields.to(SeqMap), Some(wc), Span.Null)
  }

  final case class Interface(field: String, ret: Type, span: Span) extends Type {
    def move(to: Span) = copy(span = to)
    def level = ret.level
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      ret.forallVars(f)

    override def toString = s".$field: $ret"
  }

  object Tuple {
    def apply(elems: Type*): Tuple = Tuple(elems.to(ArraySeq), Span.Null)
  }

  final case class Tuple(elems: ArraySeq[Type], span: Span) extends Type {
    def move(to: Span) = copy(span = to)
    lazy val level = elems.foldLeft(Level.Zero)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      elems.forall(_.forallVars(f))

    override def toString = elems.mkString("[", ",", "]")
  }

  final case class Function(
    params: ArraySeq[(Option[Name], Type)],
    variadic: Option[(Option[Name], Type)],
    ret: Type,
    span: Span)
      extends Type {

    def move(to: Span) = copy(span = to)
    lazy val level = params.foldLeft(ret.level)(_ max _._2.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      params.forall(_._2.forallVars(f)) &&
        variadic.forall(_._2.forallVars(f)) &&
        ret.forallVars(f)

    override def toString = {
      val vstr = variadic.map {
        case (Some(name), ty) => s"...$name: Array<$ty>"
        case (None, ty)       => s"...Array<$ty>"
      }

      val pstr = params.iterator.map {
        case (Some(n), ty) => s"$n: $ty"
        case (None, ty)    => ty.toString
      }

      (pstr ++ vstr).mkString("(", ",", s") => $ret")
    }
  }
  object Function {
    final case class Sig(params: ArraySeq[(Option[Name], Type)], ret: Type)
    object Sig {
      implicit def t0(t: (Unit, Type)) = Sig(ArraySeq(), t._2)
      implicit def t1(t: (Type, Type)) = Sig(ArraySeq(None -> t._1), t._2)
      implicit def t2(t: ((Type, Type), Type)) =
        Sig(ArraySeq(None -> t._1._1, None -> t._1._2), t._2)
      implicit def t3(t: ((Type, Type, Type), Type)) =
        Sig(ArraySeq(None -> t._1._1, None -> t._1._2, None -> t._1._3), t._2)

      implicit def t1n(t: ((String, Type), Type)) =
        Sig(ArraySeq(Some(Name(t._1._1, Span.Null)) -> t._1._2), t._2)
      implicit def t2n(t: (((String, Type), (String, Type)), Type)) =
        Sig(
          ArraySeq(
            Some(Name(t._1._1._1, Span.Null)) -> t._1._1._2,
            Some(Name(t._1._2._1, Span.Null)) -> t._1._2._2),
          t._2)
      implicit def t3n(t: (((String, Type), (String, Type), (String, Type)), Type)) =
        Sig(
          ArraySeq(
            Some(Name(t._1._1._1, Span.Null)) -> t._1._1._2,
            Some(Name(t._1._2._1, Span.Null)) -> t._1._2._2,
            Some(Name(t._1._3._1, Span.Null)) -> t._1._3._2),
          t._2
        )
    }

    def apply(sig: Sig): Function =
      Function(sig.params, None, sig.ret, Span.Null)
  }

  object Union {
    def apply(variants: Type*): Union = Union(variants, Span.Null)

    def apply(variants: Iterable[Type], span: Span): Union = {
      val builder = ArraySeq.newBuilder[Type]
      def go(ty: Type): Unit = ty match {
        case Type.Union(vs, _) => vs.foreach(go)
        case _                 => builder += ty
      }
      variants.foreach(go)
      new Union(builder.result(), span)
    }
  }

  final case class Union(variants: ArraySeq[Type], span: Span) extends Type {
    def move(to: Span) = copy(span = to)
    lazy val level = variants.foldLeft(Level.Zero)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      variants.forall(_.forallVars(f))

    override def toString = variants.mkString("(", " | ", ")")
  }

  object Intersect {
    def apply(variants: Type*): Intersect = Intersect(variants, Span.Null)

    def apply(variants: Iterable[Type], span: Span): Intersect = {
      val builder = ArraySeq.newBuilder[Type]
      def go(ty: Type): Unit = ty match {
        case Type.Intersect(vs, _) => vs.foreach(go)
        case _                     => builder += ty
      }
      variants.foreach(go)
      new Intersect(builder.result(), span)
    }
  }

  final case class Intersect(variants: ArraySeq[Type], span: Span) extends Type {
    def move(to: Span) = copy(span = to)
    lazy val level = variants.foldLeft(Level.Zero)(_ max _.level)
    def forallVars(f: Either[Type.Var, Type.Skolem] => Boolean) =
      variants.forall(_.forallVars(f))

    override def toString = variants.mkString("(", " & ", ")")
  }

  /** A literal can be coerced into multiple types, such as strings or ints, which
    *   this check is for.
    */
  def isValueOf(lit: Literal, ty: Type): Boolean =
    lit match {
      case Literal.False | Literal.True => ty == Boolean
      case Literal.Null                 => ty == Null
      case Literal.Float(_)             => ty == Number || ty == Double
      case Literal.Int(i) =>
        ty match {
          case Number | Double            => true
          case Int if i.isValidInt        => true
          case Long | ID if i.isValidLong => true
          case _                          => false
        }

      // TODO: check format of the string to see if it is a valid formatted id
      case Literal.Str(_) => ty == Str || ty == ID
    }

  /** Returns the primary type of a literal */
  def primaryLitType(lit: Literal, span: Span): Type =
    lit match {
      case Literal.False | Literal.True => Boolean(span)
      case Literal.Null                 => Null(span)
      case Literal.Float(_)             => Double(span)
      case Literal.Int(i) =>
        if (i.isValidInt) Int(span)
        else if (i.isValidLong) Long(span)
        else Double(span)

      // TODO: check format of the string to see if it is a valid formatted id
      case Literal.Str(_) => Str(span)
    }
}
