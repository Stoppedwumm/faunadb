package fql.typer

import java.util.Objects
import scala.collection.mutable.{ Set => MSet }

private[typer] trait ConstraintHelpers { self: Constraint =>
  protected def equalsImpl(o: Any): Boolean =
    (this, o) match {
      case (a, b: AnyRef) if a eq b => true

      // FIXME: This is gross, since we break structural equality here
      // (where two Lazys contain the same unevaluated type). There's no fixing
      // it here, however. We would need to remove Lazy and change how
      // typechecking query args works to use a different impl.
      case (a: Constraint.Lazy, b: Constraint.Lazy) => a.thnk == b.thnk
      case (_: Constraint.Lazy, _)                  => false

      case (a: Type.Singleton, b: Type.Singleton) => a.lit == b.lit
      case (_: Type.Singleton, _)                 => false
      case (a: Constraint.Lit, b: Constraint.Lit) => a.lit == b.lit
      case (_: Constraint.Lit, _)                 => false

      case (_: Type.Any, _: Type.Any)     => true
      case (_: Type.Any, _)               => false
      case (_: Type.Top, _: Type.Top)     => true
      case (_: Type.Top, _)               => false
      case (_: Type.Never, _: Type.Never) => true
      case (_: Type.Never, _)             => false

      case (a: Type.Var, b: Type.Var) => a.id == b.id
      case (_: Type.Var, _)           => false

      case (a: Type.Named, b: Type.Named) =>
        a.name.str == b.name.str && a.args == b.args
      case (_: Type.Named, _) => false

      case (a: Type.Skolem, b: Type.Skolem) => a.name.str == b.name.str
      case (_: Type.Skolem, _)              => false

      case (a: Type.Record, b: Type.Record) =>
        a.fields == b.fields && a.wildcard == b.wildcard
      case (_: Type.Record, _) => false
      case (a: Constraint.Rec, b: Constraint.Rec) =>
        a.fields == b.fields
      case (_: Constraint.Rec, _) => false

      case (a: Type.Tuple, b: Type.Tuple)         => a.elems == b.elems
      case (_: Type.Tuple, _)                     => false
      case (a: Constraint.Tup, b: Constraint.Tup) => a.elems == b.elems
      case (_: Constraint.Tup, _)                 => false

      case (a: Type.Union, b: Type.Union)             => a.variants == b.variants
      case (_: Type.Union, _)                         => false
      case (a: Constraint.Union, b: Constraint.Union) => a.variants == b.variants
      case (_: Constraint.Union, _)                   => false

      case (a: Type.Intersect, b: Type.Intersect) => a.variants == b.variants
      case (_: Type.Intersect, _)                 => false
      case (a: Constraint.Intersect, b: Constraint.Intersect) =>
        a.variants == b.variants
      case (_: Constraint.Intersect, _) => false

      case (a: Type.Function, b: Type.Function) =>
        a.params == b.params && a.variadic == b.variadic && a.ret == b.ret
      case (_: Type.Function, _) => false
      case (a: Constraint.Func, b: Constraint.Func) =>
        a.params == b.params && a.variadic == b.variadic && a.ret == b.ret
      case (_: Constraint.Func, _) => false
      case (a: Constraint.Apply, b: Constraint.Apply) =>
        a.args == b.args && a.ret == b.ret
      case (_: Constraint.Apply, _) => false
      case (a: Constraint.Access, b: Constraint.Access) =>
        a.args == b.args && a.ret == b.ret
      case (_: Constraint.Access, _) => false

      case (a: Constraint.Diff, b: Constraint.Diff) =>
        a.value == b.value && a.sub == b.sub
      case (_: Constraint.Diff, _) => false
      case (a: Constraint.DiffGuard, b: Constraint.DiffGuard) =>
        a.sub == b.sub && a.use == b.use
      case (_: Constraint.DiffGuard, _) => false

      case (a: Type.Interface, b: Type.Interface) =>
        a.field == b.field && a.ret == b.ret
      case (_: Type.Interface, _) => false
      case (a: Constraint.Interface, b: Constraint.Interface) =>
        a.field == b.field && a.ret == b.ret
      case (_: Constraint.Interface, _) => false

      case (a: Constraint.Proj, b: Constraint.Proj) =>
        a.proj == b.proj && a.ret == b.ret
      case (_: Constraint.Proj, _) => false
    }

  protected def hashCodeImpl() =
    this match {
      case a: Constraint.Lazy => a.thnk.hashCode

      case a: Type.Singleton => a.lit.hashCode
      case a: Constraint.Lit => a.lit.hashCode

      case _: Type.Any   => classOf[Type.Any].hashCode
      case _: Type.Top   => classOf[Type.Top].hashCode
      case _: Type.Never => classOf[Type.Never].hashCode

      case a: Type.Var => a.id.hashCode

      case a: Type.Named  => Objects.hash(a.name.str, a.args)
      case a: Type.Skolem => a.name.str.hashCode

      case a: Type.Record    => Objects.hash(a.fields, a.wildcard)
      case a: Constraint.Rec => a.fields.hashCode

      case a: Type.Tuple     => a.elems.hashCode
      case a: Constraint.Tup => a.elems.hashCode

      case a: Type.Union       => a.variants.hashCode
      case a: Constraint.Union => a.variants.hashCode

      case a: Type.Intersect       => a.variants.hashCode
      case a: Constraint.Intersect => a.variants.hashCode

      case a: Type.Function     => Objects.hash(a.params, a.variadic, a.ret)
      case a: Constraint.Func   => Objects.hash(a.params, a.variadic, a.ret)
      case a: Constraint.Apply  => Objects.hash(a.args, a.ret)
      case a: Constraint.Access => Objects.hash(a.args, a.ret)

      case a: Constraint.Diff      => Objects.hash(a.value, a.sub)
      case a: Constraint.DiffGuard => Objects.hash(a.sub, a.use)

      case a: Type.Interface       => Objects.hash(a.field, a.ret)
      case a: Constraint.Interface => Objects.hash(a.field, a.ret)

      case a: Constraint.Proj => Objects.hash(a.proj, a.ret)
    }
}

private[typer] trait ValueHelpers { self: Constraint.Value =>
  def freeTypeVars = {
    val vs = MSet.empty[String]

    def go(v: Constraint): Unit = v match {
      case Constraint.Lazy(t) => go(t)
      case _: Type.Singleton  => ()
      case _: Constraint.Lit  => ()

      case _: Type.Any   => ()
      case _: Type.Top   => ()
      case _: Type.Never => ()

      case _: Type.Var =>
        throw new IllegalStateException("cannot call freeTypeVars on a Type.Var")

      case Type.Named(n, _, args) =>
        vs += n.str
        args.foreach(go)
      case Type.Skolem(_, _) => ()

      case Type.Record(fields, wild, _) =>
        fields.foreach { case (_, v) => go(v) }
        wild.foreach(go)
      case c: Constraint.Rec => c.fields.foreach { case (_, v) => go(v) }

      case Type.Tuple(elems, _) => elems.foreach(go)
      case c: Constraint.Tup    => c.elems.foreach(go)

      case Type.Union(variants, _) =>
        variants.foreach(go)
      case c: Constraint.Union =>
        c.variants.foreach { case (_, v) => go(v) }

      case Type.Intersect(variants, _) =>
        variants.foreach(go)
      case c: Constraint.Intersect =>
        c.variants.foreach { case (_, v) => go(v) }

      case Type.Function(params, variadic, ret, _) =>
        params.foreach { case (_, u) => go(u) }
        variadic.foreach { case (_, u) => go(u) }
        go(ret)
      case c: Constraint.Func =>
        c.params.foreach { case (_, u) => go(u) }
        c.variadic.foreach { case (_, u) => go(u) }
        go(c.ret)
      case c: Constraint.Apply =>
        c.args.foreach(go)
        go(c.ret)
      case c: Constraint.Access =>
        c.args.foreach(go)
        go(c.ret)

      case d: Constraint.Diff =>
        go(d.value)
        go(d.sub)
      case d: Constraint.DiffGuard =>
        go(d.sub)
        go(d.use)

      case t: Type.Interface       => go(t.ret)
      case c: Constraint.Interface => go(c.ret)

      case Constraint.Proj(proj, ret, _) =>
        go(proj)
        go(ret)
    }

    go(this)
    vs.toSet
  }

}
