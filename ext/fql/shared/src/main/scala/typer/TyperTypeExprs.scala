package fql.typer

import fql.ast.{ Name, Span, TypeExpr }
import fql.error.{ TypeError, Warning }
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.collection.mutable.{ Builder, Map => MMap }

object TyperTypeExprs {
  lazy val TSTypeNames: Map[String, String] =
    Seq("Any", "Null", "Number", "String", "Boolean")
      .map(a => a.toLowerCase -> a)
      .toMap
}

trait TyperTypeExprs { self: Typer =>

  /** Types a TypeExpr. Primarily, this is all about validating that type
    * identifiers are real and that type constructors are supplied with the
    * correct # of arguments.
    *
    * If checked = false, this assumes that the type is "correct" and always
    * returns a successful Typecheck.
    */
  protected def typeTExpr0(
    expr: TypeExpr,
    checked: Boolean,
    allowGenerics: Boolean,
    allowVariables: Boolean
  )(
    implicit tctx: Typer.TVarCtx,
    containerAlt: Alt,
    level: Type.Level,
    pol: Boolean): (Type, List[TypeError]) = {

    val fails = List.newBuilder[TypeError]

    def go(
      expr: TypeExpr
    )(implicit tctx: Typer.TVarCtx, pol: Boolean): Type =
      expr match {
        case TypeExpr.Hole(span) =>
          if (allowVariables) {
            freshVar(span)
          } else {
            fails += TypeError.InvalidHoleType(span)
            Type.Any(span)
          }

        case TypeExpr.Any(span)          => Type.Any(span)
        case TypeExpr.Never(span)        => Type.Never(span)
        case TypeExpr.Singleton(v, span) => Type.Singleton(v, span)

        case id: TypeExpr.Id =>
          if (checked) {
            typeTCons(id.name, ArraySeq.empty, id.span, allowGenerics, fails)
          } else {
            tctx.getOrElse(id.str, Type.Named(id.name, id.span, ArraySeq.empty))
          }

        case TypeExpr.Cons(name, args, span) =>
          val args0 = args.view
            .map(go)
            .to(ArraySeq)
          if (checked) {
            typeTCons(name, args0, span, allowGenerics, fails)
          } else {
            Type.Named(name, span, args0)
          }

        case TypeExpr.Object(fields, wild, span) =>
          val fields0 = fields.view
            .map { case (n, e) =>
              n.str -> go(e)
            }
            .to(SeqMap)
          val wild0 = wild.map(go)
          Type.Record(fields0, wild0.headOption, span)

        case TypeExpr.Interface(fields, span) =>
          val fields0 = fields.view
            .map { case (n, e) =>
              val ty = go(e)
              Type.Interface(n.str, ty, span)
            }
            .to(ArraySeq)

          if (fields0.sizeIs == 1) {
            fields0.head
          } else {
            Type.Intersect(fields0, span)
          }

        case TypeExpr.Projection(_, _, _) => sys.error("Unimplemented")

        case TypeExpr.Tuple(elems, span) =>
          val elems0 =
            elems.view
              .map(go)
              .to(ArraySeq)
          Type.Tuple(elems0, span)

        case TypeExpr.Lambda(params, variadic, ret, span) =>
          val params0 = params.view
            .map { p =>
              p._1 -> go(p._2)(tctx, !pol)
            }
            .to(ArraySeq)

          val variadic0 = variadic.map { v =>
            v._1 -> go(v._2)(tctx, !pol)
          }

          val ret0 = go(ret)

          Type.Function(params0, variadic0, ret0, span)

        case TypeExpr.Union(members, span) =>
          val members0 =
            members.view
              .map(go)
              .to(ArraySeq)
          Type.Union(members0, span)

        case TypeExpr.Intersect(members, span) =>
          val members0 =
            members.view
              .map(go)
              .to(ArraySeq)
          Type.Intersect(members0, span)

        // FIXME: we have a Diff constriant, but not a Difference type
        case TypeExpr.Difference(_, _, _) => sys.error("Unimplemented")

        case TypeExpr.Recursive(name, in, span) =>
          if (allowVariables) {
            // FIXME: using a fresh var is likely not what we want, since the
            // var can accumulate constraints that may not be appropriate, and allow
            // type checks to pass that would otherwise fail if the recursive var
            // were more opaque. OTOH, it's attached to the recursive def, so maybe
            // it isn't an issue.
            val ret = freshVar(span)
            val ty = go(in)(tctx + (name.str -> ret), pol)
            // FIXME: test strategy of using polarity here
            val tc = {
              implicit val free: Typer.VarCtx = Map.empty
              if (pol) constrain(ret, ty, span) else constrain(ty, ret, span)
            }
            assert(!tc.isFailed)
            ty
          } else {
            fails += TypeError.InvalidRecursiveType(span)
            Type.Any(span)
          }

        case TypeExpr.Nullable(base, qmarkSpan, span) =>
          val ty = go(base)
          Type.Union(ArraySeq(ty, Type.Null(qmarkSpan)), span)
      }

    val ty = go(expr)

    if (checked && allowGenerics) {
      fails ++= checkUnboundSkolems(ty, tctx)
    }

    (ty, fails.result())
  }

  private def typeTCons(
    name: Name,
    args: ArraySeq[Type],
    span: Span,
    allowGenerics: Boolean,
    fails: Builder[TypeError, List[TypeError]])(
    implicit tctx: Typer.TVarCtx,
    level: Type.Level): Type = {
    def shapes = typeShapes.get(name.str) flatMap { shape =>
      if (shape.tparams.size != args.size) {
        fails += TypeError
          .InvalidTypeArity(name.span, name.str, shape.tparams.size, args.size)
        Some(Type.Any(name.span))
      } else if (Set(Type.Ref.name, Type.EmptyRef.name) contains name.str) {
        // Special case enforcing D <: a document type in Ref<D> and EmptyRef<D>.
        // TODO: Remove the special case if we add the ability to bound parameters.
        args.head match {
          case Type.Named(Name(arg, _), _, _) =>
            typeShapes.get(arg) map { argShape =>
              if (argShape.docType.isDoc) {
                Type.Named(name, span, args)
              } else {
                // See below about span choice. Same here for consistency.
                fails += TypeError.RefToNonDoc(span)
                Type.Any(span)
              }
            }

          // Skolems will show up if a generic type is used in a `Ref`, which is
          // disallowed.
          case Type.Skolem(Name(name, span), _) =>
            fails += TypeError.UnboundTypeVariable(span, name)
            Some(Type.Any(span))

          case _ =>
            // Use the span of the whole Ref because an unbound parameter
            // replaces the arg with a span-less Any.
            fails += TypeError.RefToNonDoc(span)
            Some(Type.Any(span))
        }
      } else {
        Some(Type.Named(name, span, args))
      }
    }

    def context = tctx.get(name.str) map { ty =>
      if (args.sizeIs > 0) {
        fails += TypeError.InvalidTypeArity(name.span, name.str, 1, args.size)
        Type.Any(name.span)
      } else {
        ty
      }
    }

    context.orElse(shapes).getOrElse {
      if (args.isEmpty && allowGenerics) {
        Type.Skolem(name, level)
      } else {
        fails += TypeError.UnboundTypeVariable(name.span, name.str)
        Type.Any(name.span)
      }
    }
  }

  class Bounds {
    var hasNegOcc: Boolean = false
    var hasPosOcc: Boolean = false
    var occs = List.empty[Type.Skolem]
  }

  private def checkUnboundSkolems(ty: Type, tctx: Typer.TVarCtx): List[TypeError] = {
    val bounds = MMap.empty[String, Bounds]

    def add(s: Type.Skolem, pol: Boolean) = {
      val b = bounds.getOrElseUpdate(s.name.str, new Bounds)
      if (pol) b.hasPosOcc = true else b.hasNegOcc = true
      b.occs :+= s
    }

    def go(ty: Type)(implicit pol: Boolean): Unit =
      ty match {
        case _: Type.Var           => {}
        case _: Type.UnleveledType => {}

        case s: Type.Skolem =>
          // skolems in tctx are exempt
          if (!tctx.contains(s.name.str)) add(s, pol)

        case f: Type.Function =>
          f.params.foreach { ty => go(ty._2)(!pol) }
          f.variadic.foreach { ty => go(ty._2)(!pol) }
          go(f.ret)

        case Type.Named(_, _, args)    => args.foreach(go)
        case Type.Tuple(elems, _)      => elems.foreach(go)
        case Type.Interface(_, ret, _) => go(ret)
        case Type.Record(fields, wildcard, _) =>
          fields.foreach(v => go(v._2))
          wildcard.foreach(go)

        case Type.Intersect(elems, _) => elems.foreach(go)
        case Type.Union(elems, _)     => elems.foreach(go)
      }

    go(ty)(true)

    // All skolems must have a negative bound
    bounds.view.flatMap { case (name, b) =>
      if (b.hasPosOcc && b.hasNegOcc) {
        None
      } else {
        b.occs.foreach(_.markInvalid())

        // TODO: do not make an exception for lowercase TS-like types
        TyperTypeExprs.TSTypeNames.get(name) match {
          case Some(actual) =>
            emit(
              Warning(
                s"Unknown type `$name`. Did you mean `$actual`?",
                b.occs.head.span))
            None
          case None =>
            Some(TypeError.UnboundTypeVariable(b.occs.head.span, name))
        }
      }
    }.toList
  }
}
