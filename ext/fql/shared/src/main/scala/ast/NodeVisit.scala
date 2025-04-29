package fql.ast

trait NodeVisit { self: Node =>

  // FIXME: we can more efficiently calculate this by tracking variables at
  // parse time.
  def freeVars: FreeVars = {
    var free = FreeVars.empty
    foreachVarName(n => free |= FreeVars(n.str, n.span))
    free
  }

  // FIXME: we can more efficiently calculate this by tracking variables at
  // parse time.
  def freeTypeVars: FreeVars = {
    var free = FreeVars.empty
    foreachTypeName(n => free |= FreeVars(n.str, n.span))
    free
  }

  // FIXME: we can more efficiently calculate this by tracking variables at
  // parse time.
  def freeVarsAndTypeVars: (FreeVars, FreeVars) = {
    var vars = FreeVars.empty
    var types = FreeVars.empty

    foreachName {
      case Right(v) => vars |= FreeVars(v.str, v.span)
      case Left(t)  => types |= FreeVars(t.str, t.span)
    }

    (vars, types)
  }

  def foreachVarName(f: Name => Unit, bound: Set[String] = Set.empty): Unit =
    foreachName(_.foreach(f), boundVars = bound)

  def foreachTypeName(f: Name => Unit, bound: Set[String] = Set.empty): Unit =
    foreachName(_.left.foreach(f), boundTypes = bound)

  def foreachName(
    f: Either[Name, Name] => Unit,
    boundTypes: Set[String] = Set.empty,
    boundVars: Set[String] = Set.empty): Unit = {
    var currTypes = boundTypes
    var currVars = boundVars

    visit {
      // type vars
      case (id: TypeExpr.Id, cont) if !currTypes.contains(id.name.str) =>
        f(Left(id.name))
        cont()

      case (TypeExpr.Recursive(name, _, _), cont) =>
        // Since recursive binds a name, renames within `in` should not apply.
        val prev = currTypes
        currTypes += name.str
        cont()
        currTypes = prev

      // expr vars
      case (id: Expr.Id, cont) if !currVars.contains(id.name.str) =>
        f(Right(id.name))
        cont()

      case (Expr.Lambda(ps, vari, _, _), cont) =>
        val prev = currVars
        currVars ++= ps.iterator.flatMap(_.map(_.str))
        currVars ++= vari.flatten.map(_.str)
        cont()
        currVars = prev

      case (Expr.Match(e, branches, _), _) =>
        // cont() is not called because we manually recurse
        e.foreachName(f, currTypes, currVars)
        branches.foreach { case (pat, e) =>
          e.foreachName(f, currTypes, currVars ++ pat.boundVars)
        }

      case (Expr.Project(e, bindings, _), _) =>
        // cont() is not called because we manually recurse
        e.foreachName(f, currTypes, currVars)
        bindings.foreach { case (_, e) =>
          e.foreachName(f, currTypes, currVars + Expr.This.name)
        }

      case (Expr.Block(body, _), _) =>
        // cont() is not called because we manually recurse
        val prev = currVars
        body.foreach {
          case Expr.Stmt.Let(n, t, e, _) =>
            t.foreach(_.foreachName(f, currTypes, currVars))
            e.foreachName(f, currTypes, currVars)
            currVars += n.str
          case Expr.Stmt.Expr(e) =>
            e.foreachName(f, currTypes, currVars)
        }

        currVars = prev
    }
  }

  def visit(f: PartialFunction[(Node, () => Unit), Unit]): Unit = {
    def next() =
      this match {
        // exprs
        case _: Expr.Id | _: Expr.Lit =>
        case Expr.StrTemplate(parts, _) =>
          parts.foreach(_.foreach(_.visit(f)))
        case Expr.If(p, t, _) =>
          p.visit(f)
          t.visit(f)
        case Expr.IfElse(p, t, e, _) =>
          p.visit(f)
          t.visit(f)
          e.visit(f)
        case Expr.At(ts, body, _) =>
          ts.visit(f)
          body.visit(f)
        case Expr.Match(e, branches, _) =>
          e.visit(f)
          branches.foreach { case (p, e) =>
            p.visit(f)
            e.visit(f)
          }
        case Expr.Lambda(_, _, b, _) =>
          b.visit(f)
        case Expr.OperatorCall(e, _, args, _) =>
          e.visit(f)
          args.foreach(_.visit(f))
        case Expr.MethodChain(e, chain, _) =>
          e.visit(f)
          chain.foreach {
            case Expr.MethodChain.Apply(args, _, _) =>
              args.foreach(_.visit(f))
            case Expr.MethodChain.Access(args, _, _) =>
              args.foreach(_.visit(f))
            case Expr.MethodChain.MethodCall(_, _, args, _, _, _) =>
              args.foreach(_.visit(f))
            case Expr.MethodChain.Select(_, _, _) | Expr.MethodChain.Bang(_) => ()
          }
        case Expr.Project(e, bindings, _) =>
          e.visit(f)
          bindings.foreach { case (_, e) => e.visit(f) }
        case Expr.ProjectAll(e, _) =>
          e.visit(f)
        case Expr.Object(fields, _) =>
          fields.foreach { case (_, e) => e.visit(f) }
        case Expr.Tuple(elems, _) =>
          elems.foreach(_.visit(f))
        case Expr.Array(elems, _) =>
          elems.foreach(_.visit(f))
        case Expr.Block(body, _) =>
          body.foreach(_.visit(f))

        // statements
        case Expr.Stmt.Expr(e) =>
          e.visit(f)
        case Expr.Stmt.Let(_, tpe, e, _) =>
          tpe.foreach(_.visit(f))
          e.visit(f)

        // type exprs

        case _: TypeExpr.Hole | _: TypeExpr.Any | _: TypeExpr.Never |
            _: TypeExpr.Singleton | _: TypeExpr.Id =>

        case TypeExpr.Cons(_, targs, _) =>
          targs.foreach(_.visit(f))
        case TypeExpr.Object(fields, wild, _) =>
          fields.foreach { case (_, e) => e.visit(f) }
          wild.foreach(_.visit(f))
        case TypeExpr.Interface(fields, _) =>
          fields.foreach { case (_, e) => e.visit(f) }
        case TypeExpr.Projection(proj, ret, _) =>
          proj.visit(f)
          ret.visit(f)
        case TypeExpr.Tuple(elems, _) =>
          elems.foreach(_.visit(f))
        case TypeExpr.Lambda(params, variadic, ret, _) =>
          params.foreach(_._2.visit(f))
          variadic.foreach(_._2.visit(f))
          ret.visit(f)
        case TypeExpr.Union(members, _) =>
          members.foreach(_.visit(f))
        case TypeExpr.Intersect(members, _) =>
          members.foreach(_.visit(f))
        case TypeExpr.Difference(elem, sub, _) =>
          elem.visit(f)
          sub.visit(f)
        case TypeExpr.Recursive(_, in, _) =>
          in.visit(f)
        case TypeExpr.Nullable(base, _, _) =>
          base.visit(f)

        case _: PartialTypeExpr => () // This should never show up.

        case SchemaTypeExpr.Simple(te) =>
          te.visit(f)
        case SchemaTypeExpr.Object(fs, w, _) =>
          fs.foreach { case (_, te, d) =>
            te.visit(f)
            d.foreach(_.visit(f))
          }
          w.foreach(_.visit(f))

        // pat exprs
        case PatExpr.Hole(inner, _) =>
          inner.foreach(_.visit(f))
        case PatExpr.Bind(_, inner, _) =>
          inner.foreach(_.visit(f))
        case PatExpr.Type(tpe, _) =>
          tpe.visit(f)
        case _: PatExpr.Lit =>
        case PatExpr.Object(fields, _) =>
          fields.foreach { case (_, e) => e.visit(f) }
        case PatExpr.Tuple(elems, _) =>
          elems.foreach(_.visit(f))
        case PatExpr.Array(elems, rest, _) =>
          elems.foreach(_.visit(f))
          rest.foreach(_.visit(f))

        // FSL Items
        // TODO: implement visitor for FSL item tree
        case _: FSL => ()
      }

    f.applyOrElse((this, next _), { _: (Node, () => Unit) => next() })
  }
}
