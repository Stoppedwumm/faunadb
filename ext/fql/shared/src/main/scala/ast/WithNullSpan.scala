package fql.ast

trait WithNullSpanName { self: Name =>
  def withNullSpan = Name(str, Span.Null)
}

trait WithNullSpanStmt { self: Expr.Stmt =>
  def withNullSpan: Expr.Stmt = this match {
    case Expr.Stmt.Let(name, tpe, value, _) =>
      Expr.Stmt.Let(
        name.withNullSpan,
        tpe.map(_.withNullSpan),
        value.withNullSpan,
        Span.Null)

    case Expr.Stmt.Expr(e) =>
      Expr.Stmt.Expr(e.withNullSpan)
  }
}

trait WithNullSpanExpr { self: Expr =>
  def withNullSpan: Expr = this match {
    case Expr.Id(s, _)  => Expr.Id(s, Span.Null)
    case Expr.Lit(v, _) => Expr.Lit(v, Span.Null)
    case Expr.StrTemplate(parts, _) =>
      Expr.StrTemplate(
        parts.map {
          case Left(s)  => Left(s)
          case Right(e) => Right(e.withNullSpan)
        },
        Span.Null)
    case Expr.If(pred, thn, _) =>
      Expr.If(pred.withNullSpan, thn.withNullSpan, Span.Null)
    case Expr.IfElse(pred, thn, els, _) =>
      Expr.IfElse(pred.withNullSpan, thn.withNullSpan, els.withNullSpan, Span.Null)
    case Expr.At(ts, body, _) =>
      Expr.At(ts.withNullSpan, body.withNullSpan, Span.Null)
    case Expr.Match(e, branches, _) =>
      Expr.Match(
        e.withNullSpan,
        branches.map { case (pat, e) =>
          pat.withNullSpan -> e.withNullSpan
        },
        Span.Null)
    case Expr.LongLambda(ps, v, b, _) =>
      Expr.LongLambda(
        ps.map(_.map(_.withNullSpan)),
        v.map(_.map(_.withNullSpan)),
        b.withNullSpan,
        Span.Null)
    case Expr.ShortLambda(e) => Expr.ShortLambda(e.withNullSpan)

    case Expr.OperatorCall(e, field, args, _) =>
      Expr.OperatorCall(
        e.withNullSpan,
        field.withNullSpan,
        args.map(_.withNullSpan),
        Span.Null)
    case Expr.MethodChain(e, chain, _) =>
      Expr.MethodChain(e.withNullSpan, chain.map(_.withNullSpan), Span.Null)

    case Expr.Project(e, bindings, _) =>
      Expr.Project(
        e.withNullSpan,
        bindings.map { case (name, expr) => name.withNullSpan -> expr.withNullSpan },
        Span.Null)

    case Expr.ProjectAll(e, _) =>
      Expr.ProjectAll(e.withNullSpan, Span.Null)

    case Expr.Object(fields, _) =>
      Expr.Object(
        fields.map { case (name, expr) => name.withNullSpan -> expr.withNullSpan },
        Span.Null)

    case Expr.Tuple(elems, _) =>
      Expr.Tuple(elems.map(_.withNullSpan), Span.Null)

    case Expr.Array(elems, _) =>
      Expr.Array(elems.map(_.withNullSpan), Span.Null)

    case Expr.Block(elems, _) =>
      Expr.Block(elems.map(_.withNullSpan), Span.Null)
  }
}

trait WithNullSpanMethodChain { self: Expr.MethodChain.Component =>
  def withNullSpan: Expr.MethodChain.Component = this match {
    case Expr.MethodChain.Select(_, field, optional) =>
      Expr.MethodChain.Select(Span.Null, field.withNullSpan, optional)

    case Expr.MethodChain.Bang(_) => Expr.MethodChain.Bang(Span.Null)

    case Expr.MethodChain.Apply(args, optional, _) =>
      Expr.MethodChain.Apply(
        args.map(_.withNullSpan),
        optional.map(_ => Span.Null),
        Span.Null)

    case Expr.MethodChain.MethodCall(
          _,
          field,
          args,
          selectOptional,
          applyOptional,
          _) =>
      Expr.MethodChain.MethodCall(
        Span.Null,
        field.withNullSpan,
        args.map(_.withNullSpan),
        selectOptional,
        applyOptional.map(_ => Span.Null),
        Span.Null)

    case Expr.MethodChain.Access(args, optional, _) =>
      Expr.MethodChain.Access(
        args.map(_.withNullSpan),
        optional.map(_ => Span.Null),
        Span.Null)
  }
}

trait WithNullSpanPatExpr { self: PatExpr =>
  def withNullSpan: PatExpr = this match {
    case PatExpr.Hole(inner, _) => PatExpr.Hole(inner.map(_.withNullSpan), Span.Null)
    case PatExpr.Bind(name, inner, _) =>
      PatExpr.Bind(name.withNullSpan, inner.map(_.withNullSpan), Span.Null)

    case PatExpr.Type(tpe, _) => PatExpr.Type(tpe.withNullSpan, Span.Null)
    case PatExpr.Lit(lit, _)  => PatExpr.Lit(lit, Span.Null)

    case PatExpr.Object(fields, _) =>
      PatExpr.Object(
        fields.map { case (name, expr) => name.withNullSpan -> expr.withNullSpan },
        Span.Null)
    case PatExpr.Tuple(elems, _) =>
      PatExpr.Tuple(elems.map(_.withNullSpan), Span.Null)
    case PatExpr.Array(elems, rest, _) =>
      PatExpr.Array(elems.map(_.withNullSpan), rest.map(_.withNullSpan), Span.Null)
  }
}

trait WithNullSpanTypeExpr { self: TypeExpr =>
  def withNullSpan: TypeExpr = this match {
    case TypeExpr.Hole(_)  => TypeExpr.Hole(Span.Null)
    case TypeExpr.Any(_)   => TypeExpr.Any(Span.Null)
    case TypeExpr.Never(_) => TypeExpr.Never(Span.Null)

    case TypeExpr.Singleton(lit, _) => TypeExpr.Singleton(lit, Span.Null)
    case TypeExpr.Id(str, _)        => TypeExpr.Id(str, Span.Null)
    case TypeExpr.Cons(name, targs, _) =>
      TypeExpr.Cons(name.withNullSpan, targs.map(_.withNullSpan), Span.Null)
    case TypeExpr.Object(fields, wildcard, _) =>
      TypeExpr.Object(
        fields.map { case (name, expr) => name.withNullSpan -> expr.withNullSpan },
        wildcard.map(_.withNullSpan),
        Span.Null)

    case TypeExpr.Interface(fields, _) =>
      TypeExpr.Interface(
        fields.map { case (name, expr) => name.withNullSpan -> expr.withNullSpan },
        Span.Null)

    case TypeExpr.Projection(proj, ret, _) =>
      TypeExpr.Projection(proj.withNullSpan, ret.withNullSpan, Span.Null)

    case TypeExpr.Tuple(elems, _) =>
      TypeExpr.Tuple(elems.map(_.withNullSpan), Span.Null)

    case TypeExpr.Lambda(params, variadic, ret, _) =>
      TypeExpr.Lambda(
        params.map { case (name, expr) =>
          name.map(_.withNullSpan) -> expr.withNullSpan
        },
        variadic.map { case (name, expr) =>
          name.map(_.withNullSpan) -> expr.withNullSpan
        },
        ret.withNullSpan,
        Span.Null
      )

    case TypeExpr.Union(members, _) =>
      TypeExpr.Union(members.map(_.withNullSpan), Span.Null)
    case TypeExpr.Intersect(members, _) =>
      TypeExpr.Intersect(members.map(_.withNullSpan), Span.Null)

    case TypeExpr.Difference(elem, sub, _) =>
      TypeExpr.Difference(elem.withNullSpan, sub.withNullSpan, Span.Null)

    case TypeExpr.Recursive(name, in, _) =>
      TypeExpr.Recursive(name.withNullSpan, in.withNullSpan, Span.Null)

    case TypeExpr.Nullable(base, _, _) =>
      TypeExpr.Nullable(base.withNullSpan, Span.Null, Span.Null)
  }
}

trait WithNullSpanSchemaTypeExpr { self: SchemaTypeExpr =>
  def withNullSpan: SchemaTypeExpr = this match {
    case SchemaTypeExpr.Simple(te) => SchemaTypeExpr.Simple(te.withNullSpan)
    case SchemaTypeExpr.Object(fields, wildcard, _) =>
      SchemaTypeExpr.Object(
        fields.map { case (name, expr, default) =>
          (name.withNullSpan, expr.withNullSpan, default.map(_.withNullSpan))
        },
        wildcard.map(_.withNullSpan),
        Span.Null)
  }
}

trait WithNullSpanMigrationItem { self: MigrationItem =>
  def withNullSpan: MigrationItem = this match {
    case MigrationItem.Backfill(path, FSL.Lit(expr), _) =>
      MigrationItem.Backfill(
        path.copy(span = Span.Null),
        FSL.Lit(expr.withNullSpan),
        Span.Null)
    case MigrationItem.Drop(field, _) =>
      MigrationItem.Drop(field.withNullSpan, Span.Null)
    case MigrationItem.Split(from, to, _) =>
      MigrationItem.Split(from.withNullSpan, to.map { _.withNullSpan }, Span.Null)
    case MigrationItem.Move(field, to, _) =>
      MigrationItem.Move(field.withNullSpan, to.withNullSpan, Span.Null)
    case MigrationItem.Add(field, _) =>
      MigrationItem.Add(field.withNullSpan, Span.Null)
    case MigrationItem.MoveWildcardConflicts(into, _) =>
      MigrationItem.MoveWildcardConflicts(into.withNullSpan, Span.Null)
    case MigrationItem.MoveWildcard(into, _) =>
      MigrationItem.MoveWildcard(into.withNullSpan, Span.Null)
    case MigrationItem.AddWildcard(_) =>
      MigrationItem.AddWildcard(Span.Null)
  }
}

trait WithNullSpanPath { self: Path =>
  def withNullSpan: Path = Path(
    elems.map {
      case PathElem.Field(name, _)  => PathElem.Field(name, Span.Null)
      case PathElem.Index(index, _) => PathElem.Index(index, Span.Null)
    },
    Span.Null)
}
