package fql.analyzer

import fql.ast.{ Expr, Literal, Span, TypeExpr }
import fql.ast.Expr.MethodChain
import fql.parser.Parser
import fql.typer.{ Constraint, Type, TypeScheme, TypeShape, Typer }
import fql.Result
import scala.collection.mutable.{ Map => MMap }

/** Stores the result of `walk()`. This includes the identifier under the cursor,
  * which is required by the webshell, to know what text to replace.
  *
  * If there is no identifier under the cursor, `identSpan` is None, and we simply
  * insert text instead.
  */
case class PartialCompletionResult(
  candidates: Seq[PartialCompletionItem],
  expected: Type = Type.Any) {
  def expect(ty: Type) = copy(expected = ty)

  def prioritizedCandidates(typer: Typer, identSpan: Span) = {
    val sorted = candidates.sorted {
      (a: PartialCompletionItem, b: PartialCompletionItem) =>
        {
          val aPrio = a.priority(typer, expected)
          val bPrio = b.priority(typer, expected)
          if (aPrio == bPrio) {
            a.label.compareTo(b.label)
          } else {
            // sort backwards, we want high priority things at the start of the list.
            bPrio - aPrio
          }
        }
    }

    sorted.map { partial =>
      CompletionItem(
        label = partial.label,
        detail = partial.detail,
        span = identSpan,
        replaceText = partial.replaceText,
        newCursor = partial.newCursor(identSpan),
        kind = partial.kind,
        retrigger = partial.retrigger,
        snippet = partial.snippet
      )
    }
  }
}

object PartialCompletionResult {
  val empty = PartialCompletionResult(Seq.empty)
}

sealed trait CompletionKind

object CompletionKind {
  object Keyword extends CompletionKind
  object Module extends CompletionKind
  object Type extends CompletionKind
  object Variable extends CompletionKind
  object Function extends CompletionKind
  object Field extends CompletionKind
  object Property extends CompletionKind
}

trait QueryContextCompletions extends ContextWalk { self: QueryContext =>
  def completionsImpl(cursor: Int, identSpan: Span): Seq[CompletionItem] = {
    ast match {
      case Some(ast) =>
        val res = walk(ast, Type.Any)(cursor, MMap.empty)
        res.prioritizedCandidates(this.typer, identSpan)
      case None =>
        // try re-parsing with an identifier inserted at the cursor
        val modified = query.patch(cursor, "faunaRulezz", 0)
        Parser.queryForCompletions(modified) match {
          case Result.Ok(ast) =>
            // if this worked, we need to re-typecheck, and then
            // complete with this ast.
            val typer = Typer(globals, types)
            typer.recordingSpans = true
            typer.typeExpr(ast) match {
              case Result.Ok(_)     => Seq.empty
              case Result.Err(errs) => errs
            }
            // make a new context, with the correct typer
            val copied = this.copy()
            copied.typer = typer
            val res = copied.walk(ast, Type.Any)(cursor, MMap.empty)
            // note: this is the local `typer`, not the field `this.typer`
            res.prioritizedCandidates(typer, identSpan)

          case Result.Err(_) =>
            val res = TopLevelCompletions(MMap.empty)
            res.prioritizedCandidates(this.typer, identSpan)
        }
    }
  }
}

// Can walk through FQL expressions and provide completions. Requires a typer to
// use throughout the entire AST walk.
trait ContextWalk {
  def globals: Map[String, TypeScheme]
  def types: Map[String, TypeShape]
  def typer: Typer

  def Keywords =
    Seq(
      PartialCompletionItem("if", CompletionKind.Keyword),
      PartialCompletionItem("at", CompletionKind.Keyword),
      PartialCompletionItem("let", CompletionKind.Keyword),
      PartialCompletionItem(typer, "true", Type.Boolean, CompletionKind.Keyword),
      PartialCompletionItem(typer, "false", Type.Boolean, CompletionKind.Keyword),
      PartialCompletionItem(typer, "null", Type.Null, CompletionKind.Keyword)
    )
  def Modules =
    globals.map { case (label, ty) =>
      PartialCompletionItem(typer, label, ty, CompletionKind.Module)
    }
  def TopLevelCandidates = Keywords ++ Modules
  def TopLevelCompletions(implicit vctx: VarCtx) = PartialCompletionResult(
    TopLevelCandidates ++ vctx.map { case (name, ty) =>
      PartialCompletionItem(typer, name, ty, CompletionKind.Variable)
    }
  )

  type VarCtx = MMap[String, Constraint.Value]

  def walk(expr: Expr, expected: Type)(
    implicit cursor: Int,
    vctx: VarCtx): PartialCompletionResult = {
    expr match {
      case Expr.Id(_, _) => TopLevelCompletions.expect(expected)
      case Expr.Lit(Literal.Null | Literal.True | Literal.False, _) =>
        TopLevelCompletions.expect(expected)
      // string, int, and float don't have completions
      case Expr.Lit(_, _) => PartialCompletionResult.empty
      case Expr.StrTemplate(parts, _) =>
        parts.find {
          case Right(expr) if expr.span.contains(cursor) => true
          case _                                         => false
        } match {
          case Some(Right(expr)) => walk(expr, Type.Any)
          case _                 => PartialCompletionResult.empty
        }

      case Expr.If(cond, then, _) =>
        if (cond.span.contains(cursor)) {
          walk(cond, Type.Boolean)
        } else if (then.span.contains(cursor)) {
          walk(then, Type.Any)
        } else if (cursor > then.span.end) {
          PartialCompletionResult(
            TopLevelCandidates ++ Seq(
              PartialCompletionItem("else", CompletionKind.Keyword)))
        } else {
          TopLevelCompletions
        }
      case Expr.At(ts, then, _) =>
        if (ts.span.contains(cursor)) {
          walk(ts, Type.Time)
        } else if (then.span.contains(cursor)) {
          walk(then, Type.Any)
        } else {
          TopLevelCompletions
        }
      case Expr.IfElse(cond, then, els, _) =>
        if (cond.span.contains(cursor)) {
          walk(cond, Type.Boolean)
        } else if (then.span.contains(cursor)) {
          walk(then, Type.Any)
        } else if (els.span.contains(cursor)) {
          walk(els, Type.Any)
        } else if (cursor > then.span.end) {
          PartialCompletionResult(
            TopLevelCandidates ++ Seq(
              PartialCompletionItem("else", CompletionKind.Keyword)))
        } else {
          TopLevelCompletions
        }
      case Expr.Match(_, _, _) => sys.error("todo")

      case Expr.LongLambda(args, vari, body, _) =>
        (args ++ vari).find {
          _.exists(name => cursor >= name.span.start && cursor <= name.span.end)
        } match {
          case Some(_) =>
            PartialCompletionResult.empty
          case _ =>
            if (body.span.contains(cursor)) {
              walk(body, Type.Any)(
                cursor,
                vctx.clone() ++ args.flatMap {
                  _.map { arg =>
                    arg.str -> typer.typeAt(arg.span).getOrElse(Type.Never)
                  }
                })
            } else {
              TopLevelCompletions
            }
        }
      case Expr.ShortLambda(body) =>
        if (body.span.contains(cursor)) {
          walk(body, Type.Any)
        } else {
          TopLevelCompletions
        }

      case Expr.OperatorCall(lhs, field, args, _) =>
        if (lhs.span.contains(cursor)) {
          // FIXME: Use the op type in the typer to hint at the right type.
          // It'll be a super minor improvement but nice to have.
          walk(lhs, Type.Any)
        } else if (cursor <= field.span.start) {
          // if your cursor is here, don't complete anything:
          // 1 |+ 2
          PartialCompletionResult.empty
        } else {
          args match {
            case Some(arg) if arg.span.contains(cursor) => walk(arg, Type.Any)
            case _                                      => TopLevelCompletions
          }
        }
      case Expr.MethodChain(lhs, chain, _) =>
        if (lhs.span.contains(cursor)) {
          walk(lhs, Type.Any)
        } else {
          walk(lhs.span, chain, expected)
        }

      case Expr.Project(lhs, bindings, _) =>
        def projectionFields(span: Span) = {
          completionsFromFields(
            typer
              .typeAt(span)
              .map(typer.projectionFieldsOfValue(_))
              .getOrElse(Map.empty))
        }

        if (lhs.span.contains(cursor)) {
          walk(lhs, Type.Any)
        } else {
          bindings.findLast { case (name, _) => cursor >= name.span.start } match {
            case Some((name, _)) if name.span.contains(cursor) =>
              projectionFields(lhs.span)
            case Some((_, field)) => walk(field, Type.Any)
            // cursor is elsewhere, so just fall back to the projection fields of
            // lhs.
            case None =>
              projectionFields(lhs.span)
          }
        }

      case Expr.ProjectAll(_, _) => sys.error("todo")

      case Expr.Object(fields, _) =>
        // empty blocks show up as empty objects, so return top level. However, if
        // the expected type is an object, we fall through to the object case anyway.
        if (fields.isEmpty && !expected.isInstanceOf[Type.Record]) {
          TopLevelCompletions
        } else {
          fields.findLast { case (name, expr) =>
            cursor >= name.span.start || cursor >= expr.span.start
          } match {
            case Some((name, _)) if name.span.contains(cursor) =>
              fieldsOfRecord(expected, fields.map(_._1.str), name.str)
            case Some((name, field)) if field.span.contains(cursor) =>
              // Now that we're completing the field `name`, we select that field out
              // of the expected type when recursing. This causes completions on
              // nested records to work.
              def go(ty: Type): Option[Type] = ty match {
                case Type.Record(fields, wild, _) =>
                  fields.get(name.str).orElse(wild)
                case Type.Union(types, _) =>
                  val inner = types.flatMap(go)
                  inner.length match {
                    case 0 => None
                    case 1 => Some(inner.head)
                    case _ => Some(Type.Union(inner, Span.Null))
                  }
                case _ => None
              }

              walk(field, go(expected).getOrElse(Type.Any))

            // this means the cursor is between a field and expression (same as
            // before the expression). so, complete top level.
            case Some((_, field)) if cursor < field.span.start => TopLevelCompletions
            // about to type a field name
            case _ if fields.isEmpty =>
              fieldsOfRecord(expected, fields.map(_._1.str))
            // assume the user typed a comma and they're about to type another field
            case _ if cursor > fields.last._2.span.start =>
              fieldsOfRecord(expected, fields.map(_._1.str))
            // cursor is elsewhere (likely after an expression and before a name), so
            // just fall back to nothing.
            case _ => PartialCompletionResult.empty
          }
        }

      case Expr.Tuple(elems, _) =>
        elems.find { _.span.contains(cursor) } match {
          case Some(expr) => walk(expr, Type.Any)
          case None       => TopLevelCompletions
        }
      case Expr.Array(elems, _) =>
        elems.find { _.span.contains(cursor) } match {
          case Some(expr) => walk(expr, Type.Any)
          case None       => TopLevelCompletions
        }

      case Expr.Block(stmts, _) =>
        // handle completing the first field of a record
        if (expected.isInstanceOf[Type.Record] && stmts.length == 1) {
          stmts.head match {
            case Expr.Stmt.Expr(Expr.Id(_, _)) => return fieldsOfRecord(expected)
            case _                             => ()
          }
        }

        val innerCtx = vctx.clone()
        var i = 0
        var ret: Option[PartialCompletionResult] = None

        while (i < stmts.length && ret.isEmpty) {
          val stmt = stmts(i)
          if (i == stmts.length - 1 || cursor < stmts(i + 1).span.start) {
            ret = Some(walk(stmt)(cursor, innerCtx))
          }
          stmt match {
            case Expr.Stmt.Expr(_) =>
            case Expr.Stmt.Let(name, sig, value, _) =>
              sig.flatMap(typer.typeTExpr(_).toOption) match {
                case Some(ty) => innerCtx.put(name.str, ty.raw)
                case None =>
                  innerCtx.put(
                    name.str,
                    typer.typeAt(value.span).getOrElse(Type.Never))
              }
          }
          i += 1
        }
        ret.getOrElse(TopLevelCompletions)
    }
  }

  def walk(
    stmt: Expr.Stmt)(implicit cursor: Int, vctx: VarCtx): PartialCompletionResult = {
    stmt match {
      case Expr.Stmt.Expr(expr) => walk(expr, Type.Any)
      case Expr.Stmt.Let(name, tpe, value, _) =>
        if (cursor < name.span.start) {
          TopLevelCompletions
        } else if (name.span.contains(cursor)) {
          PartialCompletionResult.empty
        } else {
          tpe match {
            case Some(tpe) if cursor <= tpe.span.end => walk(tpe)
            case _ =>
              if (value.span.contains(cursor)) {
                walk(value, Type.Any)
              } else {
                // FIXME: If cursor < the `=` return Seq.empty instead.
                TopLevelCompletions
              }
          }
        }
    }
  }

  def KeywordTypes =
    Seq(
      PartialCompletionItem(typer, "true", Type.Boolean, CompletionKind.Keyword),
      PartialCompletionItem(typer, "false", Type.Boolean, CompletionKind.Keyword),
      PartialCompletionItem(typer, "null", Type.Null, CompletionKind.Keyword)
    )
  def BuiltinTypes =
    Seq(Type.Never(Span.Null), Type.Any(Span.Null))
      .map(v => PartialCompletionItem(typer, v.toString, v, CompletionKind.Type))
  def Types = types
    .map { case (k, v) =>
      PartialCompletionItem(typer, k, v.self, CompletionKind.Type)
    }
    .toSeq
    .sortBy(_.label)
  def TypeCandidates = Types ++ BuiltinTypes ++ KeywordTypes
  def TypeCompletions =
    PartialCompletionResult(TypeCandidates)

  def walk(
    te: TypeExpr)(implicit cursor: Int, vctx: VarCtx): PartialCompletionResult = {
    val _ = (cursor, vctx)
    te match {
      case TypeExpr.Id(_, _) => TypeCompletions
      case TypeExpr.Singleton(Literal.Null | Literal.True | Literal.False, _) =>
        TypeCompletions
      case TypeExpr.Singleton(_, _) => PartialCompletionResult.empty

      case TypeExpr.Cons(name, targs, _) =>
        if (name.span.contains(cursor)) {
          TypeCompletions
        } else {
          targs.find { _.span.contains(cursor) } match {
            case Some(targ) => walk(targ)
            // FIXME: If we know the span of `>` this could be more accurate.
            case None => TypeCompletions
          }
        }

      case TypeExpr.Object(fields, wildcard, _) =>
        fields.findLast { case (name, expr) =>
          cursor >= name.span.start || cursor >= expr.span.start
        } match {
          case Some((name, _)) if name.span.contains(cursor) =>
            PartialCompletionResult.empty
          case Some((_, field)) if field.span.contains(cursor) =>
            walk(field)
          case Some((_, field)) if cursor < field.span.start =>
            TypeCompletions
          case _ =>
            wildcard match {
              case Some(wild) if wild.span.contains(cursor) => walk(wild)
              case _                                        => TypeCompletions
            }
        }
      case TypeExpr.Interface(fields, _) =>
        fields.findLast { case (name, expr) =>
          cursor >= name.span.start || cursor >= expr.span.start
        } match {
          case Some((name, _)) if name.span.contains(cursor) =>
            PartialCompletionResult.empty
          case Some((_, field)) if field.span.contains(cursor) => walk(field)
          case Some((_, field)) if cursor < field.span.start   => TypeCompletions
          case _ => PartialCompletionResult.empty
        }
      case TypeExpr.Tuple(elems, _) =>
        elems.find { _.span.contains(cursor) } match {
          case Some(te) => walk(te)
          case None     => TypeCompletions
        }

      case TypeExpr.Lambda(params, variadic, ret, _) =>
        params.find { _._2.span.contains(cursor) } match {
          case Some((_, te)) => walk(te)
          case None =>
            variadic match {
              case Some(v) if v._2.span.contains(cursor) => walk(v._2)
              case _ =>
                if (ret.span.contains(cursor)) {
                  walk(ret)
                } else {
                  TypeCompletions
                }
            }
        }

      case _ => TypeCompletions
    }
  }

  /** `expected` is the expected return type of the _entire_ method chain.
    */
  private def walk(lhs: Span, mc: Seq[MethodChain.Component], expected: Type)(
    implicit cursor: Int,
    vctx: VarCtx): PartialCompletionResult = {
    val index = mc.indexWhere { mc => mc.span.contains(cursor) }

    if (index == -1) {
      return TopLevelCompletions
    }

    // check if we're in args, and return early if we are
    mc(index) match {
      case component @ MethodChain.Apply(args, _, applySpan)
          // this span is shortened, because we want to check if the cursor is within
          // the parens, and the span represents the outside of the parens.
          if applySpan
            .copy(start = applySpan.start + 1, end = applySpan.end - 1)
            .contains(cursor) =>
        return walkApply(component.span, args)

      case MethodChain.MethodCall(_, field, args, _, _, applySpan)
          if applySpan
            .copy(start = applySpan.start + 1, end = applySpan.end - 1)
            .contains(cursor) =>
        return walkApply(field.span, args)

      // these cases are dumb, return nothing

      case MethodChain.MethodCall(_, _, _, _, _, applySpan)
          if cursor >= applySpan.end =>
        return PartialCompletionResult.empty
      case MethodChain.Bang(_) => return PartialCompletionResult.empty

      case _ => ()
    }

    val lhsSpan = if (index == 0) {
      lhs
    } else {
      mc(index - 1).span
    }
    val res = completionsFromFields(
      typer
        .typeAt(lhsSpan)
        .map(typer.fieldsOfValue(_))
        .getOrElse(Map.empty))

    res.expect(if (index == mc.length - 1) expected else Type.Any)
  }

  private def walkApply(span: Span, args: Seq[Expr])(
    implicit cursor: Int,
    vctx: VarCtx) = {
    val argTypes = typer.typeAt(span).flatMap(typer.paramTypesOf)
    args.lastIndexWhere { cursor >= _.span.start } match {
      // edge case for completing arguments like `foo(|)`
      case _ if args.isEmpty =>
        val argType = argTypes.flatMap(_.headOption) match {
          case Some(te) =>
            // Make an empty typescheme, because let's hope that there aren't any
            // generics in `te` :)
            val sch = TypeExpr.Scheme(Seq.empty, te)
            typer.typeTSchemeUncheckedType(sch)
          case None => Type.Any
        }
        TopLevelCompletions.expect(argType)

      // edge case for completing the next argument, like `foo(1, |)`. its not quite
      // right because we don't know if theres a trailing comma or not, but its the
      // best we can do with the current AST.
      case index if index == args.length - 1 && !args.last.span.contains(cursor) =>
        val argType = argTypes.flatMap(_.lift(index + 1)) match {
          case Some(te) =>
            val sch = TypeExpr.Scheme(Seq.empty, te)
            typer.typeTSchemeUncheckedType(sch)
          case None => Type.Any
        }
        TopLevelCompletions.expect(argType)

      // handle completing arguments normally
      case index if index >= 0 =>
        val argType = argTypes.flatMap(_.lift(index)) match {
          case Some(te) =>
            val sch = TypeExpr.Scheme(Seq.empty, te)
            typer.typeTSchemeUncheckedType(sch)
          case None => Type.Any
        }
        walk(args(index), argType)

      case _ =>
        TopLevelCompletions
    }
  }

  def completionsFromFields(fields: Map[String, Constraint.Value]) =
    PartialCompletionResult(fields.map {
      case (name, v) => {
        PartialCompletionItem(
          typer,
          name,
          v,
          te =>
            if (PartialCompletionItem.isTypeExprFunction(te)) {
              CompletionKind.Function
            } else {
              CompletionKind.Field
            })
      }
    }.toSeq)

  def fieldsOfRecord(
    ty: Type,
    existing: Seq[String] = Seq.empty,
    completing: String = "") =
    ty match {
      case Type.Record(fields, _, _) =>
        PartialCompletionResult(
          fields
            .filter(v => v._1 == completing || !existing.contains(v._1))
            .map { case (field, ty) =>
              PartialCompletionItem(
                typer,
                field,
                ty,
                CompletionKind.Property,
                // prioritize required fields
                if (typer.allowMissingField(ty)) 0 else 10
              )
            }
            .toSeq)
      case _ => PartialCompletionResult.empty
    }
}
