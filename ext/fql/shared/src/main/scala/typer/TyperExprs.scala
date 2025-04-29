package fql.typer

import fql.ast.{ Expr, Literal, Name, Span, TypeExpr }
import fql.ast.display._
import fql.error.TypeError
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.concurrent.duration._

trait TyperExprs { self: Typer =>

  protected def typeExpr0(expr: Expr)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level,
    deadline: Deadline): Typecheck.Final = {
    if (deadline.isOverdue()) {
      return Typecheck.fail(TypeError.TimeLimitExceeded(expr.span))
    }
    dbgFrame(s"type ${expr.display}") {
      val tc: Typecheck.Final = expr match {
        case Expr.Id(name, span) =>
          vctx.get(name).orElse(globals.get(name)) match {
            case None =>
              Typecheck.fail(TypeError.UnboundVariable(span, name))
            case Some(sch) =>
              val ty = instantiateValue(sch)
              Typecheck(if (ty.span == Span.Null) ty.move(span) else ty)
          }

        // FIXME: this exception here feels like a wart, but is currently
        // necessary to get Difference to work. remove once Difference is fully
        // fleshed out beyond simple null subtraction.
        case Expr.Lit(Literal.Null, span) => Typecheck(Type.Null(span))
        case Expr.Lit(lit, span)          => Typecheck(Constraint.Lit(lit, span))

        case Expr.StrTemplate(parts, span) =>
          Typecheck
            .sequence(
              parts.flatMap {
                case Left(_)     => None
                case Right(expr) => Some(typeExpr0(expr))
              }
            )
            .map(_ => Type.Str(span))

        case Expr.If(pred, thn, span) =>
          for {
            pred_ty <- typeExpr0(pred)
            _       <- constrain(pred_ty, Type.Boolean(pred.span), pred.span)
            _       <- typeExpr0(thn)
          } yield Type.Null(span)

        case Expr.IfElse(pred, thn, els, span) =>
          for {
            pred_ty <- typeExpr0(pred)
            _       <- constrain(pred_ty, Type.Boolean(pred.span), pred.span)
            thn_ty  <- typeExpr0(thn)
            els_ty  <- typeExpr0(els)
            res = freshVar(span)
            _ <- constrain(thn_ty, res, span)
            _ <- constrain(els_ty, res, span)
          } yield res

        case Expr.At(ts, body, _) =>
          for {
            ts_ty   <- typeExpr0(ts)
            body_ty <- typeExpr0(body)
            _ <- constrain(
              ts_ty,
              Type.Union(ArraySeq(Type.Time, Type.TransactionTime), ts.span),
              ts.span)
          } yield body_ty

        case Expr.LongLambda(params, vari, body, span) =>
          val p_tys = (params).iterator
            .map { p =>
              val v = freshVar(p.fold(Span.Null)(_.span))
              p.foreach(arg => visit(arg.span, v))
              v
            }
            .to(ArraySeq)
          val v_ty = vari.map { p =>
            val v = freshVar(p.fold(Span.Null)(_.span))
            p.foreach(arg => visit(arg.span, v))
            v
          }
          val pctx =
            params.view.zip(p_tys).flatMap { case (n, ty) =>
              n.map(_.str -> TypeScheme.Simple(ty))
            }
          val varictx =
            vari.zip(v_ty).flatMap { case (n, e_ty) =>
              n.map(_.str -> TypeScheme.Simple(Type.Array(e_ty, e_ty.span)))
            }

          val vctx0 = vctx ++ pctx ++ varictx

          typeExpr0(body)(vctx0, containerAlt, level, deadline).map { b_ty =>
            Constraint.Func(p_tys.map(None -> _), v_ty.map(None -> _), b_ty, span)
          }

        case Expr.ShortLambda(body) =>
          val p_ty = freshVar(body.span)
          val vctx0 = vctx + (Expr.This.name -> TypeScheme.Simple(p_ty))
          typeExpr0(body)(vctx0, containerAlt, level, deadline).map { b_ty =>
            Constraint.Func(ArraySeq(None -> p_ty), None, b_ty, body.span)
          }

        case c @ Expr.OperatorCall(e, op, args, argsSpan) =>
          op.str match {
            case "??" =>
              for {
                lhs_ty <- typeExpr0(e)
                rhs_ty <- typeExpr0(args.head)
                diff = Constraint.Diff(lhs_ty, Type.Null(op.span))
                res = freshVar(argsSpan)
                _ <- constrain(diff, res, e.span)
                _ <- constrain(rhs_ty, res, args.head.span)
              } yield res

            case "&&" | "||" =>
              for {
                lhs_ty <- typeExpr0(e)
                rhs_ty <- typeExpr0(args.head)
                _      <- constrain(lhs_ty, Type.Boolean(op.span), e.span)
                _      <- constrain(rhs_ty, Type.Boolean(op.span), args.head.span)
              } yield Type.Boolean(c.span)

            // TODO: Implement a typecheck when a proper type describes the RHS.
            case "isa" => Typecheck(Type.Boolean(c.span))

            case _ if ops.contains(op.str) =>
              typeApply(
                Typecheck(instantiateValue(ops(op.str))),
                (e +: args.toSeq).map(a => (typeExpr0(a), a.span)),
                argsSpan,
                c.span)

            // only happens if an operator is never defined.
            case _ =>
              typeExpr0(e).flatMap { lhs_ty =>
                val te = TypeConstraintFailure.toError(
                  TypeConstraintFailure.MissingField(lhs_ty, op.span, e.span, op.str)
                )(displayValue, displayUse)
                Typecheck.fail(te)
              }
          }

        case Expr.Project(e, sels, span) =>
          for {
            e_ty   <- typeExpr0(e)
            lam_ty <- typeExpr0(Expr.ShortLambda(Expr.Object(sels, span)))
            ret = freshVar(span)
            proj = Constraint.Proj(lam_ty, ret, span)
            _ <- constrain(e_ty, proj, span)
          } yield {
            ret
          }

        case Expr.Tuple(elems, span) =>
          // Special-case single elem parens Tuple
          if (elems.sizeIs == 1) {
            typeExpr0(elems.head).map(_.move(span))
          } else {
            Typecheck
              .sequence(elems.map(typeExpr0))
              .map(e_tys => Constraint.Tup(e_tys.to(ArraySeq), span))
          }

        // array lits and tuples are the same
        case Expr.Array(elems, span) =>
          Typecheck
            .sequence(elems.map(typeExpr0))
            .map(e_tys => Constraint.Tup(e_tys.to(ArraySeq), span))

        case Expr.Object(fields, span) =>
          val names = List.newBuilder[String]
          val tcs = List.newBuilder[Typecheck.Final]

          fields.foreach { case (n, v) =>
            names += n.str
            tcs += typeExpr0(v)
          }

          Typecheck.sequence(tcs.result()) map { tys =>
            Constraint.Rec(names.result().zip(tys).to(SeqMap), span)
          }

        case Expr.Block(body, _) =>
          var innerCtx = vctx

          // This is only hit when typechecking queries for auto-completion.
          val seed = Typecheck(Type.Never: Constraint.Value)

          body.foldLeft(seed) {
            case (tc0, Expr.Stmt.Let(name, sig, expr, _)) =>
              val tc = typeLetRhs(false, name, sig, expr)(
                innerCtx,
                containerAlt,
                level,
                deadline)
              // FIXME: This += copies the whole vctx, should probably use an
              // MMap
              innerCtx += (name.str -> tc.value)
              tc0.flatMap(_ => tc.map(_ => Type.Never))
            case (tc0, Expr.Stmt.Expr(expr)) =>
              val tc = typeExpr0(expr)(innerCtx, containerAlt, level, deadline)
              tc0.flatMap(_ => tc)
          }

        case Expr.MethodChain(e, chain, _) =>
          // This span is used to keep track of where we are as we progress through
          // the method chain.  This is needed so that we can correctly place spans
          // during apply type errors.
          var processedSpan = e.span
          // Records whether the result of the method chain may be null because
          // of an intermediate optional selection, e.g. x.?y.z.
          var possiblyNull = false
          val mctc = chain.foldLeft(typeExpr0(e)) { (e_tc, mcc) =>
            val tc = mcc match {
              case Expr.MethodChain.Bang(span) =>
                possiblyNull = false
                e_tc.map(Constraint.Diff(_, Type.Null(span)))

              case Expr.MethodChain.Select(_, field, false) =>
                typeSelect(e_tc, field, processedSpan)

              case Expr.MethodChain.Select(_, field, true) =>
                for {
                  // Remove "| null" constraints to type the select...
                  e_ty <- typeSelect(
                    e_tc.map(Constraint.Diff(_, Type.Null)),
                    field,
                    processedSpan)
                  res = freshVar(field.span)
                  _ <- constrain(e_ty, res, field.span)
                } yield {
                  // ... then note they may need to be re-added in the end.
                  // See also Apply, Access, and MethodCall.
                  possiblyNull = true
                  res
                }

              case Expr.MethodChain.Apply(args, None, span) =>
                typeApply(
                  e_tc,
                  args.map(a => (typeExpr0(a), a.span)),
                  span,
                  e.span.copy(end = span.end))

              case Expr.MethodChain.Apply(args, Some(_), span) =>
                for {
                  ap_ty <- typeApply(
                    e_tc.map(Constraint.Diff(_, Type.Null)),
                    args.map(a => (typeExpr0(a), a.span)),
                    span,
                    e.span.copy(end = span.end))
                  res = freshVar(span)
                  _ <- constrain(ap_ty, res, span)
                } yield {
                  possiblyNull = true
                  res
                }

              // TODO: If we can fix how we type access below (get it all into
              // one
              // constrain() call), then we can remove this typeLitAccess special
              // case.
              // And the typer would be more flexible as this would work:
              // { let x = "foo", rec[x] }
              case Expr.MethodChain.Access(Seq(lit: Expr.Lit), None, span) =>
                typeLitAccess(e_tc, lit, span, e.span.copy(end = span.end))
              case Expr.MethodChain.Access(Seq(lit: Expr.Lit), Some(_), span) =>
                for {
                  acc_ty <- typeLitAccess(
                    e_tc.map(Constraint.Diff(_, Type.Null)),
                    lit,
                    span,
                    e.span.copy(end = span.end))
                  res = freshVar(span)
                  _ <- constrain(acc_ty, res, span)
                } yield {
                  possiblyNull = true
                  res
                }

              case Expr.MethodChain.Access(args, None, span) =>
                typeAccess(
                  e_tc,
                  args.map(a => (typeExpr0(a), a.span)).toSeq,
                  span,
                  e.span.copy(end = span.end))
              case Expr.MethodChain.Access(args, Some(_), span) =>
                for {
                  acc_ty <- typeAccess(
                    e_tc.map(Constraint.Diff(_, Type.Null)),
                    args.map(a => (typeExpr0(a), a.span)).toSeq,
                    span,
                    e.span.copy(end = span.end))
                  res = freshVar(span)
                  _ <- constrain(acc_ty, res, span)
                } yield {
                  possiblyNull = true
                  res
                }

              case Expr.MethodChain.MethodCall(
                    _,
                    field,
                    args,
                    fieldOptional,
                    applyOptional,
                    argsSpan) =>
                val selSpan = field.span
                val appSpan = e.span.copy(end = argsSpan.end)

                val e_tc_opt = if (fieldOptional) {
                  possiblyNull = true
                  e_tc.map(Constraint.Diff(_, Type.Null))
                } else {
                  e_tc
                }

                val sel_tc = typeSelect(e_tc_opt, field, processedSpan)
                visit(selSpan, sel_tc.value)
                val sel_tc_opt = if (applyOptional.isDefined) {
                  possiblyNull = true
                  sel_tc.map(Constraint.Diff(_, Type.Null))
                } else {
                  sel_tc
                }

                val arg_tcs = args.map(a => (typeExpr0(a), a.span))
                val ap_tc = typeApply(sel_tc_opt, arg_tcs, argsSpan, appSpan)
                ap_tc
            }
            processedSpan = processedSpan.copy(end = mcc.span.end)

            dbg(s"typed as: ${tc.value}")
            visitMethodChain(mcc, tc)
            tc
          }
          if (possiblyNull) {
            dbg("re-adding null constraint to ?. chain")
            for {
              mc_ty <- mctc
              res = freshVar(e.span)
              _ <- constrain(mc_ty, res, e.span)
              _ <- constrain(Type.Null, res, e.span)
            } yield res
          } else {
            mctc
          }
        case ex @ (_: Expr.Match | _: Expr.ProjectAll) =>
          Typecheck.fail(TypeError.UnimplementedExpr(ex.span, ex))
      }
      if (dbgEnabled) {
        dbg(s"typed as: ${tc.value}")

        val bounds = TyperDebug.getBounds(tc.value)
        if (bounds.nonEmpty) {
          dbg("BOUNDS")
          bounds.foreach(b => dbg(s" + $b"))
        }

        val (valts, ualts) = typeAlts(tc.value, true, containerAlt)
        if (valts.nonEmpty || ualts.nonEmpty) {
          dbg("Alts:")
          dbg(s" ~ Values: ${if (valts.isEmpty) "-" else ""}")
          valts.foreach(a => dbg(s"   + $a"))
          dbg(s" ~ Uses: ${if (ualts.isEmpty) "-" else ""}")
          ualts.foreach(a => dbg(s"   + $a"))
        }
      }

      tc.value match {
        case v: Type.Var if tc.isFailed =>
          dbg(s"poisoning ${tc.value}")
          v.poison()
        case _ => ()
      }
      visitExpr(expr, tc)
      tc
    }
  }

  private def typeLetRhs(
    isrec: Boolean,
    name: Name,
    sig: Option[TypeExpr],
    rhs: Expr)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level,
    deadline: Deadline): Typecheck[TypeScheme] = {
    val ret_tc = if (!isrec) {
      typeExpr0(rhs)(vctx, containerAlt, level.incr, deadline)
    } else {
      val ret = freshVar(sig.map(_.span).getOrElse(rhs.span))(level.incr)
      val vctx0 = vctx + (name.str -> TypeScheme.Simple(ret))

      for {
        rhs_ty <- typeExpr0(rhs)(vctx0, containerAlt, level.incr, deadline)
        _ <- constrain(rhs_ty, ret, name.span)(
          vctx0,
          containerAlt,
          level.incr,
          deadline)
      } yield ret
    }

    sig match {
      case None =>
        ret_tc.map { ty =>
          val free = freeAlts(vctx)
          val annealed = annealValue(ty, containerAlt, level.incr, free)
          TypeScheme.Polymorphic(annealed, level)
        }
      case Some(sig) =>
        for {
          ret <- ret_tc
          (sig_ty, errs) = typeTExpr0(
            sig,
            checked = true,
            allowGenerics = true,
            allowVariables = true
          )(Map.empty, containerAlt, level.incr, pol = true)
          _ <- Typecheck.lift(errs)
          _ <- constrain(ret, sig_ty, name.span)(
            vctx,
            containerAlt,
            level.incr,
            deadline)
        } yield TypeScheme.Skolemized(sig_ty)
    }
  }

  /** Select operators build an Interface.
    */
  private def typeSelect(r_tc: Typecheck.Final, field: Name, processedSpan: Span)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level
  ): Typecheck.Final = {
    val dotSpan =
      processedSpan.copy(start = processedSpan.end, end = field.span.start)
    for {
      e_ty <- r_tc
      res = freshVar(field.span)
      sel = Constraint.Interface(field.str, res, dotSpan, field.span)
      _ <- constrain(e_ty, sel, processedSpan)
    } yield res
  }

  private def typeApply(
    fn_tc: Typecheck.Final,
    args: Seq[(Typecheck.Final, Span)],
    argsSpan: Span,
    appSpan: Span)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level
  ): Typecheck.Final = {
    for {
      fn_ty   <- fn_tc
      arg_tys <- Typecheck.sequence(args.iterator.map(_._1))
      p_tys = args.iterator.map(a => freshVar(a._2)).to(ArraySeq)
      res = freshVar(argsSpan)
      app = Constraint.Apply(p_tys, res, argsSpan)
      _ <- constrain(fn_ty, app, appSpan)
      arg_tcs = args.iterator
        .map(_._2)
        .zip(arg_tys)
        .zip(p_tys)
        .map { case ((sp, a_ty), p_ty) => constrain(a_ty, p_ty, sp) }
      _ <- Typecheck.sequence(arg_tcs)
    } yield {
      res
    }
  }

  private def typeAccess(
    lhs_tc: Typecheck.Final,
    args: Seq[(Typecheck.Final, Span)],
    argsSpan: Span,
    accSpan: Span)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level
  ): Typecheck.Final = {
    for {
      lhs_ty  <- lhs_tc
      arg_tys <- Typecheck.sequence(args.iterator.map(_._1))
      p_tys = args.iterator.map(a => freshVar(a._2)).to(ArraySeq)
      res = freshVar(argsSpan)
      acc = Constraint.Access(p_tys, res, argsSpan)
      _ <- constrain(lhs_ty, acc, accSpan)
      arg_tcs = args.iterator
        .map(_._2)
        .zip(arg_tys)
        .zip(p_tys)
        .map { case ((sp, a_ty), p_ty) =>
          // If we typecheck at the arg span, errors introduced here can
          // have causes which point to the apply or fn spans, which is
          // confusing. To get around this, we move the errors to the
          // apply and then back in order to sweep up causes to the right
          // spot.
          constrain(a_ty, p_ty, sp)
        }
      _ <- Typecheck.sequence(arg_tcs)
    } yield {
      res
    }
  }

  private def typeLitAccess(
    lhs_tc: Typecheck.Final,
    arg: Expr.Lit,
    argsSpan: Span,
    accSpan: Span)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level
  ): Typecheck.Final = {
    for {
      lhs_ty <- lhs_tc
      res = freshVar(argsSpan)
      acc = Constraint.Access(
        ArraySeq(Constraint.Lit(arg.value, arg.span)),
        res,
        argsSpan)
      _ <- constrain(lhs_ty, acc, accSpan)
    } yield {
      res
    }
  }
}
