package fql.ast

/** `strict` controls if we are extra strict or not.
  * - integers and floats are redacted only when strict is true.
  * - strings are entirely redacted when `strict`, but their length is shown when
  *   not strict.
  * - refs are hidden only when `strict` is true.
  */
final class Redact(strict: Boolean) {
  def redactLiteral(lit: Literal, sp: Span): Expr =
    lit match {
      case Literal.Str(_) if strict => Expr.Id(s"<string>", sp)
      case Literal.Str(str) => Expr.Id(s"<string with length ${str.length}>", sp)

      case Literal.Int(_) if strict   => Expr.Id(s"<integer>", sp)
      case Literal.Float(_) if strict => Expr.Id(s"<float>", sp)

      case _ => Expr.Lit(lit, sp)
    }

  def redactExpr(expr: Expr): Expr = expr match {
    case Expr.Id(id, sp) => Expr.Id(id, sp)
    case Expr.Lit(l, sp) => redactLiteral(l, sp)
    case Expr.StrTemplate(parts: Seq[Either[String, Expr]], sp) =>
      if (strict) {
        Expr.Id(s"<string>", sp)
      } else {
        parts match {
          case Seq(Left(str)) => Expr.Id(s"<string with length ${str.length}>", sp)
          case _              => Expr.Id(s"<string with templates>", sp)
        }
      }
    case Expr.If(pred, thn, sp) => Expr.If(redactExpr(pred), redactExpr(thn), sp)
    case Expr.IfElse(pred, thn, els, sp) =>
      Expr.IfElse(redactExpr(pred), redactExpr(thn), redactExpr(els), sp)
    case Expr.At(ts, body, sp) => Expr.At(ts, redactExpr(body), sp)
    case _: Expr.Match         => sys.error("unimplemented")
    case Expr.LongLambda(params, vari, body, sp) =>
      Expr.LongLambda(params, vari, redactExpr(body), sp)
    case Expr.ShortLambda(body) => Expr.ShortLambda(redactExpr(body))
    case Expr.OperatorCall(e, op, args, sp) =>
      Expr.OperatorCall(redactExpr(e), op, args.map(redactExpr), sp)
    case Expr.Project(e, bindings, sp) =>
      val b = bindings.map { case (n, v) => n -> redactExpr(v) }
      Expr.Project(redactExpr(e), b, sp)
    case Expr.ProjectAll(e, sp) => Expr.ProjectAll(redactExpr(e), sp)
    case Expr.Object(fields, sp) =>
      Expr.Object(fields.map { case (n, e) => n -> redactExpr(e) }, sp)
    case Expr.Tuple(elems, sp) => Expr.Tuple(elems.map(redactExpr), sp)
    case Expr.Array(elems, sp) => Expr.Array(elems.map(redactExpr), sp)
    case Expr.Block(body, sp) =>
      Expr.Block(body.map(redactStmt), sp)
    case Expr.MethodChain(e, chain, sp) =>
      val c = chain.map {
        case app: Expr.MethodChain.Apply =>
          app.copy(args = app.args.map(redactExpr))
        case acc: Expr.MethodChain.Access =>
          acc.copy(args = acc.args.map(redactExpr))
        case call: Expr.MethodChain.MethodCall =>
          call.copy(args = call.args.map(redactExpr))
        case elem => elem
      }
      Expr.MethodChain(redactExpr(e), c, sp)
  }

  def redactStmt(stmt: Expr.Stmt): Expr.Stmt = stmt match {
    case Expr.Stmt.Let(n, tpe, rhs, sp) =>
      Expr.Stmt.Let(n, tpe, redactExpr(rhs), sp)
    case Expr.Stmt.Expr(e) => Expr.Stmt.Expr(redactExpr(e))
  }
}

package object redact {
  implicit class ExprRedactOps(val expr: Expr) extends AnyVal {
    def redact = new Redact(false).redactExpr(expr)
    def redactStrict = new Redact(true).redactExpr(expr)
  }
}
