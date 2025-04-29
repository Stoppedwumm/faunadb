package fql.ast

import fql.ast.display._
import fql.parser.Parser
import fql.Result

package object syntax {
  // TODO: turn these into macros
  implicit class FQLHelper(val sc: StringContext) extends AnyVal {
    def fqls(args: Any*): SchemaItem = {
      val strings = sc.parts.iterator
      val exprs = args.iterator
      val buf = new StringBuilder(strings.next())
      while (strings.hasNext) {
        buf ++= exprs.next().toString
        buf ++= strings.next()
      }
      Parser.schemaItems(buf.result()) match {
        case Result.Ok(Seq(item)) => item
        case Result.Ok(_) =>
          throw new IllegalArgumentException("multiple items defined")
        case Result.Err(errs) =>
          throw new IllegalArgumentException(errs.mkString(", "))
      }
    }

    def fqlc(args: Any*): SchemaItem.Collection =
      fqls(args).asInstanceOf[SchemaItem.Collection]

    def fql(args: Any*): Expr = {
      val strings = sc.parts.iterator
      val exprs = args.iterator
      val buf = new StringBuilder(strings.next())
      while (strings.hasNext) {
        buf ++= exprs.next().toString
        buf ++= strings.next()
      }
      Parser.expr(buf.result()) match {
        case Result.Ok(e) => e
        case Result.Err(errs) =>
          throw new IllegalArgumentException(errs.mkString(", "))
      }
    }

    def fqlt(args: TypeExpr*): TypeExpr = {
      val strings = sc.parts.iterator
      val texprs = args.iterator
      val buf = new StringBuilder(strings.next())
      while (strings.hasNext) {
        // FIXME: this string-based construction is error prone. use some
        // sort of symbol-based subst later.
        buf ++= texprs.next().display
        buf ++= strings.next()
      }
      Parser.typeExpr(buf.result()) match {
        case Result.Ok(e) => e
        case Result.Err(errs) =>
          throw new IllegalArgumentException(errs.mkString(", "))
      }
    }
  }
}
