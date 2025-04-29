package fql.ast

import fql.parser.{ Chars, Tokens }
import fql.typer.{ Type, Typer }

final class Displayer(val sb: StringBuilder = new StringBuilder) {
  var _indent = 0
  var needsIndent = true

  def write(v: String) = {
    if (needsIndent) {
      sb.append(s"  ".repeat(_indent))
      needsIndent = false
    }
    sb.append(v)
    if (v.endsWith("\n")) {
      needsIndent = true
    }
  }

  def writeln() = write("\n")
  def writeln(v: String): Unit = {
    write(v)
    writeln()
  }

  def writeStrLit(v: String): Unit = {
    // This handles any indents
    write("\"")
    // Now that we won't be appending any newlines, we can write to `sb` directly.
    writeStr0(v)
    write("\"")
  }

  private def writeChar(c: Char): Unit =
    Chars.Unescaped.get(c) match {
      case Some(replace) =>
        sb.append('\\')
        sb.append(replace)
      case None =>
        if (Chars.isDisplay(c)) {
          sb.append(c)
        } else {
          sb.append(f"\\u$c%04x")
        }
    }

  private def writeCodepoint(i: Int): Unit = {
    require(i <= 0x10ffff, "codepoint outside of valid range")
    if (i <= 0xffff) {
      // 2-byter. Treat as char.
      writeChar(i.asInstanceOf[Char])
    } else {
      // This codepoint's a mouthful.
      sb.append(f"\\u{$i%06x}")
    }
  }

  private def writeStr0(v: String): Unit = {
    // NB: I would use v.codePoints, but scalaJS doesn't include IntStream.
    val n = v.codePointCount(0, v.size)
    (0 to n - 1) map { i =>
      v.codePointAt(v.offsetByCodePoints(0, i))
    } foreach (writeCodepoint)
  }

  def writeIdent(v: String) = {
    if (Tokens.isValidIdent(v)) {
      write(v)
    } else {
      writeStrLit(v)
    }
  }

  def indent() = {
    _indent += 1
  }
  def deindent() = {
    _indent -= 1
  }

  def writeLit(lit: Literal): Unit =
    lit match {
      case Literal.Null       => write("null")
      case Literal.True       => write("true")
      case Literal.False      => write("false")
      case Literal.Int(num)   => write(num.toString)
      case Literal.Float(num) => write(num.toString)
      case Literal.Str(str)   => writeStrLit(str)
    }

  // TODO: Split onto multiple lines if the line would be too long.
  def writeCommaSeparated[V](seq: Seq[V])(writeFn: V => Unit) = {
    seq.zipWithIndex.foreach { case (v, idx) =>
      writeFn(v)
      if (idx != seq.length - 1) {
        write(", ")
      }
    }
  }

  def writeCommaSeparatedNewlines[V](seq: Seq[V])(writeFn: V => Unit) = {
    seq.zipWithIndex.foreach { case (v, idx) =>
      writeFn(v)
      if (idx != seq.length - 1) {
        write(",")
      }
      writeln("")
    }
  }

  def writeExpr(expr: Expr): Unit = expr match {
    case Expr.Id(n, _)  => if (n != Expr.This.name) write(n)
    case Expr.Lit(l, _) => writeLit(l)
    case Expr.StrTemplate(parts: Seq[Either[String, Expr]], _) =>
      write("\"")
      parts.foreach {
        case Left(str) =>
          writeStr0(str)
        case Right(ex) =>
          write("#{")
          writeExpr(ex)
          write("}")
      }
      write("\"")
    case Expr.If(pred, thn, _) =>
      write("if (")
      writeExpr(pred)
      write(") ")
      writeExpr(thn)
    case Expr.IfElse(pred, thn, els, _) =>
      write("if (")
      writeExpr(pred)
      write(") ")
      writeExpr(thn)
      write(" else ")
      writeExpr(els)
    case Expr.At(ts, body, _) =>
      write("at (")
      writeExpr(ts)
      write(") ")
      writeExpr(body)
    case _: Expr.Match => sys.error("unimplemented")
    case Expr.LongLambda(params, vari, body, _) =>
      write("(")
      val paramStrs = params.map { _.fold("_") { _.str } }
      val variStr = vari.map { _.fold("..._") { n => s"...${n.str}" } }
      writeCommaSeparated(paramStrs ++ variStr) { write(_) }
      write(") => ")
      writeExpr(body)
    case Expr.ShortLambda(body) =>
      writeExpr(body)
    case Expr.OperatorCall(e, op, None, _) =>
      write(op.str)
      writeExpr(e)
    case Expr.OperatorCall(e, op, Some(rhs), _) =>
      writeExpr(e)
      write(" ")
      write(op.str)
      write(" ")
      writeExpr(rhs)
    case Expr.Project(e, bindings, _) =>
      writeExpr(e)
      writeln(" {")
      indent()
      writeCommaSeparatedNewlines(bindings) { case (n, v) =>
        writeIdent(n.str)
        write(": ")
        writeExpr(v)
      }
      deindent()
      writeln("}")
    case Expr.ProjectAll(e, _) =>
      writeExpr(e)
      writeln(" { * }")
    case Expr.Object(fields, _) =>
      writeln("{")
      indent()
      writeCommaSeparatedNewlines(fields) { case (n, e) =>
        writeIdent(n.str)
        write(": ")
        writeExpr(e)
      }
      deindent()
      write("}")
    case Expr.Tuple(Seq(e), _) =>
      write("(")
      writeExpr(e)
      write(")")
    case Expr.Tuple(elems, _) =>
      write("(")
      writeCommaSeparated(elems)(writeExpr)
      write(")")
    case Expr.Array(elems, _) =>
      write("[")
      writeCommaSeparated(elems)(writeExpr)
      write("]")
    case Expr.Block(body, _) =>
      writeln("{")
      indent()
      writeBody(body)
      deindent()
      write("}")
    case Expr.MethodChain(e, chain, _) =>
      writeExpr(e)
      chain.foreach {
        case Expr.MethodChain.Bang(_) =>
          write("!")
        case Expr.MethodChain.Select(_, field, optional) =>
          if (optional) write("?")
          write(".")
          write(field.str)
        case Expr.MethodChain.Apply(args, optional, _) =>
          if (optional.isDefined) write("?.")
          write("(")
          writeCommaSeparated(args)(writeExpr)
          write(")")
        case Expr.MethodChain.Access(args, optional, _) =>
          if (optional.isDefined) write("?.")
          write("[")
          writeCommaSeparated(args)(writeExpr)
          write("]")
        case Expr.MethodChain.MethodCall(
              _,
              field,
              args,
              fieldOptional,
              applyOptional,
              _) =>
          if (fieldOptional) write("?")
          write(".")
          write(field.str)
          if (applyOptional.isDefined) write("?.")
          write("(")
          writeCommaSeparated(args)(writeExpr)
          write(")")
      }
  }

  def writeBody(stmts: Seq[Expr.Stmt]): Unit = {
    stmts.foreach { s =>
      writeStmt(s)
      writeln("")
    }
  }

  def writeStmt(stmt: Expr.Stmt): Unit = stmt match {
    case Expr.Stmt.Let(n, Some(tpe), rhs, _) =>
      write("let ")
      write(n.str)
      write(": ")
      write(displayTExpr(tpe))
      write(" = ")
      writeExpr(rhs)
    case Expr.Stmt.Let(n, None, rhs, _) =>
      write("let ")
      write(n.str)
      write(" = ")
      writeExpr(rhs)
    case Expr.Stmt.Expr(e) =>
      writeExpr(e)
  }

  def displayTExpr(expr: TypeExpr): String = {
    import display._

    def display0(expr: TypeExpr, prec: Int): String =
      expr match {
        case TypeExpr.Hole(_)         => "_"
        case TypeExpr.Any(_)          => "Any"
        case TypeExpr.Never(_)        => "Never"
        case TypeExpr.Singleton(v, _) => v.display
        case TypeExpr.Id(name, _)     => name
        case TypeExpr.Cons(name, targs, _) =>
          targs.iterator
            .map(a => display0(a, 0))
            .mkString(s"${name.str}<", ", ", ">")
        case TypeExpr.Object(fields, wildcard, _) =>
          val fstrs = fields.iterator
            .map { case (n, e) => s"${displayFieldName(n.str)}: ${display0(e, 0)}" }
          val wstr = wildcard.iterator.map { tpe => s"*: ${display0(tpe, 0)}" }
          val elems = fstrs.concat(wstr)
          if (elems.isEmpty) "{}" else elems.mkString("{ ", ", ", " }")
        case TypeExpr.Interface(fields, _) =>
          val elems = fields.iterator
            .map { case (n, e) => s"${displayFieldName(n.str)}: ${display0(e, 0)}" }
          if (elems.isEmpty) "{}" else elems.mkString("{ ", ", ", ", ... }")
        case TypeExpr.Projection(proj, ret, _) =>
          val pstr = display0(proj, 0)
          val rstr = display0(ret, 0)
          s".{ $pstr } => $rstr"
        case TypeExpr.Tuple(elems, _) =>
          elems.iterator.map(display0(_, 0)).mkString("[", ", ", "]")
        case TypeExpr.Lambda(Seq((name, p)), None, ret, _) =>
          val str = name match {
            case Some(name) =>
              s"(${name.str}: ${display0(p, 11)}) => ${display0(ret, 10)}"
            case None => s"${display0(p, 11)} => ${display0(ret, 10)}"
          }
          if (prec > 10) s"($str)" else str
        case TypeExpr.Lambda(ps, variadic, ret, _) =>
          val argStrs = ps.map {
            case (Some(name), ty) => s"${name.str}: ${display0(ty, 10)}"
            case (None, ty)       => display0(ty, 10)
          }
          val vStr = variadic.map {
            case (Some(name), v) => s"${name.str}: ...${display0(v, 10)}"
            case (None, v)       => s"...${display0(v, 10)}"
          }

          val args = argStrs ++ vStr
          val str = args.mkString("(", ", ", s") => ${display0(ret, 10)}")
          if (prec > 10) s"($str)" else str
        case TypeExpr.Union(members, _) =>
          val str = members.iterator.map(display0(_, 20)).mkString(" | ")
          if (prec > 20) s"($str)" else str
        case TypeExpr.Intersect(members, _) =>
          val str = members.iterator.map(display0(_, 30)).mkString(" & ")
          if (prec > 30) s"($str)" else str
        case TypeExpr.Difference(elem, sub, _) =>
          val str1 = display0(elem, 50)
          val str2 = display0(sub, 50)
          val str = s"$str1 - $str2"
          if (prec > 15) s"($str)" else str
        case TypeExpr.Recursive(name, in, _) =>
          val str = s"${display0(in, 50)} as ${name.str}"
          if (prec > 40) s"($str)" else str
        case TypeExpr.Nullable(base, _, _) =>
          s"${display0(base, 50)}?"
      }

    display0(expr, 0)
  }

  def displaySchemaTExpr(expr: SchemaTypeExpr): String = {
    import display._

    def display0(expr: SchemaTypeExpr): String =
      expr match {
        case SchemaTypeExpr.Simple(te) => te.display
        case SchemaTypeExpr.Object(fields, wildcard, _) =>
          val fstrs = fields.iterator
            .map {
              case (n, e, None) =>
                s"${displayFieldName(n.str)}: ${display0(e)}"
              case (n, e, Some(default)) =>
                s"${displayFieldName(n.str)}: ${display0(e)} = ${default.display}"
            }
          val wstr = wildcard.iterator.map { tpe => s"*: ${tpe.display}" }
          val elems = fstrs.concat(wstr)
          if (elems.isEmpty) "{}" else elems.mkString("{ ", ", ", " }")
      }

    display0(expr)
  }

  def displayTSchemeExpr(sch: TypeExpr.Scheme): String = {
    val tstr = displayTExpr(sch.expr)
    val pstr = if (sch.params.isEmpty) "" else sch.params.mkString("<", ", ", "> ")
    s"$pstr$tstr"
  }

  private def displayFieldName(n: String) =
    if (Tokens.isValidIdent(n)) {
      n
    } else {
      val quote = "\""
      s"$quote$n$quote"
    }

  def writeSchemaItem(item: SchemaItem): Unit = {
    item.annotations foreach { writeAnnotation(_) }
    write(item.kind.keyword)
    write(" ")
    writeIdent(item.name.str)

    item match {
      case config: SchemaItem.ItemConfig =>
        writeln(" {")
        indent()
        config.fields foreach { writeField(_) }
        config.members foreach { writeMember(_) }
        deindent()
        writeln("}")

      case fn: SchemaItem.Function =>
        writeFunctionSig(fn.sig)
        write(" ")
        writeExpr(fn.body)
        writeln()
    }
  }

  def writeFunctionSig(sig: SchemaItem.Function.Sig): Unit = {
    write("(")
    writeCommaSeparated(sig.args) { arg =>
      if (arg.variadic) write("...")
      write(arg.name.str)
      arg.ty foreach { ty =>
        write(": ")
        write(displayTExpr(ty))
      }
    }
    write(")")
    sig.ret foreach { ty =>
      write(": ")
      write(displayTExpr(ty))
    }
  }

  def writeAnnotation(ann: Annotation): Unit = {
    write("@")
    write(ann.kind.keyword)
    write("(")
    writeConfig(ann.config)
    writeln(")")
  }

  def writeField(f: Field): Unit = {
    f.kind match {
      case FSL.FieldKind.Defined  => ()
      case FSL.FieldKind.Compute  => write("compute ")
      case FSL.FieldKind.Wildcard => ()
    }
    write(f.name.str)

    f match {
      case f: Field.Defined =>
        write(": ")
        write(displaySchemaTExpr(f.schemaType))
      case _ =>
        f.ty foreach { ty =>
          write(": ")
          write(displayTExpr(ty))
        }
    }

    f match {
      case _: Field.Defined =>
        f.value.foreach { v =>
          write(" = ")
          writeConfig(v)
        }
      case f: Field.Computed =>
        write(" = ")
        f._value match {
          case Config.Lambda(_: Expr.ShortLambda, _) =>
            write("(")
            writeConfig(f._value)
            write(")")
          case _ =>
            writeConfig(f._value)
        }
      case _: Field.Wildcard => ()
    }
    writeln()
  }

  def writeMigrationItem(item: MigrationItem) = item match {
    case MigrationItem.Backfill(field, value, _) =>
      write("backfill ")
      writePath(field)
      write(" = ")
      writeExpr(value.expr)
      writeln()
    case MigrationItem.Drop(field, _) =>
      write("drop ")
      writePath(field)
      writeln()
    case MigrationItem.Split(field, to, _) =>
      write("split ")
      writePath(field)
      write(" -> ")
      writeCommaSeparated(to) { t =>
        writePath(t)
      }
      writeln()
    case MigrationItem.Move(from, to, _) =>
      write("move ")
      writePath(from)
      write(" -> ")
      writePath(to)
      writeln()
    case MigrationItem.Add(field, _) =>
      write("add ")
      writePath(field)
      writeln()
    case MigrationItem.MoveWildcardConflicts(field, _) =>
      write("move_conflicts ")
      writePath(field)
      writeln()
    case MigrationItem.MoveWildcard(field, _) =>
      write("move_wildcard ")
      writePath(field)
      writeln()
    case MigrationItem.AddWildcard(_) =>
      write("add_wildcard")
      writeln()
  }

  def writeMigrationBlock(m: MigrationBlock): Unit = {
    if (m.items.isEmpty) {
      writeln("{}")
      return
    }

    writeln("{")
    indent()

    m.items foreach writeMigrationItem
    deindent()
    write("}")
  }

  def writeMember(mem: Member): Unit =
    mem match {
      // Implicit memebers don't show up in FSL.
      case _: Member.Default.Implicit[_] => ()
      case _ =>
        write(mem.kind.keyword)
        mem.nameOpt foreach { name =>
          write(" ")
          writeIdent(name.str)
        }
        mem.config match {
          case opt: Config.Opt if opt.config.isEmpty => ()
          case other =>
            write(" ")
            writeConfig(other)
        }
        writeln()
    }

  def writeConfig(config: Config): Unit =
    config match {
      case Config.Id(value)               => writeIdent(value.str)
      case Config.Bool(value, _)          => write(value.toString)
      case Config.Str(value, _)           => writeStrLit(value)
      case Config.Long(value, _)          => write(value.toString)
      case Config.Expression(expr)        => writeExpr(expr)
      case Config.IndexTerm(path, mva, _) => writeTerm(path, mva)

      case Config.IndexValue(path, mva, asc, _) =>
        if (!asc) write("desc(")
        writeTerm(path, mva)
        if (!asc) write(")")

      case Config.CheckPredicate(lambda, _) =>
        write("(")
        writeExpr(lambda)
        write(")")

      case Config.Predicate(lambda, _) =>
        writeln("{")
        indent()
        write("predicate (")
        writeExpr(lambda)
        writeln(")")
        deindent()
        write("}")

      case Config.Lambda(lambda, _) =>
        writeExpr(lambda)

      case opt: Config.Opt =>
        opt.config match {
          case Some(cfg) => writeConfig(cfg)
          case None      => ()
        }

      case seq: Config.Seq =>
        write("[")
        writeCommaSeparated(seq.configs)(writeConfig)
        write("]")

      case block: Config.Block =>
        writeln("{")
        indent()
        block.members foreach { writeMember(_) }
        deindent()
        write("}")

      case m: MigrationBlock => writeMigrationBlock(m)
    }

  def writeTerm(path: Path, mva: Boolean) = {
    if (mva) write("mva(")
    writePath(path)
    if (mva) write(")")
  }

  def writePath(path: Path) = {
    var first = true
    path.elems foreach { elem =>
      elem match {
        case PathElem.Field(field, _) =>
          if (Tokens.isValidIdent(field)) {
            write(".")
            write(field)
          } else {
            if (first) {
              write(".")
            }
            write("[")
            writeStrLit(field)
            write("]")
          }
        case PathElem.Index(index, _) =>
          if (first) {
            write(".")
          }
          write("[")
          write(index.toString)
          write("]")
      }
      first = false
    }
  }
}

package object display {

  implicit class ExprDisplayOps(val expr: Expr) extends AnyVal {
    def display = {
      val d = new Displayer()
      d.writeExpr(expr)
      d.sb.result()
    }
  }

  implicit class ExprBlockDisplayOps(val expr: Expr.Block) extends AnyVal {
    def displayBody = {
      val d = new Displayer()
      d.writeBody(expr.body)
      d.sb.result()
    }
  }

  implicit class TypeExprDisplayOps(val texpr: TypeExpr) extends AnyVal {
    def display = new Displayer().displayTExpr(texpr)
  }

  implicit class SchemaTypeExprDisplayOps(val te: SchemaTypeExpr) extends AnyVal {
    def display = new Displayer().displaySchemaTExpr(te)
  }

  implicit class TypeDisplayOps(val t: Type) extends AnyVal {
    def display = new Displayer().displayTExpr(Typer().valueToExpr(t))
  }

  implicit class TypeSchemeExprDisplayOps(val sch: TypeExpr.Scheme) extends AnyVal {
    def display = new Displayer().displayTSchemeExpr(sch)
  }

  implicit class LiteralDisplayOps(val lit: Literal) extends AnyVal {
    def display = {
      val d = new Displayer()
      d.writeLit(lit)
      d.sb.result()
    }
  }

  implicit class SchemaItemOps(val item: SchemaItem) extends AnyVal {
    def display = {
      val d = new Displayer()
      d.writeSchemaItem(item)
      d.sb.result()
    }
  }

  implicit class MigrationItemOps(val item: MigrationItem) extends AnyVal {
    def display = {
      val d = new Displayer()
      d.writeMigrationItem(item)
      d.sb.result()
    }
  }

  implicit class SchemaItemFunctionOps(val fn: SchemaItem.Function) extends AnyVal {
    def displayAsUDFBody = {
      val d = new Displayer
      d.write("(")
      d.writeCommaSeparated(fn.sig.args) { arg =>
        if (arg.variadic) d.write("...")
        d.write(arg.name.str)
      }
      d.write(") => ")
      d.writeExpr(fn.body)
      d.sb.result()
    }

    def displayAsUDFSig =
      if (fn.sig.args.forall { _.ty.nonEmpty } && fn.sig.ret.nonEmpty) {
        val d = new Displayer
        d.write("(")
        d.writeCommaSeparated(fn.sig.args) { arg =>
          d.write(arg.name.str)
          d.write(": ")
          if (arg.variadic) d.write("...")
          d.write(d.displayTExpr(arg.ty.get))
        }
        d.write(") => ")
        d.write(d.displayTExpr(fn.sig.ret.get))
        d.sb.result()
      } else {
        ""
      }
  }

  implicit class SchemaFieldOps(val f: Field) extends AnyVal {
    def display = {
      val d = new Displayer
      d.writeField(f)
      d.sb.result()
    }
  }

  implicit class SchemaMemberOps(val member: Member) extends AnyVal {
    def display = {
      val d = new Displayer
      d.writeMember(member)
      d.sb.result()
    }
  }

  implicit class SchemaAnnotationOps(val ann: Annotation) extends AnyVal {
    def display = {
      val d = new Displayer
      d.writeAnnotation(ann)
      d.sb.result()
    }
  }

  implicit class SchemaConfigOps(val config: Config) extends AnyVal {
    def display = {
      val d = new Displayer()
      d.writeConfig(config)
      d.sb.result()
    }
  }

  implicit class SchemaPathOps(val term: Config.IndexTerm) extends AnyVal {
    def displayTerm = {
      val d = new Displayer
      d.writePath(term.path)
      d.sb.result()
    }
  }

  implicit class SchemaOrderedPathOps(val value: Config.IndexValue) extends AnyVal {
    def displayValue = {
      val d = new Displayer
      d.writePath(value.path)
      d.sb.result()
    }
  }

  implicit class SchemaFunctionSigOps(val sig: SchemaItem.Function.Sig)
      extends AnyVal {
    def display = {
      val d = new Displayer
      d.writeFunctionSig(sig)
      d.sb.result()
    }
  }

  implicit class PathOps(val path: Path) extends AnyVal {
    def display = {
      val d = new Displayer
      d.writePath(path)
      d.sb.result()
    }
  }
}
