package fql.parser

import fastparse._
import fastparse.NoWhitespace._
import fql.ast._
import fql.error.ParseError
import scala.collection.mutable.Stack

object SchemaItemParser {
  def termsFieldPath(expr: Expr) = {
    expr match {
      case Expr.MethodChain(
            Expr.Id("mva", _),
            List(Expr.MethodChain.Apply(List(subexpr), _, _)),
            _) =>
        (ExprParser.processFieldPathExpr(subexpr), true)
      case Expr.MethodChain(Expr.Id(_, fspan), _, _) =>
        ((Path.empty, Some(ParseError("Expected 'mva'", fspan))), false)
      case e =>
        (ExprParser.processFieldPathExpr(e), false)
    }
  }

  def valuesFieldPath(expr: Expr) = {
    expr match {
      case Expr.MethodChain(
            Expr.Id(fn0, span0),
            List(
              Expr.MethodChain.Apply(
                List(
                  Expr.MethodChain(
                    Expr.Id(fn1, span1),
                    List(Expr.MethodChain.Apply(List(subexpr), _, _)),
                    _)),
                _,
                _)),
            _) =>
        val Seq((order, ospan), (mva, mspan)) =
          Seq((fn0, span0), (fn1, span1)).sortBy(_._1) // "asc" < "desc" < "mva".
        if (order != "asc" && order != "desc") {
          (
            (
              Path.empty,
              Some(ParseError("Expected 'asc', 'desc', or 'mva'", ospan))),
            true,
            false)
        } else if (mva != "mva") {
          (
            (
              Path.empty,
              Some(ParseError("Expected 'asc', 'desc', or 'mva'", mspan))),
            true,
            false)
        } else {
          (ExprParser.processFieldPathExpr(subexpr), order == "asc", true)
        }
      case Expr.MethodChain(
            Expr.Id(fn, fspan),
            List(Expr.MethodChain.Apply(List(subexpr), _, _)),
            _) =>
        if (fn != "asc" && fn != "desc" && fn != "mva") {
          (
            (
              Path.empty,
              Some(ParseError("Expected 'asc', 'desc', or 'mva'", fspan))),
            true,
            false)
        } else {
          val asc = if (fn == "mva") true else fn == "asc"
          val mva = fn == "mva"
          (ExprParser.processFieldPathExpr(subexpr), asc, mva)
        }
      case e =>
        (ExprParser.processFieldPathExpr(e), true, false)
    }
  }
}

// fastparse macro expansion introduces this unused `charIn` var in many places.
// ignore resulting warnings.
@annotation.nowarn("msg=pattern var charIn*")
// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait SchemaItemParser { parser: Parser =>

  def parseRawSchemaItems[_: P] =
    P(itemnl ~~ rawItemWithDoc.repX(sep = itemsep) ~~ itemsep.? ~~ End)

  private[this] var lastDocComment = Span.Null
  def docComment[_: P] = P(
    Index ~~ (lineComment | (blockComment ~~ CharIn(" \t").repX ~~ "\n".?))
      .repX(1) ~~ Index)
    .map { case (i1, i2) =>
      lastDocComment = span(i1, i2)
      ()
    }

  def itemnl[_: P] = P((P(CharIn("; \t\r\n")) | docComment).repX)
  def itemsep[_: P] = P(
    ws ~~ (((";" | "\n" | "\r\n").repX(1) | docComment) ~~ CharIn(" \t").repX)
      .repX(1))

  def rawItemWithDoc[_: P] = RawSchema.item.map { item =>
    if (lastDocComment != Span.Null && lastDocComment.end == item.span.start) {
      item.copy(
        docComment = Some(lastDocComment),
        span = item.span.from(lastDocComment)
      )
    } else {
      item
    }
  }

  object RawSchema {

    def items[_: P]: P[Seq[FSL.Node]] =
      P(blocksep.? ~~ item.repX(sep = blocksep) ~~ blocksep.?)

    def item[_: P]: P[FSL.Node] =
      P(Index ~~ annotations ~~ item0 ~~ ws ~~ lookahead(optsemi))
        .map { case (i1, annotations, item, semi) =>
          item.copy(
            annotations = annotations,
            span = span(i1, semi.getOrElse(item.span.end)))
        }

    // NB: Parse `fieldItem` first, so that conflicting keywords (like `privileges`)
    // still parse as fields.
    def item0[_: P]: P[FSL.Node] =
      P(fieldItem | genericItem | funcItem | accessProviderItem | noNameItem | migrationItem | invalidItem)

    // Annotations.
    def annotations[_: P] =
      (spanned(`@` ~~ genericAnn ~~ `(` ~~ schemaName ~~ `)`) {
        case ((keyword, config), span) =>
          FSL.Annotation(keyword, config, span)
      } ~~ nl).repX

    val genericAnns = Set("alias", "role")

    def genericAnn[_: P]: P[Name] =
      P(ident ~~ nl).flatMap { kw =>
        if (genericAnns.contains(kw.str)) {
          Pass(kw)
        } else {
          Fail
        }
      }

    // Schema names.

    // Legacy schema names. Chars derived from fauna.model.Parsing.Chars.
    def schemaName[_: P] = P(ident | legacyName, "identifier")

    def star[_: P] = (Index ~~ `*`) map { i => Name("*", span(i, i + "*".length)) }

    def legacyName[_: P] =
      P(Index ~~ legacyNameQuoted ~~ Index).opaque("name").map {
        case (i1, name, i2) => Name(name, span(i1, i2))
      }

    def legacyNameQuoted[_: P] =
      P(string0("'", legacyNameStr) | string0("\"", legacyNameStr))

    def legacyNameStr[_: P] = P(CharsWhileIn("""a-zA-Z0-9;@+$\-_.!~%""").!)

    // Generic items. This will parse the following:
    //
    // ```
    // collection {}
    // collection Foo {}
    // index myIndex 3
    // terms 3
    // ```
    //
    // This will not parse anything with an invalid keyword.
    def genericItem[_: P]: P[FSL.Node] =
      P(genericKW ~~ (schemaName ~~ ws).? ~~ value.?).map { case (kw, name, value) =>
        FSL.Node(
          Nil,
          kw,
          name,
          value,
          value
            .map(_.span)
            .orElse(name.map(_.span))
            .getOrElse(kw.span)
        )
      }

    val genericKWs = Set(
      "collection",
      "role",
      "index",
      "terms",
      "values",
      "unique",
      "predicate",
      "migrations",
      "privileges",
      "membership",
      "history_days",
      "ttl_days",
      "check")

    def genericKW[_: P]: P[Name] =
      P(ident ~~ nl).flatMap { kw =>
        if (genericKWs.contains(kw.str)) {
          Pass(kw)
        } else {
          Fail
        }
      }

    // Handles functions, as their signature syntax is unique.
    def funcItem[_: P]: P[FSL.Node] =
      P(Index ~~ `function` ~~ schemaName ~~ ws ~~ func).map { case (i1, name, f) =>
        FSL.Node(
          Nil,
          Name("function", span(i1, i1 + "function".length)),
          Some(name),
          Some(f),
          f.span)
      }

    // Handles access providers, as the double keyword is unique.
    def accessProviderItem[_: P]: P[FSL.Node] =
      P(Index ~~ `access` ~~ ws ~~ Index ~~ `provider` ~~ schemaName ~~ ws ~~ value)
        .map { case (i1, i2, name, value) =>
          FSL.Node(
            Nil,
            Name("access provider", span(i1, i2 + "provider".length)),
            Some(name),
            Some(value),
            value.span)
        }

    private def fieldModifier[_: P]: P[(FSL.FieldKind, Option[Span])] =
      (spanned(`compute`) { (_, sp) => sp }.?).map {
        case Some(sp) => (FSL.FieldKind.Compute, Some(sp))
        case None     => (FSL.FieldKind.Defined, None)
      }

    // This is really stretching the definition of an FSL.Node. Fields can look
    // like this:
    // ```
    // compute foo : Int = 3
    // ^^^^^^^ ^^^ ^^^^^   ^ value
    // |       |   | colonType
    // |       | name
    // | modifier
    // ```
    //
    // Because of all the types of fields, almost all of these are optional:
    // ```
    // foo: Int             (only has `name` and `colonType`)
    // bar = 3              (only has `name` and `value`)
    // compute baz: Int = 5 (has `modifier`, `name`, `colonType`, and `value`)
    // ```
    //
    // So instead of making a new `FSL.Node` variant, I just place a sentinel
    // "field" name in the keyword slot, and stick all the other things into
    // `FSL.Field`. This avoids special casing fields named `history_days` and the
    // like, and is pretty easy to work with in the schema item converter.
    //
    // This structure will also support things like private or immutable fields if we
    // ever want anything like that.
    def fieldItem[_: P]: P[FSL.Node] =
      P(fieldModifier ~~ (schemaName | star) ~~ ws ~~ colonPartialType.? ~~ ws ~~ (`=` ~~ lit).?)
        .flatMap { case (kind0, kwSpan, name, ct, value) =>
          val kind = if (name.str == "*") FSL.FieldKind.Wildcard else kind0
          // Defined fields require type signatures or values. We just emit a
          // `Fail` here so that standalone keywords like `read` or `write` fail over
          // to the `noNameItem`.
          if (kind == FSL.FieldKind.Defined && ct.isEmpty && value.isEmpty) {
            P(Fail)
          } else if (kind == FSL.FieldKind.Wildcard && ct.isEmpty) {
            P(Fail)
          } else {
            Pass(
              FSL.Node(
                Nil,
                FSL.Field.Keyword(kwSpan.getOrElse(name.span)),
                Some(name),
                Some(FSL.Field(kind, ct, value)),
                name.span.to(
                  value.map(_.span).getOrElse(ct.map(_.span).getOrElse(name.span)))
              ))
          }
        }

    // Handles items that will cause ambiguity if they have a name. For example:
    // ```
    // privileges {
    //   read
    //   write // this could be considered the `name` of the `read` above
    // }
    //
    // issuer "foo" // this could be considered a quoted name "foo"
    // ```
    def noNameItem[_: P]: P[FSL.Node] =
      P(noNameKW ~~ value.?).map { case (kw, value) =>
        FSL.Node(Nil, kw, None, value, value.map(_.span).getOrElse(kw.span))
      }

    val noNames = Set(
      "create",
      "create_with_id",
      "delete",
      "read",
      "write",
      "history_read",
      "history_write",
      "unrestricted_read",
      "call",
      "issuer",
      "jwks_uri",
      "document_ttls" // Because 'true' and 'false' are keywords.
    )

    def noNameKW[_: P]: P[Name] =
      P(ident ~~ ws).flatMap { name =>
        if (noNames.contains(name.str)) {
          Pass(name)
        } else {
          Fail
        }
      }

    // Handles migrations. These all get similar errors, so they're in a group
    // together.
    // ```
    // migrations {
    //   add .foo
    //   backfill .foo = 3
    //   split .a -> .b, .c, .X,
    //   drop .X
    //   move .c -> .foo
    //   move_conflicts .dump
    //   move_wildcard .dump
    //   add_wildcard
    // }
    // ```
    def migrationItem[_: P]: P[FSL.Node] = migrationKW./.flatMap {
      case kw @ Name("backfill", _) =>
        P(fieldPathExpr ~~ ws ~~ `=` ~~ lit).map { case (path, value) =>
          val sp = kw.span.to(value.span)
          FSL.Node(Nil, kw, None, Some(FSL.Migration.Backfill(path, value, sp)), sp)
        }

      case kw @ Name("drop", _) =>
        fieldPathExpr.map { path =>
          val sp = kw.span.to(path.span.end)
          FSL.Node(Nil, kw, None, Some(FSL.Migration.Drop(path, sp)), sp)
        }

      case kw @ Name("split", _) =>
        def splitInto0 = if (completions) {
          (`->` ~~ nl ~~ fieldPathExpr.repX(sep = `,`)).?.map {
            _.getOrElse(Seq.empty)
          }
        } else {
          `->` ~~ nl ~~ fieldPathExpr.repX(sep = `,`)
        }

        (fieldPathExpr ~~ ws ~~ splitInto0)
          .flatMap { case (outOf, into) =>
            // Massage the span so we can parse splits with no output fields.
            val sp = kw.span.to(into.lastOption.fold(outOf.span.end) { _.span.end })
            Pass(
              FSL.Node(
                Nil,
                kw,
                None,
                Some(FSL.Migration.Split(outOf, into, sp)),
                sp
              ))
          }

      case kw @ Name("move", _) =>
        def moveInto0 = if (completions) {
          (Index ~~ (`->` ~~ fieldPathExpr).?).map {
            case (_, Some(path)) => path
            case (i, None)       => Path(Nil, span(i, i))
          }
        } else {
          `->` ~~ fieldPathExpr
        }

        (fieldPathExpr ~~ ws ~~ moveInto0).map { case (from, to) =>
          val sp = kw.span.to(to.span)
          FSL.Node(Nil, kw, None, Some(FSL.Migration.Move(from, to, sp)), sp)
        }

      case kw @ Name("add", _) =>
        fieldPathExpr.map { path =>
          val sp = kw.span.to(path.span)
          FSL.Node(Nil, kw, None, Some(FSL.Migration.Add(path, sp)), sp)
        }

      case kw @ Name("move_conflicts", _) =>
        fieldPathExpr.map { path =>
          val sp = kw.span.to(path.span)
          FSL.Node(
            Nil,
            kw,
            None,
            Some(FSL.Migration.MoveWildcardConflicts(path, sp)),
            sp)
        }

      case kw @ Name("move_wildcard", _) =>
        fieldPathExpr.map { path =>
          val sp = kw.span.to(path.span)
          FSL.Node(Nil, kw, None, Some(FSL.Migration.MoveWildcard(path, sp)), sp)
        }

      case kw @ Name("add_wildcard", span) =>
        Pass map { _ =>
          FSL.Node(Nil, kw, None, Some(FSL.Migration.AddWildcard(span)), span)
        }

      case _ => sys.error("unreachable")
    }

    val migrationNames = Set(
      "backfill",
      "drop",
      "split",
      "move",
      "add",
      "move_conflicts",
      "move_wildcard",
      "add_wildcard"
    )

    def migrationKW[_: P]: P[Name] =
      P(ident ~~ ws).flatMap { name =>
        if (migrationNames.contains(name.str)) {
          Pass(name)
        } else {
          Fail
        }
      }

    def invalidItem[_: P]: P[FSL.Node] =
      P(ident ~~ ws ~~ Index ~~ InvalidItem.invalidChars ~~ Index)
        .map { case (kw, i1, i2) =>
          FSL.Node(
            Nil,
            kw,
            None,
            Some(FSL.Invalid(span(i1, i2))),
            span(kw.span.start, i2)
          )
        }

    def value[_: P]: P[FSL.Value] = P(block | paths | lit)

    def block[_: P]: P[FSL.Block] =
      P(Index ~~ `{` ~~ items ~~ Index ~~ `}`).map { case (i1, items, i2) =>
        FSL.Block(span(i1, i1 + 1), items, span(i2, i2 + 1))
      }

    def paths[_: P]: P[FSL.Paths] =
      P(Index ~~ `[` ~~ Exprs.expr(true).repX(sep = `,`) ~~ Index ~~ `]`).map {
        case (i1, paths, i2) =>
          FSL.Paths(span(i1, i1 + 1), paths, span(i2, i2 + 1))
      }

    def func[_: P]: P[FSL.Value] =
      P(Index ~~ `(` ~~ args ~~ Index ~~ `)` ~~ colonType.? ~~ nl ~~ exprBlock)
        .map {
          case (i1, (args, varArgs), i2, ret, Right(body)) =>
            FSL.Function(
              span(i1, i1 + 1),
              args,
              varArgs,
              span(i2, i2 + 1),
              ret,
              body
            )
          case (i1, _, _, _, Left(i2)) =>
            FSL.Invalid(span(i1, i2))
        }

    def lit[_: P]: P[FSL.Lit] =
      P(Exprs.expr0(false)).map { expr =>
        FSL.Lit(expr)
      }

    def args[_: P] = P(((arg | varArg).repX(sep = `,`) ~~ `,`.? ~~ nl).map { items =>
      items.zipWithIndex.foreach {
        case (arg @ FSL.VarArg(_, _, _), i) if i != items.length - 1 =>
          emit(ParseError("Variadic argument must be the last argument.", arg.span))
        case _ => ()
      }
      val (args, varArgs) = items.partitionMap {
        case arg: FSL.Arg       => Left(arg)
        case varArg: FSL.VarArg => Right(varArg)
        case _                  => sys.error("unreachable")
      }
      (args, varArgs.headOption)
    })

    def arg[_: P] = (ident ~~ nl ~~ colonType.?).map { case (name, ty) =>
      FSL.Arg(name, ty)
    }
    def varArg[_: P] = (Index ~~ `...` ~~ ident ~~ nl ~~ colonType.?).map {
      case (i1, name, ty) =>
        FSL.VarArg(span(i1, i1 + 3), name, ty)
    }

    def colonType[_: P] = P(Index ~~ `:` ~~ Types.expr0, "a type annotation").map {
      case (i1, ty) =>
        FSL.ColonType(span(i1, i1 + 1), ty)
    }

    def colonPartialType[_: P] =
      P(Index ~~ `:` ~~ PartialTypes.expr0, "a type annotation").map {
        case (i1, ty) =>
          FSL.ColonPartialType(span(i1, i1 + 1), ty)
      }

    def exprBlock[_: P] = P(
      NoCut(exprBlock0).map(Right(_)) |
        (InvalidItem.invalidChars ~~ Index).map(Left(_)))

    def exprBlock0[_: P] = P(Index ~~ `{` ~~ Exprs.blockBody ~~ Index ~~ `}`).map {
      // add 1 to include the `}` without whitespace
      case (i1, body, i2) => Expr.Block(body, span(i1, i2 + 1))
    }
  }
}

object InvalidItem {
  sealed trait Delimiter
  object Paren extends Delimiter
  object Bracket extends Delimiter
  object Brace extends Delimiter

  def invalidChars(implicit ctx: P[_]): P[Unit] = {
    var i = ctx.index
    val start = i
    var done = false

    val stack = Stack.empty[Delimiter]

    // Advance one character
    def eat(n: Int = 1) = i += n

    // Returns the next character, or a NUL if there isn't a next character.
    def peek = if (ctx.input.isReachable(i + 1)) {
      ctx.input(i + 1)
    } else {
      '\u0000'
    }

    // Helper functions to consume things where braces should be ignored. None of
    // these helpers consume the final character, as there is a trailing `eat()` that
    // will be called in the main loop below.

    def consumeLineComment(): Unit =
      while (ctx.input.isReachable(i)) {
        ctx.input(i) match {
          case '\n' | '\r' => return
          case _           => ()
        }
        eat()
      }

    def consumeBlockComment(): Unit = {
      // skip the first '/*'
      eat(2)
      var depth = 1
      while (ctx.input.isReachable(i)) {
        ctx.input(i) match {
          case '/' if peek == '*' => depth += 1
          case '*' if peek == '/' => depth -= 1
          case _                  => ()
        }
        if (depth > 0) {
          eat()
        } else {
          return
        }
      }
    }

    def consumeSingleStr(): Unit = {
      // consume the opening `'`
      eat()
      while (ctx.input.isReachable(i)) {
        ctx.input(i) match {
          case '\\' => eat()
          case '\'' => return
          case _    => ()
        }
        eat()
      }
    }

    def consumeDoubleStr(): Unit = {
      // consume the opening `"`
      eat()
      while (ctx.input.isReachable(i)) {
        ctx.input(i) match {
          case '\\' => eat()
          case '"'  => return
          case _    => ()
        }
        eat()
      }
    }

    while (ctx.input.isReachable(i) && !done) {
      ctx.input(i) match {
        case '\'' => consumeSingleStr()
        case '"'  => consumeDoubleStr()

        case '/' if peek == '/' => consumeLineComment()
        case '/' if peek == '*' => consumeBlockComment()

        case '\r' | '\n' | ';' | '}' if stack.isEmpty => done = true

        // not sure what to do with these, but we'll just ignore them, to avoid `pop`
        // exploding below.
        case ')' | ']' if stack.isEmpty => ()

        case '(' => stack.push(Paren)
        case '[' => stack.push(Bracket)
        case '{' => stack.push(Brace)

        // give up if mismatched. for exprs, we probably want to break if mismatched
        // `)` or `]` are found, but for schema items we really only care about `}`.
        case ')' => stack.pop()
        case ']' => stack.pop()
        case '}' => if (stack.pop() != Brace) { done = true }

        case _ => ()
      }
      if (!done) {
        eat()
      }
    }
    val res = ctx.freshSuccessUnit(index = i)
    if (ctx.verboseFailures) ctx.aggregateTerminal(start, () => "invalid-chars")
    res
  }
}
