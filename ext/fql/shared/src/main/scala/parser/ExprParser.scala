package fql.parser

import fastparse._
import fastparse.NoWhitespace._
import fql.ast._
import fql.ast.Expr.MethodChain
import fql.error.Hint
import fql.error.ParseError
import scala.collection.mutable.{ Map => MMap, Stack }
import scala.collection.BufferedIterator

object ExprParser {

  val ExprOpPrecedences: Map[String, Int] = Seq(
    Seq("??"),
    Seq("||"),
    Seq("&&"),
    Seq("==", "!=", "=~", "!~"),
    Seq(">", "<", ">=", "<="),
    Seq("isa"),
    Seq("|"),
    Seq("^"),
    Seq("&"),
    Seq("+", "-"),
    Seq("*", "/", "%"),
    Seq("**")
  ).zipWithIndex.flatMap { case (ops, i) =>
    ops.map((_, i))
  }.toMap

  val ExprRightAcc = Set("**")

  private[ExprParser] sealed trait Chain
  private[ExprParser] final case class Sel(
    dotSpan: Span,
    field: Name,
    optional: Boolean)
      extends Chain
  private[ExprParser] final case class Bang(span: Span) extends Chain
  private[ExprParser] final case class App(
    args: Seq[Expr],
    optional: Option[Span],
    span: Span)
      extends Chain
  private[ExprParser] final case class Acc(
    args: Seq[Expr],
    optional: Option[Span],
    span: Span)
      extends Chain

  private[ExprParser] final case class Op(
    prec: Int,
    racc: Boolean,
    name: Name,
    rhs: Expr)

  def processFieldPathExpr(expr: Expr) = {
    import Expr.{ ShortLambda, MethodChain, Lit }
    import Expr.MethodChain.{ Select, Access }
    expr match {
      case ShortLambda(MethodChain(_, chain, span)) =>
        var err = Option.empty[ParseError]
        val iter = chain.iterator
        val b = List.newBuilder[PathElem]

        while (err.isEmpty && iter.hasNext) {
          iter.next() match {
            case Select(_, field, false) =>
              b += PathElem.Field(field.str, field.span)
            case Access(Seq(Lit(fql.ast.Literal.Str(str), span)), None, _) =>
              b += PathElem.Field(str, span)
            case Access(Seq(Lit(fql.ast.Literal.Int(num), span)), None, _)
                if num >= 0 && num.isValidLong =>
              b += PathElem.Index(num.longValue, span)
            case c =>
              err = Some(ParseError("Invalid field path", c.span))
          }
        }
        (Path(b.result(), span), err)
      case e =>
        (Path.empty, Some(ParseError("Invalid field path", e.span)))
    }
  }
}

import ExprParser._

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait ExprParser { parser: Parser =>

  def parseQuery[_: P] =
    P(nl ~~ Index ~~ Exprs.queryBody ~~ Index ~~ nl ~~ End)
      .map { case (s, body, e) => Expr.Block(body, span(s, e)) }

  def parseExpr[_: P] = P(nl ~~ Exprs.expr(false) ~~ End)
  def parseExprAllowShortLambda[_: P] = P(nl ~~ Exprs.expr(true) ~~ End)

  def parseFieldPathExpr[_: P] = P(nl ~~ fieldPathExpr ~~ End)

  def parseLambdaExpr[_: P] = P(nl ~~ Exprs.lambdaExpr ~~ End)

  def fieldPathExpr[_: P] = P(Exprs.expr0(true)).map { expr =>
    val (fp, err) = ExprParser.processFieldPathExpr(expr)
    err foreach { fail(_) }
    fp
  }

  trait Util { self: Exprs =>

    protected def strexpr[_: P] = P(nl ~~ expr)

    protected def tupleBody[_: P] = P(`(` ~~ expressionList ~~ `)`./)

    protected def tupleBodyLambdaArgs[_: P] = P(
      `(` ~~ expressionList ~~ (`...` ~~ ident).? ~~ `)`./)

    protected def expressionList[_: P]: P[(Seq[Expr], Boolean)] = {
      implicit val repeater = TupleishRepeater
      (P(expr(shortLambda = true).repX(1, sep = `,`), "expression") ~~ `,`.?).?.map {
        _.getOrElse { (Seq.empty, true) }
      }
    }

    protected def literal[_: P]: P[Expr.Lit] = P(
      nullLit | trueLit | falseLit | numberLit | stringLit)

    private def nullLit[_: P] = P(
      spanned(`null`)((_, s) => Expr.Lit(Literal.Null, s)))
    private def trueLit[_: P] = P(
      spanned(`true`)((_, s) => Expr.Lit(Literal.True, s)))
    private def falseLit[_: P] = P(
      spanned(`false`)((_, s) => Expr.Lit(Literal.False, s)))
    private def numberLit[_: P] = P(spanned(number)(Expr.Lit))
    private def stringLit[_: P] = P(
      spanned(heredocstring | singlestring)((v, s) => Expr.Lit(Literal.Str(v), s)))
  }

  trait ObjectsAndBlocks { self: Exprs =>

    protected def objectOrBlock[_: P] = P(
      Index ~~ `{` ~~ (
        (`}` ~~ Pass).map(_ => Left(Nil)) |
          (objectBody ~~ `}`./).map(Left(_)) |
          (blockBody ~~ `}`./).map(Right(_))
      ) ~~/ Index
    ).map {
      case (s, Left(fields), e) => Expr.Object(fields, span(s, e))
      case (s, Right(body), e)  => Expr.Block(body, span(s, e))
    }

    def queryBody[_: P] = blockBody0(isQuery = true)

    def blockBody[_: P] = blockBody0(isQuery = false)

    def objectFieldName[_: P]: P[Name] = P(
      spanned(plainIdentStr | singlestring | objectFieldDoubleString)(Name))

    // objects

    // FIXME: make this private once PatExprParser does not use this.
    protected[parser] def objectFieldDoubleString[_: P]: P[String] =
      (Index ~~ istring0(strexpr) ~~ Index).flatMap {
        case (_, Seq(), _)        => Pass("")
        case (_, Seq(Left(s)), _) => Pass(s)
        case (s, sections, e) =>
          val sp = span(s, e)
          val (start, interpolated) =
            sections.collectFirst { case Right(v) => v }.get
          fail(
            ParseError(
              "Object keys do not support interpolation",
              interpolated.span,
              hints = Seq(
                Hint(
                  "Add a backslash to ignore the interpolation",
                  span(start, start),
                  Some("\\")),
                Hint("Use Object.fromEntries if you need a dynamic key", sp)
              )
            ))
      }

    private def objectValue0[_: P] = if (completions) {
      (`:` ~~ expr).?.flatMap {
        case Some(e) => Pass(e)

        // Consume newlines if there wasn't a `:`.
        case None => nl.map { _ => Expr.Lit(Literal.Null, Span.Null) }
      }
    } else {
      `:` ~~ expr
    }

    // Scala compiler error means these need to split these up
    private def objectBody0[_: P] =
      P((objectFieldName ~~ ws ~~ objectValue0).repX(min = 1, sep = `,`) ~~ `,`.?)

    private def objectBody[_: P] =
      P(objectBody0, "expression").flatMap { body =>
        val seen = MMap.empty[String, Name]
        var res = Pass(body)
        body.foreach { case (k, _) =>
          seen.get(k.str) match {
            case Some(original) =>
              res = res.flatMap { body =>
                continue(Failure.DuplicateObjectKey(original.span, k.span), body)
              }
            case None => seen += (k.str -> k)
          }
        }
        res
        Pass(body)
      }

    // blocks

    private def letStmt[_: P]: P[Expr.Stmt] =
      P(Index ~~ `let` ~~ ident ~~ nl ~~ (`:` ~~ Types.expr).? ~~ `=` ~~ expr0)
        .flatMap { case (idx, n, tpe, v) =>
          if (Tokens.invalidVariables.contains(n.str)) {
            fail(Failure.InvalidIdent(n))
          } else {
            Pass(Expr.Stmt.Let(n, tpe, v, span(idx, v.span.end)))
          }
        }

    private def exprStmt[_: P]: P[Expr.Stmt] = expr0.map(Expr.Stmt.Expr)

    private def blockBody1[_: P]: P[Seq[Expr.Stmt]] = {
      P((letStmt | exprStmt).repX(sep = blocksep) ~~ blocksep.?)
    }

    private def blockBody0[_: P](isQuery: Boolean) = if (completions) {
      blockBody1
    } else {
      blockBody1.flatMap {
        case Seq() =>
          (nl ~~ Index).flatMap { i =>
            if (isQuery) {
              fail(Failure.Unexpected.EndOfQuery(span(i, i + 1)))
            } else {
              fail(Failure.Unexpected.EndOfBlock(span(i, i + 1)))
            }
          }
        case stmts if stmts.last.isInstanceOf[Expr.Stmt.Expr] =>
          nl.map { _ => stmts }
        // we continue to consume whitespace before failing here in order to
        // fail on the next non WS character.
        case stmts =>
          (nl ~~ Index).flatMap { i =>
            fail(Failure.MissingReturn(span(i, i + 1), stmts.last.span))
          }
      }
    }

    private[ExprParser] def lambdaExpr[_: P]: P[Expr.Lambda] = {
      (letStmt | exprStmt).repX(sep = blocksep).flatMap {
        case Seq() =>
          (nl ~~ Index).flatMap { i =>
            fail(Failure.Unexpected.EndOfQuery(span(i, i + 1)))
          }

        case Seq(Expr.Stmt.Expr(l: Expr.LongLambda)) => nl.map { _ => l }
        case Seq(Expr.Stmt.Expr(Expr.Tuple(Seq(l: Expr.ShortLambda), _))) =>
          nl.map { _ => l }

        // If there's a short lambda missing parenthesis, we can't suggest
        // adding two things at once, so just use the existing error.
        case Seq(Expr.Stmt.Expr(_: Expr.ShortLambda)) if errs.nonEmpty =>
          nl.flatMap { _ => Fail }

        // If there's just one expression, suggest adding `() =>` before their
        // query.
        case Seq(_) =>
          (nl ~~ Index).flatMap { i =>
            fail(Failure.InvalidLambdaDefinition.DeclareLambda(span(0, i)))
          }

        case _ =>
          (nl ~~ Index).flatMap { i =>
            fail(Failure.InvalidLambdaDefinition.BodyMustBeALambda(span(0, i)))
          }
      }
    }
  }

  trait MethodChains { self: Exprs =>
    protected def methodChainRhs[_: P] = P(
      (optRhs | selectRhs | applyRhs | accessRhs | bangRhs).repX)

    protected def anonChainStart[_: P] =
      P(Index ~~ dotNoCut ~~ Index ~~ (select | applyRhs | accessRhs)).map {
        case (s1, e1, ch) =>
          val ths = Expr.This(span(s1, s1))
          val chain = ch match {
            case sel: Sel => sel.copy(dotSpan = span(s1, e1))
            case v        => v
          }
          Expr.ShortLambda(buildMethodChain(ths, Seq(chain)))
      }

    private def selector[_: P] = P(spanned(plainIdentStr)(Name))

    private def selectRhs[_: P] = P(nl ~~ Index ~~ `.` ~~ Index ~~ select).map {
      case (s, e, sel) => sel.copy(dotSpan = span(s, e))
    }
    private def select[_: P] =
      P(selector ~~ ws).map { field =>
        // Note: This null span is set above in SelectRhs.
        Sel(Span.Null, field, false)
      }

    private def applyRhs[_: P] =
      P(Index ~~ `(` ~~ expressionList ~~ `)` ~~ Index ~~ ws)
        .map { case (i1, (args, _), i2) => App(args, None, span(i1, i2)) }

    private def accessRhs[_: P] =
      P(Index ~~ `[` ~~ expressionList ~~ `]` ~~ Index ~~ ws)
        .map { case (i1, (args, _), i2) => Acc(args, None, span(i1, i2)) }

    private def optRhs[_: P] = P(
      (nl ~~ Index ~~ `?.` ~~ Index).flatMapX { case (s1, e1) =>
        applyRhs.map { app => app.copy(optional = Some(span(s1, e1))) } |
          accessRhs.map { acc => acc.copy(optional = Some(span(s1, e1))) } |
          (selector ~~ ws).map { id => Sel(span(s1, e1), id, true) }
      })

    private def bangRhs[_: P] = P(!("!=" | "!~") ~~ Index ~~ `!` ~~ Index ~~ ws)
      .map { case (s, e) => Bang(span(s, e)) }

    final def buildMethodChain(init: Expr, ops: IterableOnce[Chain]): Expr = {
      val iter = ops.iterator.buffered
      if (iter.isEmpty) return init

      def methodChainToChain(mcc: MethodChain.Component): Seq[Chain] = {
        mcc match {
          case MethodChain.Bang(span) => Seq(Bang(span))
          case MethodChain.Select(dotSpan, field, optional) =>
            Seq(Sel(dotSpan, field, optional))
          case MethodChain.Apply(args, optional, span) =>
            Seq(App(args, optional, span))
          case MethodChain.Access(args, optional, span) =>
            Seq(Acc(args, optional, span))
          case MethodChain.MethodCall(
                dotSpan,
                field,
                args,
                fieldOptional,
                applyOptional,
                span) =>
            Seq(
              Sel(dotSpan, field, fieldOptional),
              App(args, applyOptional, span)
            )
        }
      }

      // In certain scenarios, the initial expression can be a method chain ending in
      // a select
      // This is currently possible with short lambdas `.foo()`
      // In order to account for this, we reset the expression and iterator chain so
      // that the code
      // in buildChain will correctly translate that to a method call.
      def getInitExprAndOpIterator(
        init: Expr,
        opIterator: Iterator[Chain]): (Expr, BufferedIterator[Chain]) = {
        init match {
          case MethodChain(e, chain, _) =>
            val combinedChain: Iterator[Chain] =
              chain.flatMap(methodChainToChain).iterator ++ opIterator
            (e, combinedChain.buffered)
          case e =>
            (e, opIterator.buffered)
        }
      }

      def buildChain(init: Expr) = {
        val exprChainBuilder = List.newBuilder[MethodChain.Component]

        val (mcExpr, opIter) = getInitExprAndOpIterator(init, iter)

        while (opIter.hasNext) {
          val op = opIter.next()
          op match {
            case Bang(sp) =>
              exprChainBuilder += MethodChain.Bang(sp)
            case Sel(dotSpan, field, fieldOptional) =>
              opIter.headOption match {
                case Some(App(args, applyOptional, applySpan)) =>
                  exprChainBuilder += MethodChain.MethodCall(
                    dotSpan,
                    field,
                    args,
                    fieldOptional,
                    applyOptional,
                    applySpan)
                  opIter.next()
                case _ =>
                  exprChainBuilder += MethodChain.Select(
                    dotSpan,
                    field,
                    fieldOptional)
              }
            case App(args, optional, sp) =>
              exprChainBuilder += MethodChain.Apply(args, optional, sp)
            case Acc(args, optional, sp) =>
              exprChainBuilder += MethodChain.Access(args, optional, sp)
          }
        }

        val exprChain = exprChainBuilder.result()
        val chainSpan = exprChain.lastOption map { op =>
          span(mcExpr.span.start, op.span.end)
        } getOrElse mcExpr.span

        MethodChain(mcExpr, exprChain, chainSpan)
      }

      init match {
        case Expr.ShortLambda(init) =>
          Expr.ShortLambda(buildChain(init))
        case init => buildChain(init)
      }
    }
  }

  trait OpChains { self: Exprs =>
    protected def opChainRhs[_: P] =
      P((spanned(binaryop | nlbinaryop | kwbinaryop)(Name) ~~ nl ~~/ term).repX)
        .map(_.map { case (op, e) =>
          Op(ExprOpPrecedences(op.str), ExprRightAcc(op.str), op, e)
        })

    private def kwbinaryop[_: P] =
      P((StringIn("isa") ~~ !plainIdentRest).!./).map(_.intern())

    private def nlbinaryop[_: P] = P(
      nl ~~ StringIn("??", "||", "&&", "|", "^", "&").!./
    ).map(_.intern())

    private def binaryop[_: P] = P(
      // need to specifically reject comment starts here, as otherwise / op will try
      // to consume them.
      !("//" | "/*" | "->") ~~
        StringIn(
          "==",
          "!=",
          "=~",
          "!~",
          ">",
          "<",
          ">=",
          "<=",
          "+",
          "-",
          "*",
          "/",
          "%",
          "**").!./
    ).map(_.intern())

    protected def buildOpChain(init: Expr, ops: IterableOnce[Op]): Expr = {
      val iter = ops.iterator
      if (iter.isEmpty) return init

      val exprStack = Stack.empty[Expr]
      val opStack = Stack.empty[Op]
      var isShortLambda = false

      exprStack.push(init)

      def buildOp(op: Name) = {
        val rhs = exprStack.pop() match {
          case Expr.ShortLambda(e) =>
            isShortLambda = true
            e
          case Expr.Tuple(Seq(Expr.ShortLambda(e)), ts) =>
            isShortLambda = true
            Expr.Tuple(Seq(e), ts)
          case e => e
        }

        val lhs = exprStack.pop() match {
          case Expr.ShortLambda(e) =>
            isShortLambda = true
            e
          case Expr.Tuple(Seq(Expr.ShortLambda(e)), ts) =>
            isShortLambda = true
            Expr.Tuple(Seq(e), ts)
          case e => e
        }

        val call = Expr.OperatorCall(lhs, op, Some(rhs), rhs.span)
        exprStack.push(call)
      }

      iter.foreach { op =>
        opStack.popWhile {
          case op0 if op0.prec > op.prec || (op0.prec == op.prec && !op0.racc) =>
            buildOp(op0.name)
            true
          case _ => false
        }
        // even though we don't use the op's rhs past this point, we can save an
        // allocation by reusing the op struct instead of creating a tuple.
        opStack.push(op)
        exprStack.push(op.rhs)
      }

      opStack foreach { op => buildOp(op.name) }
      val e = exprStack.pop()
      if (isShortLambda) Expr.ShortLambda(e) else e
    }
  }

  trait Projection { self: Exprs =>

    type Projection = (Seq[ProjElem], Int)
    type ProjElem = (Name, Expr)

    protected def projectRhs[_: P]: P[Option[Projection]] = {
      implicit val repeater = ProjectElemRepeater
      P(
        (`{` ~~ (
          `*`.map(_ => Nil) |
            P(projectElem.repX(min = 1, sep = `,`) ~~ `,`.?, "identifier")
        ) ~~ `}` ~~ Index ~~ ws).?)
    }

    protected def buildProjection(lhs: Expr, prj: Option[Projection]): Expr =
      prj match {
        case Some((elems, idx)) =>
          def build0(lhs: Expr) =
            if (elems.isEmpty) {
              Expr.ProjectAll(lhs, span(lhs.span.start, idx))
            } else {
              Expr.Project(lhs, elems, span(lhs.span.start, idx))
            }
          lhs match {
            case Expr.ShortLambda(lhs) => Expr.ShortLambda(build0(lhs))
            case lhs                   => build0(lhs)
          }
        case _ => lhs
      }

    private def projectElem[_: P]: P[ProjElem] = P(
      (ident ~~ ws ~~ `:` ~~ expr(shortLambda = true) ~~ nl) |
        (ident ~~ ws ~~ projectRhs ~~ nl).map(buildProjElem)
    )

    private def buildProjElem(t: (Name, Option[Projection])): ProjElem = {
      val (l, prj) = t
      val sp = span(l.span.start, l.span.start)
      val e = Expr.MethodChain(
        Expr.This(sp),
        Seq(MethodChain.Select(sp, l, false)),
        l.span
      )

      (l, buildProjection(e, prj))
    }
  }

  // fastparse macro expansion introduces unused vars. ignore those
  @annotation.nowarn("msg=pattern var charIn*")
  trait Terms { self: Exprs =>

    protected def baseterm[_: P]: P[Expr] = P(baseterm1 | unaryOp)
    protected def term[_: P]: P[Expr] = P(term1 | unaryOp)

    private def baseterm1[_: P] =
      P(baseterm0 ~~ methodChainRhs).map { case (e, chain) =>
        buildMethodChain(e, chain)
      }

    private def term1[_: P] = P(term0 ~~ methodChainRhs)
      .map { case (e, chain) => buildMethodChain(e, chain) }

    private def placeholder[_: P] =
      Option.when(isTemplate) {
        P(spanned(TemplateSigil.Value) { case (_: Unit, span) =>
          nextTemplateValue(span)
        })
      }

    private def baseterm0[_: P] =
      placeholder match {
        case Some(ph) =>
          P((ph | literal | strtemplate | objectOrBlock | array | anonChainStart) ~~ ws)
        case None =>
          P((literal | strtemplate | objectOrBlock | array | anonChainStart) ~~ ws)
      }
    private def term0[_: P] = P((baseterm0 | tuple | identifier) ~~ ws)

    private def identifier[_: P] = P(ident)
      .map { id => Expr.Id(id.str, id.span) }

    private def strtemplate[_: P] =
      P(Index ~~ (istring(strexpr) | heredocistring(strexpr)) ~~ Index)
        .map {
          case (i1, Nil, i2)            => Expr.Lit(Literal.Str(""), span(i1, i2))
          case (i1, Seq(Left(str)), i2) => Expr.Lit(Literal.Str(str), span(i1, i2))
          case (i1, v, i2)              => Expr.StrTemplate(v, span(i1, i2))
        }

    private def array[_: P] =
      P(Index ~~ `[` ~~ expr.repX(sep = `,`) ~~ `,`.? ~~ `]` ~~ Index)
        .map { case (i1, es, i2) => Expr.Array(es, span(i1, i2)) }

    private def tuple[_: P] = P(Index ~~ tupleBody ~~ Index).map {
      case (s, (elems, _), e) =>
        Expr.Tuple(elems, span(s, e))
    }

    private def unaryOp[_: P]: P[Expr] =
      P(spanned(CharIn("\\-~!").!)(Name) ~~/ ws ~~ term)
        .map {
          case (op, Expr.ShortLambda(e)) =>
            Expr.ShortLambda(
              Expr.OperatorCall(e, op, None, span(op.span.start, e.span.end)))
          case (op, e) =>
            Expr.OperatorCall(e, op, None, span(op.span.start, e.span.end))
        }
  }

  abstract class Exprs
      extends Util
      with ObjectsAndBlocks
      with MethodChains
      with OpChains
      with Projection
      with Terms {
    type ExprRhs = (Seq[Chain], Seq[Op], Option[this.Projection])

    def expr[_: P]: P[Expr] = expr(false)
    def expr[_: P](shortLambda: Boolean): P[Expr] =
      P(expr0(shortLambda) ~~ nl, "expression")

    protected[parser] def expr0[_: P]: P[Expr] = expr0(false)
    protected[parser] def expr0[_: P](shortLambda: Boolean): P[Expr] =
      P(ifExpr | atExpr | matchExpr | baseExpr | identOrLambda | tupleOrLambda)
        .flatMap {
          case lambda: Expr.ShortLambda if !shortLambda =>
            continue(Failure.InvalidShortLambda(lambda.span), lambda)
          case other => Pass(other)
        }

    private def ifExpr[_: P] =
      P(Index ~~ `if` ~~ `(` ~~ expr ~~ `)` ~~ nl ~~ expr0 ~~ (nl ~~ `else` ~~ expr0).?)
        .map {
          case (idx, pred, e, None) => Expr.If(pred, e, span(idx, e.span.end))
          case (idx, pred, e1, Some(e2)) =>
            Expr.IfElse(pred, e1, e2, span(idx, e2.span.end))
        }

    private def atExpr[_: P] =
      P(Index ~~ `at` ~~ `(` ~~ expr ~~ `)` ~~ nl ~~ expr0)
        .map { case (idx, ts, body) =>
          Expr.At(ts, body, span(idx, body.span.end))
        }

    private def matchExpr[_: P] =
      P(Index ~~ `match` ~~ matchScrut ~~ matchBody ~~ Index ~~ ws./)
        .map { case (i1, scrut, branches, i2) =>
          Expr.Match(scrut, branches, span(i1, i2))
        }
    private def matchScrut[_: P] = P(term ~~ methodChainRhs)
      .map { case (e, chain) => buildMethodChain(e, chain) }
    private def matchBody[_: P] = P(
      `{` ~~ matchCase.repX(sep = blocksep) ~~ blocksep.? ~~ `}`)
    private def matchCase[_: P] = P(`case` ~~ Patterns.expr ~~ `=>` ~~ expr0)

    private def baseExpr[_: P] = P(baseterm ~~ exprRhs, "expression")
      .map { case (e, rhs) => buildExpr(e, rhs) }

    private def identOrLambda[_: P] = P(ident ~~ ws ~~ lambdaArrow).flatMapX {
      case (p, Some(b)) =>
        val param = Option.when(p.str != Expr.Underscore.name)(p)
        Pass(Expr.LongLambda(List(param), None, b, span(p.span.start, b.span.end)))
      case (id, None) =>
        exprRhs.map(buildExpr(Expr.Id(id.str, id.span), _))
    }

    private def tupleOrLambda[_: P] =
      P(Index ~~ tupleBodyLambdaArgs ~~ Index ~~ ws).flatMapX {
        case (i1, (_, false, Some(_)), i2) =>
          fail(ParseError("invalid variadic tuple", span(i1, i2)))
        case (i1, (elems, false, None), i2) =>
          exprRhs.map(buildExpr(Expr.Tuple(elems, span(i1, i2)), _))
        case (i1, (elems, true, variOpt), i2) =>
          lambdaArrow.flatMapX {
            case Some(body) =>
              val ids = elems.asInstanceOf[Seq[Expr.Id]].map { e =>
                Option.when(e.name.str != Expr.Underscore.name)(e.name)
              }
              val vari = variOpt map { n =>
                Option.when(n.str != Expr.Underscore.name)(n)
              }
              val lambda = Expr.LongLambda(ids, vari, body, span(i1, body.span.end))
              val seen = MMap.empty[String, Name]
              var res = Pass(lambda)
              (ids ++ vari).foreach {
                _.foreach { id =>
                  seen.get(id.str) match {
                    case Some(original) =>
                      res = res.flatMap { lambda =>
                        continue(
                          Failure.DuplicateLambdaArgument(original.span, id.span),
                          lambda)
                      }
                    case None => seen += (id.str -> id)
                  }
                }
              }
              res
            case None =>
              exprRhs.map(buildExpr(Expr.Tuple(elems, span(i1, i2)), _))
          }
      }

    private def exprRhs[_: P] = P(methodChainRhs ~~ opChainRhs ~~ projectRhs)
    private def buildExpr(e: Expr, rhs: ExprRhs): Expr = {
      val (apps, ops, prj) = rhs
      val e0 = buildMethodChain(e, apps)
      val e1 = buildOpChain(e0, ops)
      buildProjection(e1, prj)
    }

    private def lambdaArrow[_: P] = P((`=>` ~~ expr0).?)
  }

  object Exprs extends Exprs
}
