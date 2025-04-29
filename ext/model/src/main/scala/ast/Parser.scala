package fauna.ast

import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.runtime.Effect
import fauna.model.RefParser.RefScope._
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.trace.GlobalTracer
import fauna.util.Base64
import io.netty.buffer.Unpooled
import java.time.{ DateTimeException, LocalDate }
import java.util.UUID
import scala.annotation.unused

// @formatter:off

object Parser {
  def sequenceT[E, V](exprs: List[Query[Either[List[E], V]]]): Query[Either[List[E], List[V]]] = {
    val errs = List.empty[E]
    val vs = List.empty[V]

    exprs.accumulateEitherT(errs, vs) { _ ++ _ } { case (acc, v) => v +: acc } mapT { _.reverse }
  }

  def sequence[E, V](exprs: List[Either[List[E], V]]): Either[List[E], List[V]] = {
    val errs = List.empty[E]
    val vs = List.empty[V]

    exprs.accumulateEither(errs, vs) { _ ++ _ } { case (acc, v) => v +: acc } map { _.reverse }
  }

  def collectErrors[E](es: Either[List[E], _]*): Either[List[E], Nothing] =
    Left(es.view.collect { case Left(e) => e }.flatten.toList)
}

/**
  * Expands "escaped" symbols (those beginning with "@") in the input
  * into our internal representation. These typically represent value
  * types in the language, but also includes literal query and object
  * expressions.
  *
  * The primary - and ideally only - entry point is `parse()`.
  */
object EscapesParser {

  def parse(auth: Auth, input: Literal, apiVersion: APIVersion, pos: Position = RootPosition): Query[PResult[Literal]] =
    input match {
      case ArrayL(vals)   => parseArray(auth, vals, apiVersion, pos)
      case ObjectL(alist) => escapes(alist, (auth, pos), apiVersion) getOrElse parseObject(auth, alist, apiVersion, pos)
      case _              => PResult.successfulQ(input)
    }

  private def parseAndCast[T](auth: Auth, input: Literal, cast: TypeCast[T], apiVersion: APIVersion, pos: Position): Query[PResult[T]] =
    parse(auth, input, apiVersion, pos) flatMapT { r =>
      Query.value(cast(r, pos) match {
        case Right(t)   => PResult.Success(t)
        case Left(errs) => PResult.Failure(errs map ParseEvalError)
      })
    }

  private def parseArray(auth: Auth, values: List[Literal], apiVersion: APIVersion, pos: Position): Query[PResult[Literal]] = {
    val arr = values.view.zipWithIndex
      .map { case (v, i) => parse(auth, v, apiVersion, pos at i) }
    arr.sequenceT mapT { ArrayL(_) }
  }

  private def parseObject(auth: Auth, alist: List[(String, Literal)], apiVersion: APIVersion, pos: Position): Query[PResult[ObjectL]] = {
    val obj = alist map { case (k, v) => parse(auth, v, apiVersion, pos at k) mapT { k -> _ } }
    obj.sequenceT mapT { ObjectL(_) }
  }

  // public for REST pagination params and QueryParser
  def parseRef(auth: Auth, str: String, @unused apiVersion: APIVersion, pos: Position): Query[PResult[Literal]] = {
    LegacyRefParser.Ref.parse(str) match {
      case Some(ref) => transformRef(auth, ref, pos)
      case None      => PResult.failedQ(InvalidRefExpr(pos))
    }
  }

  // public for QueryParser
  def parseRef(auth: Auth, obj: ObjectL, apiVersion: APIVersion, pos: Position): Query[PResult[Literal]] =
    RefParser.parse(obj, pos, apiVersion) match {
      case Right(ref) => transformRef(auth, ref, pos)
      case Left(errs) => PResult.failedQ(errs map ParseEvalError)
    }

  private def transformRef(auth: Auth, parsedRef: Ref, pos: Position): Query[PResult[Literal]] =
    parsedRef match {
      // TODO: remove once keys can be self referenced
      case SelfRef(KeyClassRef) =>
        PResult.failedQ(InvalidRefExpr(pos))

      // self-refs
      case ref @ SelfRef(_) =>
        ModelData.lookupRef(auth, ref) map {
          case Some(ref) => PResult.Success(ref)
          case None      => PResult.Failure(InvalidRefExpr(pos))
        }

      case ref =>
        ModelData.lookupRef(auth, ref) map {
          case Some(ref) => PResult.Success(ref)
          case None      => PResult.Success(UnresolvedRefL(parsedRef))
        }
      }

  // public for QueryParser
  def parseSet(auth: Auth, alist: List[(String, Literal)], apiVersion: APIVersion, pos: Position): Query[PResult[Literal]] =
    sets(alist, (auth, pos), apiVersion) getOrElse PResult.failedQ(InvalidSetExpr(pos)) mapT { SetL(_) }

  // public for QueryParser, Expression
  def parseLambda(auth: Auth, lit: Literal, apiVersion: APIVersion, pos: Position): Query[PResult[LambdaL]] =
    lit match {
      case ObjectL(alist) =>
        lambdas(alist, (auth, pos), apiVersion) getOrElse PResult.failedQ(InvalidLambdaExpr(pos))
      case _ =>
        PResult.failedQ(InvalidLambdaExpr(pos))
    }

  private val lambdas = MapRouter.build[Literal, Query[PResult[LambdaL]], (Auth, Position)] { lambdas =>
    lambdas.add("lambda", "expr") {
      case (params, expr, (auth, pos), apiVersion) =>
        QueryParser.parseLambdaExpr(auth, params, expr, apiVersion, pos) flatMap {
          case PResult.Success(le, _) =>
            if (le.free.isEmpty) {
              PResult.successfulQ(LambdaL(le))
            } else {
              PResult.failedQ(InvalidLambdaExpr(pos))
            }
          case e => PResult.failedQ(e.getErrors)
        }
    }

    lambdas.add("lambda", "expr", "api_version") {
      case (params, expr, StringL(APIVersion(apiVersion)), (auth, pos), _) =>
        QueryParser.parseLambdaExpr(auth, params, expr, apiVersion, pos) flatMap {
          case PResult.Success(le, _) =>
            if (le.free.isEmpty) {
              PResult.successfulQ(LambdaL(le))
            } else {
              PResult.failedQ(InvalidLambdaExpr(pos))
            }
          case e => PResult.failedQ(e.getErrors)
        }
      case (_, _, _, (_, pos), _) =>
        PResult.failedQ(InvalidLambdaExpr(pos))
    }
  }

  private val escapes = MapRouter.build[Literal, Query[PResult[Literal]], (Auth, Position)] { escapes =>
    escapes.add("@obj") {
      case (ObjectL(alist), (auth, pos), apiVersion) => parseObject(auth, alist, apiVersion, pos)
      case (_, (_, pos), _)                          => PResult.failedQ(InvalidObjectExpr(pos))
    }

    escapes.add("@ref") {
      case (StringL(str), (auth, pos), apiVersion)     => parseRef(auth, str, apiVersion, pos)
      case (obj @ ObjectL(_), (auth, pos), apiVersion) => parseRef(auth, obj, apiVersion, pos)
      case (_, (_, pos), _)                            => PResult.failedQ(InvalidRefExpr(pos))
    }

    escapes.add("@set") {
      case (ObjectL(alist), (auth, pos), apiVersion) => parseSet(auth, alist, apiVersion, pos)
      case (_, (_, pos), _)                          => PResult.failedQ(InvalidSetExpr(pos))
    }

    escapes.add("@ts") {
      case (StringL(str), (_, pos), _) =>
        try {
          PResult.successfulQ(TimeL(Timestamp.parse(str)))
        } catch {
          case _: DateTimeException =>
            PResult.failedQ(InvalidTimeExpr(pos))
        }
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidTimeExpr(pos))
    }

    escapes.add("@date") {
      case (StringL(str), (_, pos), _) =>
        try {
          PResult.successfulQ(DateL(LocalDate.parse(str)))
        } catch {
          case _: DateTimeException =>
            PResult.failedQ(InvalidDateExpr(pos))
        }
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidDateExpr(pos))
    }

    escapes.add("@query") {
      case (lambda, (auth, pos), _) =>
        parseLambda(auth, lambda, APIVersion.LambdaDefaultVersion, pos)
    }

    escapes.add("@bytes") {
      case (StringL(str), (_, _), _) =>
        PResult.successfulQ(BytesL(Unpooled.wrappedBuffer(Base64.decode(str))))
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidBytesExpr(pos))
    }

    escapes.add("@uuid") {
      case (StringL(str), (_, pos), _) =>
        try {
          PResult.successfulQ(UUIDL(UUID.fromString(str)))
        } catch {
          case _: IllegalArgumentException =>
            PResult.failedQ(InvalidUUIDExpr(pos))
        }
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidUUIDExpr(pos))
    }
  }

  private def liftSetR(r: Query[R[SetL]]): Query[PResult[EventSet]] =
    r map {
      case Right(SetL(set)) => PResult.Success(set)
      case Left(errs)       => PResult.Failure(errs map { ParseEvalError(_) })
    }

  private val sets = MapRouter.build[Literal, Query[PResult[EventSet]], (Auth, Position)] { sets =>

    sets.add("match", "index") {
      case (term, idx, (auth, pos), apiVersion) =>
        (parseAndCast(auth, term, Casts.ZeroOrMore(Casts.MatchTerm), apiVersion, pos at "match"),
          parseAndCast(auth, idx, Casts.StringOrIndexRef, apiVersion, pos at "index")) parT { (term, idx) =>
          liftSetR(MatchFunction(term, idx, auth, pos))
        }
    }

    sets.add("match", "terms") {
      case (idx, term, (auth, pos), apiVersion) =>
        val parsedTerm = parseAndCast(auth, term, Casts.ZeroOrMore(Casts.MatchTerm), apiVersion, pos at "terms")
        val parsedIndex = parseAndCast(auth, idx, Casts.StringOrIndexRef, apiVersion, pos at "match")
        (parsedTerm, parsedIndex) parT { (term, idx) =>
          liftSetR(MatchFunction(term, idx, auth, pos))
        }
    }

    sets.add("match") {
      case (idx, (auth, pos), apiVersion) =>
        parseAndCast(auth, idx, Casts.StringOrIndexRef, apiVersion, pos at "match") flatMapT { idx =>
          liftSetR(MatchFunction(Nil, idx, auth, pos))
        }
    }

    sets.add("singleton") {
      case (set, (auth, pos), apiVersion) =>
        parseAndCast(auth, set, Casts.Ref, apiVersion, pos at "singleton") flatMapT { ref =>
          liftSetR(SingletonFunction(ref, auth, pos))
        }
    }

    sets.add("events") {
      case (set, (auth, pos), apiVersion) =>
        parseAndCast(auth, set, Casts.Identifier, apiVersion, pos at "events") flatMapT { set =>
          liftSetR(EventsFunction(set, auth, pos))
        }
    }

    sets.add("documents") {
      case (set, (auth, pos), apiVersion) =>
        parseAndCast(auth, set, Casts.ClassRef, apiVersion, pos at "documents") flatMapT { ref =>
          liftSetR(DocumentsFunction(ref, auth, pos))
        }
    }

    sets.add("union") {
      case (sets, (auth, pos), apiVersion) =>
        parseAndCast(auth, sets, Casts.OneOrMore(Casts.Set), apiVersion, pos at "union") flatMapT { sets =>
          liftSetR(UnionFunction(sets, pos))
        }
    }

    sets.add("intersection") {
      case (sets, (auth, pos), apiVersion) =>
        parseAndCast(auth, sets, Casts.OneOrMore(Casts.Set), apiVersion, pos at "intersection") flatMapT { sets =>
          liftSetR(IntersectionFunction(sets, pos))
        }
    }

    sets.add("difference") {
      case (sets, (auth, pos), apiVersion) =>
        parseAndCast(auth, sets, Casts.OneOrMore(Casts.Set), apiVersion, pos at "difference") flatMapT { sets =>
          liftSetR(DifferenceFunction(sets, pos))
        }
    }

    sets.add("distinct") {
      case (set, (auth, pos), apiVersion) =>
        parseAndCast(auth, set, Casts.Set, apiVersion, pos at "distinct") flatMapT { set =>
          liftSetR(DistinctFunction(set, pos))
        }
    }

    sets.add("reverse") {
      case (set, (auth, pos), apiVersion) =>
        parseAndCast(auth, set, Casts.Set, apiVersion, pos at "reverse") flatMapT { set =>
          liftSetR(ReverseFunction(set, pos))
        }
    }

    sets.add("range", "from", "to") {
      case (set, from, to, (auth, pos), apiVersion) =>
        val parsedSet = parseAndCast(auth, set, Casts.Set, apiVersion, pos at "range")
        val parsedFrom = parse(auth, from, apiVersion, pos at "from")
        val parsedTo = parse(auth, to, apiVersion, pos at "to")

        (parsedSet, parsedFrom, parsedTo) parT { (set, from, to) =>
          liftSetR(FunctionAdapters.Range(set, from, to, auth.scopeID, apiVersion, pos))
        }
    }

    // FIXME: `with` in joinset literals is a raw lambda expr construction, not
    // an encoded lambda value ("@query" tagged object). This can be fixed when
    // redoing the wire protocol.
    sets.add("join", "with") {
      case (source, lambda, (auth, pos), apiVersion) =>
        (parseAndCast(auth, source, Casts.Set, apiVersion, pos at "join"),
          parseLambda(auth, lambda, apiVersion, pos at "with")) parT { (src, lambda) =>
          liftSetR(JoinFunction(src, lambda, pos))
        }
    }

    // FIXME: `filter` in filterset literals is a raw lambda expr construction,
    // not an encoded lambda value ("@query" tagged object). This can be fixed
    // when redoing the wire protocol.
    sets.add("filter", "collection") {
      case (lambda, source, (auth, pos), apiVersion) =>
        (parseLambda(auth, lambda, apiVersion, pos at "filter"),
          parseAndCast(auth, source, Casts.Set, apiVersion, pos at "collection")) parT { (lambda, src) =>
          liftSetR(FilterFunction(lambda, src, auth, pos))
        }
    }
  }
}

/**
  * Expands symbols in the input into an expression tree, which can be
  * evaluated by the interpreter in `EvalContext`.
  *
  * The primary - and ideally only - entry point is `parse()`.
  */
object QueryParser {

  def parse(auth: Auth, input: Literal, apiVersion: APIVersion, pos: Position = RootPosition): Query[Either[List[ParseError], Expression]] =
    parseExpr(auth, input, apiVersion, pos) map { _.toEither }

  private def parseExpr(
    auth: Auth,
    input: Literal,
    apiVersion: APIVersion,
    pos: Position): Query[PResult[Expression]] = {

    // Put a lazy-bound into the evaluation of the current literal to avoid
    // stack-overflow during recursive evaluation of inner expressions.
    Query.defer {
      input match {
        case ObjectL(alist) =>
          forms(alist, (auth, pos), apiVersion) orElse {
            escapes(alist, (auth, pos), apiVersion) map {
              _ flatMapT {
                case l @ ObjectL(_) => parseExpr(auth, l, apiVersion, pos at "@obj")
                case l              => PResult.successfulQ(l)
              }
            }
          } getOrElse {
            PResult.failedQ(InvalidFormExpr(alist map { _._1 }, pos))
          }

        case ArrayL(vals) => parseList(auth, vals, apiVersion, pos) mapT { ArrayE(_) }
        case other        => PResult.successfulQ(other)
      }
    }
  }

  private def parseOpt(auth: Auth, input: Option[Literal], apiVersion: APIVersion, pos: Position): Query[PResult[Option[Expression]]] =
    input match {
      case Some(inp) => parseExpr(auth, inp, apiVersion, pos) mapT { Some(_) }
      case None      => PResult.successfulQ(None)
    }

  private def parseList(auth: Auth, values: List[Literal], apiVersion: APIVersion, pos: Position): Query[PResult[List[Expression]]] = {
    val parsedExpressions = values.view.zipWithIndex
      .map { case (v, i) => parseExpr(auth, v, apiVersion, pos at i) }
    parsedExpressions.sequenceT
  }

  private val lambdas = MapRouter.build[Literal, Query[PResult[LambdaE]], (Auth, Position)] { lambdas =>
    lambdas.add("lambda", "expr") {
      case (params, expr, (auth, pos), apiVersion) => parseLambdaExpr(auth, params, expr, apiVersion, pos)
    }
  }

  private def parseLambda(auth: Auth, lambda: Literal, apiVersion: APIVersion, pos: Position): Query[PResult[LambdaE]] =
    lambda match {
      case ObjectL(alist) =>
        lambdas(alist, (auth, pos), apiVersion) getOrElse {
          PResult.failedQ(InvalidFormExpr(alist map { _._1 }, pos))
        }
      case v =>
        PResult.failedQ(InvalidExprType(List(Type.Lambda), v.rtype, pos))
    }

  // public for EscapesParser
  def parseLambdaExpr(auth: Auth, params: Literal, expr: Literal, apiVersion: APIVersion, pos: Position): Query[PResult[LambdaE]] = {
    val paramsE = Query.value(parseLambdaPat(params, pos at "lambda"))
    val exprE = parseExpr(auth, expr, apiVersion, pos at "expr")

    (paramsE, exprE) par {
      case (PResult.Success(ps, _), PResult.Success(expr, vars)) =>
        val free = vars -- ps.params
        PResult.successfulQ(LambdaE(ps, expr, free, apiVersion, pos), free)

      case (e1, e2) =>
        PResult.failedQ(e1.getErrors ++ e2.getErrors)
    }
  }

  private def parseLambdaPat(ps: Literal, pos: Position): PResult[LambdaPat] =
    ps match {
      case StringL("_") => PResult.Success(LambdaNullPat)
      case StringL(v)   => PResult.Success(LambdaScalarPat(v))

      case ArrayL(vs) =>
        val lambdas = vs.view.zipWithIndex.map {
          case (StringL("_"), _) => PResult.Success(LambdaNullPat)
          case (StringL(v), _)   => PResult.Success(LambdaScalarPat(v))
          case (v, i)            => PResult.Failure(InvalidExprType(List(Type.String), v.rtype, pos at i))
        }
        lambdas.sequence.map { ps => LambdaArrayPat(ps.toList) }

      case v =>
        PResult.Failure(InvalidExprType(List(Type.String, Type.Array), v.rtype, pos))
    }

  private val escapes = MapRouter.build[Literal, Query[PResult[Literal]], (Auth, Position)] { escapes =>
    escapes.add("@obj") {
      case (l @ ObjectL(_), (_, _), _) => PResult.successfulQ(l)
      case (_, (_, pos), _)                 => PResult.failedQ(InvalidObjectExpr(pos))
    }

    escapes.add("@ref") {
      case (StringL(str), (auth, pos), apiVersion)     => EscapesParser.parseRef(auth, str, apiVersion, pos)
      case (obj @ ObjectL(_), (auth, pos), apiVersion) => EscapesParser.parseRef(auth, obj, apiVersion, pos)
      case (_, (_, pos), _)                            => PResult.failedQ(InvalidRefExpr(pos))
    }

    escapes.add("@set") {
      case (ObjectL(alist), (auth, pos), apiVersion) => EscapesParser.parseSet(auth, alist, apiVersion, pos)
      case (_, (_, pos), _)                          => PResult.failedQ(InvalidSetExpr(pos))
    }

    escapes.add("@ts") {
      case (StringL(str), (_, pos), _) =>
        try {
          PResult.successfulQ(TimeL(Timestamp.parse(str)))
        } catch {
          case _: DateTimeException =>
            PResult.failedQ(InvalidTimeExpr(pos))
        }
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidTimeExpr(pos))
    }

    escapes.add("@date") {
      case (StringL(str), (_, pos), _) =>
        try {
          PResult.successfulQ(DateL(LocalDate.parse(str)))
        } catch {
          case _: DateTimeException =>
            PResult.failedQ(InvalidDateExpr(pos))
        }
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidDateExpr(pos))
    }

    escapes.add("@query") {
      case (lambda, (auth, pos), _) =>
        EscapesParser.parseLambda(auth, lambda, APIVersion.LambdaDefaultVersion, pos)
    }

    escapes.add("@bytes") {
      case (StringL(str), (_, _), _) =>
        PResult.successfulQ(BytesL(Unpooled.wrappedBuffer(Base64.decode(str))))
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidBytesExpr(pos))
    }

    escapes.add("@uuid") {
      case (StringL(str), (_, pos), _) =>
        try {
          PResult.successfulQ(UUIDL(UUID.fromString(str)))
        } catch {
          case _: IllegalArgumentException =>
            PResult.failedQ(InvalidUUIDExpr(pos))
        }
      case (_, (_, pos), _) =>
        PResult.failedQ(InvalidUUIDExpr(pos))
    }
  }

  private val forms = MapRouter.build[Literal, Query[PResult[Expression]], (Auth, Position)] { forms =>
    forms.add("at", "expr") {
      case (ts, expr, (auth, pos), apiVersion) =>
        (parseExpr(auth, ts, apiVersion, pos at "at"), parseExpr(auth, expr, apiVersion, pos at "expr")) parT { (ts, expr) =>
          PResult.successfulQ(AtE(ts, expr, pos))
        }
    }

    forms.add("var") {
      case (StringL(s), (_, pos), _) => PResult.successfulQ(VarE(s), FreeVars(s, pos))
      case (name, (_, pos), _)       => PResult.failedQ(List(InvalidExprType(List(Type.String), name.rtype, pos at "var")))
    }

    forms.add("let", "in") {
      case (bindings, expr, (auth, pos), apiVersion) =>
        val exprE: Query[PResult[Expression]] = parseExpr(auth, expr, apiVersion, pos at "in")
        val bindingsE: Query[Seq[(String, PResult[Expression])]] =
          bindings match {
            case ArrayL(elems) =>
              val expressions = elems.view.zipWithIndex.map {
                case (ObjectL(List((n, b))), i) =>
                  parseExpr(auth, b, apiVersion, pos at "let" at i at n) map { n -> _ }
                case (b, i) =>
                  Query.value("" -> PResult.Failure(List(InvalidExprType(List(Type.Object), b.rtype, pos at "let" at i))))
              }
              expressions.sequence

            case ObjectL(alist) =>
              escapes(alist, (auth, pos at "let"), apiVersion) map {
                _ flatMapT {
                  case ObjectL(o) => PResult.successfulQ(o)
                  case r          => PResult.failedQ(InvalidExprType(List(Type.Object), r.rtype, pos))
                }
              } getOrElse {
                PResult.successfulQ(alist)
              } flatMap {
                case PResult.Success(alist, _) =>
                  (alist map { case (k, v) => parseExpr(auth, v, apiVersion, pos at "let" at k) map { k -> _ } }).sequence
                case f @ PResult.Failure(_) =>
                  Query.value(List("" -> f))
              }

            case bindings =>
              Query.value(List("" -> PResult.Failure(List(InvalidExprType(List(Type.Array), bindings.rtype, pos at "let")))))
          }

        (exprE, bindingsE) par { (expr, bs) =>
          val bindings = List.newBuilder[(String, Expression)]
          val errs = List.newBuilder[ParseError]
          var bound = Set.empty[String]
          var bfree = FreeVars.empty

          bs foreach {
            case (n, PResult.Success(v, f)) =>
              bindings += (n -> v)
              bfree = bfree | (f -- bound)
              bound = bound + n

            case (_, PResult.Failure(es)) =>
              errs ++= es
          }

          (errs.result(), expr) match {
            case (Nil, PResult.Success(v, efree)) =>
              PResult.successfulQ(LetE(bindings.result(), v), bfree | (efree -- bound))

            case (errs, expr) =>
              PResult.failedQ(errs ++ expr.getErrors)
          }
        }
    }

    forms.add("if", "then", "else") {
      case (condR, thenR, elseR, (auth, pos), apiVersion) =>
        val condE = parseExpr(auth, condR, apiVersion, pos at "if")
        val thenE = parseExpr(auth, thenR, apiVersion, pos at "then")
        val elseE = parseExpr(auth, elseR, apiVersion, pos at "else")

        (condE, thenE, elseE) parT { (c, t, e) =>
          PResult.successfulQ(IfE(c, t, e, pos))
        }
    }

    forms.add("and", APIVersion.introducedOn(APIVersion.V3)) {
      case (ArrayL(exprs), (auth, pos), apiVersion) =>
        parseList(auth, exprs, apiVersion, pos at "and") flatMapT {
          case Nil   => PResult.failedQ(EmptyExpr(pos at "and"))
          case exprs => PResult.successfulQ(AndE(exprs, pos))
        }

      case (expr, (auth, pos), apiVersion) =>
        parseExpr(auth, expr, apiVersion, pos at "and") mapT { expr =>
          AndE(List(expr), pos)
        }
    }

    forms.add("or", APIVersion.introducedOn(APIVersion.V3)) {
      case (ArrayL(exprs), (auth, pos), apiVersion) =>
        parseList(auth, exprs, apiVersion, pos at "or") flatMapT {
          case Nil   => PResult.failedQ(EmptyExpr(pos at "or"))
          case exprs => PResult.successfulQ(OrE(exprs, pos))
        }

      case (expr, (auth, pos), apiVersion) =>
        parseExpr(auth, expr, apiVersion, pos at "or") mapT { expr =>
          OrE(List(expr), pos)
        }
    }

    forms.add("quote") {
      case (ir, (auth, pos), apiVersion) => EscapesParser.parse(auth, ir, apiVersion, pos at "quote")
    }

    forms.add("object") {
      case (ObjectL(alist), (auth, pos), apiVersion) =>
        escapes(alist, (auth, pos at "object"), apiVersion) map {
          _ flatMapT {
            case ObjectL(o) => PResult.successfulQ(o)
            case r          => PResult.failedQ(InvalidExprType(List(Type.Object), r.rtype, pos))
          }
        } getOrElse {
          PResult.successfulQ(alist)
        } flatMapT { alist =>
          val parsed = alist map { case (k, v) => parseExpr(auth, v, apiVersion, pos at "object" at k) mapT { k -> _ } }
          parsed.sequenceT mapT { ObjectE(_) }
        }

      case (alist, (_, pos), _) =>
        PResult.failedQ(InvalidExprType(List(Type.Object), alist.rtype, pos at "object"))
    }

    forms.add("do") {
      case (ArrayL(irs), (auth, pos), apiVersion) =>
        parseList(auth, irs, apiVersion, pos at "do") flatMapT {
          case expr :: exprs => PResult.successfulQ(DoE(expr, exprs))
          case Nil           => PResult.failedQ(EmptyExpr(pos at "do"))
        }
      case (irs, (_, pos), _) => PResult.failedQ(InvalidExprType(List(Type.Array), irs.rtype, pos at "do"))
    }

    forms.add("call") {
      case (call, (auth, pos), apiVersion) =>
        parseExpr(auth, call, apiVersion, pos at "call") mapT { CallE(_, None, pos) }
    }

    forms.add("call", "arguments") {
      case (call, args, (auth, pos), apiVersion) =>
        val callE = parseExpr(auth, call, apiVersion, pos at "call")
        val argsE = parseExpr(auth, args, apiVersion, pos at "arguments")

        (callE, argsE) parT { (ref, args) =>
          PResult.successfulQ(CallE(ref, Some(args), pos))
        }
    }

    // Functions

    forms.addFunction("abort" -> Casts.String, AbortFunction)

    forms.add("query") {
      case (lambdaE, (auth, pos), apiVersion) =>
        parseLambda(auth, lambdaE, apiVersion, pos at "query") mapT { lambdaE =>
          NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.query") }
              val lambdaL = ec.eval(lambdaE) map { _.flatMap { Casts.Lambda(_, pos at "query") } }
              lambdaL flatMapT { QueryFunction(_, ec, pos) }
            },
            () => ObjectL("query" -> lambdaE.literal),
            lambdaE.maxEffect,
            QueryFunction.effect,
            pos)
        }
    }

    forms.addFunction("equals" -> Casts.OneOrMore(Casts.Any), EqualsFunction)
    forms.addFunction("contains" -> Casts.Path, "in" -> Casts.Any, ContainsFunction, APIVersion.deprecatedOn(APIVersion.V3))
    forms.addFunction("contains_path" -> Casts.Path, "in" -> Casts.Any, ContainsPathFunction, APIVersion.introducedOn(APIVersion.V3))
    forms.addFunction("contains_field" -> Casts.String, "in" -> Casts.Any, ContainsFieldFunction, APIVersion.introducedOn(APIVersion.V3))
    forms.addFunction("contains_value" -> Casts.Any, "in" -> Casts.Any, ContainsValueFunction, APIVersion.introducedOn(APIVersion.V3))
    forms.addFunction(
      "select" -> Casts.Path,
      "from" -> Casts.Any,
      "all" -> Option(Casts.Boolean),
      "default" -> Option(Casts.Any), SelectFunction, APIVersion.removedOn(APIVersion.V3))
    forms.addFunction(
      "select_all" -> Casts.Path,
      "from" -> Casts.Any,
      "default" -> Option(Casts.Any), SelectAllFunction, APIVersion.removedOn(APIVersion.V3))
    forms.addFunction(
      "select_as_index" -> Casts.Path,
      "from" -> Casts.Any,
      "default" -> Option(Casts.Any), SelectAllFunction, APIVersion.removedOn(APIVersion.V3))

    forms.add("select", "from", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")

        (pathE, fromE) parT { (path, from) =>
          PResult.successfulQ(SelectE("select", path, from, None, None, pos))
        }
    }

    forms.add("select", "from", "default", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, default, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")
        val defaultE = parseExpr(auth, default, apiVersion, pos at "default")

        (pathE, fromE, defaultE) parT { (path, from, default) =>
          PResult.successfulQ(SelectE("select", path, from, None, Some(default), pos))
        }
    }

    forms.add("select", "from", "all", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, all, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")
        val allE = parseExpr(auth, all, apiVersion, pos at "all")

        (pathE, fromE, allE) parT { (path, from, all) =>
          PResult.successfulQ(SelectE("select", path, from, Some(all), None, pos))
        }
    }

    forms.add("select", "from", "all", "default", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, all, default, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")
        val allE = parseExpr(auth, all, apiVersion, pos at "all")
        val defaultE = parseExpr(auth, default, apiVersion, pos at "default")

        (pathE, fromE, allE, defaultE) parT { (path, from, all, default) =>
          PResult.successfulQ(SelectE("select", path, from, Some(all), Some(default), pos))
        }
    }

    forms.add("select_all", "from", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select_all")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")

        (pathE, fromE) parT { (path, from) =>
          PResult.successfulQ(SelectE("select_all", path, from, Some(TrueL), None, pos))
        }
    }

    forms.add("select_all", "from", "default", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, default, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select_all")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")
        val defaultE = parseExpr(auth, default, apiVersion, pos at "default")

        (pathE, fromE, defaultE) parT { (path, from, default) =>
          PResult.successfulQ(SelectE("select_all", path, from, Some(TrueL), Some(default), pos))
        }
    }

    forms.add("select_as_index", "from", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select_as_index")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")

        (pathE, fromE) parT { (path, from) =>
          PResult.successfulQ(SelectE("select_as_index", path, from, Some(TrueL), None, pos))
        }
    }

    forms.add("select_as_index", "from", "default", APIVersion.introducedOn(APIVersion.V3)) {
      case (path, from, default, (auth, pos), apiVersion) =>
        val pathE = parseExpr(auth, path, apiVersion, pos at "select_as_index")
        val fromE = parseExpr(auth, from, apiVersion, pos at "from")
        val defaultE = parseExpr(auth, default, apiVersion, pos at "default")

        (pathE, fromE, defaultE) parT { (path, from, default) =>
          PResult.successfulQ(SelectE("select_as_index", path, from, Some(TrueL), Some(default), pos))
        }
    }

    // Comparison

    forms.addFunction("lt" -> Casts.OneOrMore(Casts.Any), LessThanFunction)
    forms.addFunction("lte" -> Casts.OneOrMore(Casts.Any), LessThanOrEqualsFunction)
    forms.addFunction("gt" -> Casts.OneOrMore(Casts.Any), GreaterThanFunction)
    forms.addFunction("gte" -> Casts.OneOrMore(Casts.Any), GreaterThanOrEqualsFunction)

    // AbstractArrays used to be called collections and in version 2.6 a project
    // was done to rename:
    //         collection to AbstractArray
    //         class to collection
    //         instance to document
    // Due to the clients use of the term collection, the term collection could
    // not be removed without causing compabilitility issues for our customers.

    forms.add("map", "collection") {
      case (lambda, coll, (auth, pos), apiVersion) =>
        val lambdaE = parseLambda(auth, lambda, apiVersion, pos at "map")
        val collE = parseExpr(auth, coll, apiVersion, pos at "collection")

        (lambdaE, collE) parT { (lambdaE, collE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.map") }
              val lambdaL = ec.eval(lambdaE) map { _.flatMap { Casts.Lambda(_, pos at "map") } }
              val collL = ec.eval(collE) map { _.flatMap { Casts.Iterable(_, pos at "collection") } }

              (lambdaL, collL) parT { (lambda, coll) =>
                MapFunction(lambda, coll, ec, pos)
              }
            },
            () => ObjectL("map" -> lambdaE.literal, "collection" -> collE.literal),
            collE.maxEffect + lambdaE.body.maxEffect,
            MapFunction.effect,
            pos))
        }
    }

    forms.add("foreach", "collection") {
      case (lambda, coll, (auth, pos), apiVersion) =>
        val lambdaE = parseLambda(auth, lambda, apiVersion, pos at "foreach")
        val collE = parseExpr(auth, coll, apiVersion, pos at "collection")

        (lambdaE, collE) parT { (lambdaE, collE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.foreach") }
              val lambdaL = ec.eval(lambdaE) map { _.flatMap { Casts.Lambda(_, pos at "foreach") } }
              val collL = ec.eval(collE) map { _.flatMap { Casts.Iterable(_, pos at "collection") } }

              (lambdaL, collL) parT { (lambda, coll) =>
                ForeachFunction(lambda, coll, ec, pos)
              }
            },
            () => ObjectL("foreach" -> lambdaE.literal, "collection" -> collE.literal),
            collE.maxEffect + lambdaE.body.maxEffect,
            ForeachFunction.effect,
            pos))
        }
    }

    forms.add("filter", "collection") {
      case (lambda, coll, (auth, pos), apiVersion) =>
        val lambdaE = parseLambda(auth, lambda, apiVersion, pos at "filter")
        val collE = parseExpr(auth, coll, apiVersion, pos at "collection")

        (lambdaE, collE) parT { (lambdaE, collE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.filter") }
              val lambdaL = ec.eval(lambdaE) map { _.flatMap { Casts.Lambda(_, pos at "filter") } }
              val collL = ec.eval(collE) map { _.flatMap { Casts.Iterable(_, pos at "collection") } }

              (lambdaL, collL) parT { (lambda, coll) =>
                FilterFunction(lambda, coll, ec, pos)
              }
            },
            () => ObjectL("filter" -> lambdaE.literal, "collection" -> collE.literal),
            collE.maxEffect + lambdaE.body.maxEffect,
            FilterFunction.effect,
            pos))
        }
    }

    forms.add("reduce", "initial", "collection") {
      case (lambda, initial, coll, (auth, pos), apiVersion) =>
        val lambdaE = parseLambda(auth, lambda, apiVersion, pos at "reduce")
        val initialE = parseExpr(auth, initial, apiVersion, pos at "initial")
        val collE = parseExpr(auth, coll, apiVersion, pos at "collection")

        (lambdaE, initialE, collE) parT { (lambdaE, initialE, collE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.reduce") }
              val lambdaL = ec.eval(lambdaE) map { _.flatMap { Casts.Lambda(_, pos at "reduce") } }
              val initialL = ec.eval(initialE) map { _.flatMap { Casts.Any(_, pos at "initial") } }
              val collL = ec.eval(collE) map { _.flatMap { Casts.Iterable(_, pos at "collection") } }

              (lambdaL, initialL, collL) parT { (lambda, initial, coll) =>
                ReduceFunction(lambda, initial, coll, ec, pos)
              }
            },
            () => ObjectL("reduce" -> lambdaE.literal, "initial" -> initialE.literal, "collection" -> collE.literal),
            ReduceFunction.effect + collE.maxEffect + initialE.maxEffect + lambdaE.body.maxEffect,
            ReduceFunction.effect,
            pos))
        }
    }

    forms.addFunction(
      "prepend" -> Casts.ZeroOrMore(Casts.Any),
      "collection" -> Casts.Array, PrependFunction)
    forms.addFunction(
      "append" -> Casts.ZeroOrMore(Casts.Any),
      "collection" -> Casts.Array, AppendFunction)

    forms.addFunction(
      "take" -> Casts.Integer,
      "collection" -> Casts.Any, TakeFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction(
      "take" -> Casts.Integer,
      "collection" -> Casts.Iterable, TakeFunction2, APIVersion.introducedOn(APIVersion.Unstable))

    forms.addFunction(
      "drop" -> Casts.Integer,
      "collection" -> Casts.Any, DropFunction)
    forms.addFunction(
      "is_empty" -> Casts.Iterable, IsEmptyFunction)
    forms.addFunction(
      "is_nonempty" -> Casts.Iterable, IsNonEmptyFunction)

    // Objects

    forms.add("merge", "with", "lambda") {
      case (merge, with_, lambda, (auth, pos), apiVersion) =>
        val mergeE = parseExpr(auth, merge, apiVersion, pos at "merge")
        val withE = parseExpr(auth, with_, apiVersion, pos at "with")
        val lambdaE = parseLambda(auth, lambda, apiVersion, pos at "lambda")

        (mergeE, withE, lambdaE) parT { (mergeE, withE, lambdaE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.merge") }
              val mergeL = ec.eval(mergeE) map { _.flatMap { Casts.Any(_, pos at "merge") } }
              val withL = ec.eval(withE) map { _.flatMap { Casts.Any(_, pos at "with") } }
              val lambdaL = ec.eval(lambdaE) map { _.flatMap { Casts.Lambda(_, pos at "lambda") } }

              (mergeL, withL, lambdaL) parT { (merge, with_, lambda) =>
                MergeFunction(merge, with_, Some(lambda), ec, pos)
              }
            },
            () => ObjectL("merge" -> mergeE.literal, "with" -> withE.literal, "lambda" -> lambdaE.literal),
            mergeE.maxEffect + withE.maxEffect + lambdaE.body.maxEffect,
            MergeFunction.effect,
            pos))
        }
    }
    forms.add("merge", "with") {
      case (merge, with_, (auth, pos), apiVersion) =>
        val mergeE = parseExpr(auth, merge, apiVersion, pos at "merge")
        val withE = parseExpr(auth, with_, apiVersion, pos at "with")

        (mergeE, withE) parT { (mergeE, withE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.merge") }
              val mergeL = ec.eval(mergeE) map { _.flatMap { Casts.Any(_, pos at "merge") } }
              val withL = ec.eval(withE) map { _.flatMap { Casts.Any(_, pos at "with") } }

              (mergeL, withL) parT { (merge, with_) =>
                MergeFunction(merge, with_, None, ec, pos)
              }
            },
            () => ObjectL("merge" -> mergeE.literal, "with" -> withE.literal),
            mergeE.maxEffect + withE.maxEffect,
            MergeFunction.effect,
            pos))
        }
    }

    // Strings

    forms.addFunction("concat" -> Casts.ZeroOrMore(Casts.String),
                      "separator" -> Option(Casts.String),
                      ConcatFunction)
    forms.addFunction("casefold" -> Casts.ZeroOrMore(Casts.Scalar),
                      "normalizer" -> Option(Casts.Normalizer),
                      CaseFoldFunction)
    forms.addFunction("regexescape" -> Casts.String,
                      RegexEscapeFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("regex_escape" -> Casts.String,
                      RegexEscapeFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("startswith" -> Casts.String,
                      "search" -> Casts.String,
                      StartsWithFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("starts_with" -> Casts.String,
                      "search" -> Casts.String,
                      StartsWithFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("endswith" -> Casts.String,
                      "search" -> Casts.String,
                      EndsWithFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("ends_with" -> Casts.String,
                      "search" -> Casts.String,
                      EndsWithFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("containsstr" -> Casts.String,
                      "search" -> Casts.String,
                      ContainsStrFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("contains_str" -> Casts.String,
                      "search" -> Casts.String,
                      ContainsStrFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("containsstrregex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      ContainsStrRegexFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("contains_str_regex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      ContainsStrRegexFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("findstr" -> Casts.String,
                      "find" -> Casts.String,
                      "start" -> Option(Casts.Integer),
                      FindStrFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("find_str" -> Casts.String,
                      "find" -> Casts.String,
                      "start" -> Option(Casts.Integer),
                      FindStrFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("findstrregex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      "start" -> Option(Casts.Integer),
                      "num_results" -> Option(Casts.Integer),
                      FindStrRegexFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("find_str_regex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      "start" -> Option(Casts.Integer),
                      "num_results" -> Option(Casts.Integer),
                      FindStrRegexFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("length" -> Casts.String, LengthStringFunction)
    forms.addFunction("lowercase" -> Casts.String, LowerCaseFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("lower_case" -> Casts.String, LowerCaseFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("ltrim" -> Casts.String, LTrimFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("l_trim" -> Casts.String, LTrimFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("ngram" -> Casts.ZeroOrMore(Casts.Scalar),
                      "min" -> Option(Casts.Integer),
                      "max" -> Option(Casts.Integer),
                      NGramFunction)
    forms.addFunction("repeat" -> Casts.String,
                      "number" -> Option(Casts.Integer),
                      RepeatStringFunction)
    forms.addFunction("replacestr" -> Casts.String,
                      "find" -> Casts.String,
                      "replace" -> Casts.String,
                       ReplaceStringFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("replace_str" -> Casts.String,
                      "find" -> Casts.String,
                      "replace" -> Casts.String,
                      ReplaceStringFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("replacestrregex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      "replace" -> Casts.String,
                      "first" -> Option(Casts.Boolean),
                      ReplaceStrRegexFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("replace_str_regex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      "replace" -> Casts.String,
                      "first" -> Option(Casts.Boolean),
                      ReplaceStrRegexFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("rtrim" -> Casts.String, RTrimFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("r_trim" -> Casts.String, RTrimFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("space" -> Casts.Integer, SpaceFunction)
    forms.addFunction("substring" -> Casts.String,
                      "start" -> Option(Casts.Integer),
                      "length" -> Option(Casts.Integer),
                      SubStringFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("sub_string" -> Casts.String,
                      "start" -> Option(Casts.Integer),
                      "length" -> Option(Casts.Integer),
                      SubStringFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("titlecase" -> Casts.String, TitleCaseFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("title_case" -> Casts.String, TitleCaseFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("trim" -> Casts.String, TrimFunction)
    forms.addFunction("uppercase" -> Casts.String, UpperCaseFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("upper_case" -> Casts.String, UpperCaseFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("format" -> Casts.String,
                      "values" -> Casts.ZeroOrMore(Casts.Any),
                      FormatFunction)
    forms.addFunction("split_str" -> Casts.String,
                      "token" -> Casts.String,
                      "count" -> Option(Casts.Integer),
                      SplitStrFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("split_str_regex" -> Casts.String,
                      "pattern" -> Casts.Regex,
                      "count" -> Option(Casts.Integer),
                      SplitStrRegexFunction, APIVersion.introducedOn(APIVersion.Unstable))



    // Numerics

    forms.addFunction("abs" -> Casts.Number, AbsFunction)
    forms.addFunction("add" -> Casts.OneOrMore(Casts.Number), AddFunction)
    forms.addFunction("bitand" -> Casts.OneOrMore(Casts.Number), BitAndFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("bit_and" -> Casts.OneOrMore(Casts.Number), BitAndFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("bitnot" -> Casts.Number, BitNotFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("bit_not" -> Casts.Number, BitNotFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("bitor" -> Casts.OneOrMore(Casts.Number), BitOrFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("bit_or" -> Casts.OneOrMore(Casts.Number), BitOrFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("bitxor" -> Casts.OneOrMore(Casts.Number), BitXorFunction, APIVersion.removedOn(APIVersion.Unstable))
    forms.addFunction("bit_xor" -> Casts.OneOrMore(Casts.Number), BitXorFunction, APIVersion.introducedOn(APIVersion.Unstable))
    forms.addFunction("ceil" -> Casts.Number, CeilFunction)
    forms.addFunction("divide" -> Casts.OneOrMore(Casts.Number), DivideFunction)
    forms.addFunction("floor" -> Casts.Number, FloorFunction)
    forms.addFunction("max" -> Casts.Iterable, MaxFunction)
    forms.addFunction("min" -> Casts.Iterable, MinFunction)
    forms.addFunction("modulo" -> Casts.OneOrMore(Casts.Number), ModuloFunction)
    forms.addFunction("multiply" -> Casts.OneOrMore(Casts.Number), MultiplyFunction)
    forms.addFunction("round" -> Casts.Number,
                      "precision" -> Option(Casts.Integer), RoundFunction)
    forms.addFunction("subtract" -> Casts.OneOrMore(Casts.Number), SubtractFunction)
    forms.addFunction("sign" -> Casts.Number, SignFunction)
    forms.addFunction("sqrt" -> Casts.Number, SqrtFunction)
    forms.addFunction("trunc" -> Casts.Number,
                      "precision" -> Option(Casts.Integer), TruncFunction)
    forms.addFunction("mean" -> Casts.Iterable, MeanFunction)
    forms.addFunction("count" -> Casts.Iterable, CountFunction)
    forms.addFunction("sum" -> Casts.Iterable, SumFunction)
    forms.addFunction("any" -> Casts.Iterable, AnyFunction)
    forms.addFunction("all" -> Casts.Iterable, AllFunction)

    // Trig

    forms.addFunction("acos" -> Casts.Number, ACosFunction)
    forms.addFunction("asin" -> Casts.Number, ASinFunction)
    forms.addFunction("atan" -> Casts.Number, ATanFunction)
    forms.addFunction("cos" -> Casts.Number, CosFunction)
    forms.addFunction("cosh" -> Casts.Number, CoshFunction)
    forms.addFunction("degrees" -> Casts.Number, DegreesFunction)
    forms.addFunction("exp" -> Casts.Number, ExpFunction)
    forms.addFunction("hypot" -> Casts.Number,
                       "b" -> Option(Casts.Number), HypotFunction)
    forms.addFunction("ln" -> Casts.Number, LnFunction)
    forms.addFunction("log" -> Casts.Number, LogFunction)
    forms.addFunction("pow" -> Casts.Number,
                      "exp" -> Option(Casts.Number), PowFunction)
    forms.addFunction("radians" -> Casts.Number, RadiansFunction)
    forms.addFunction("sin" -> Casts.Number, SinFunction)
    forms.addFunction("sinh" -> Casts.Number, SinhFunction)
    forms.addFunction("tan" -> Casts.Number, TanFunction)
    forms.addFunction("tanh" -> Casts.Number, TanhFunction)

    // Booleans

    forms.addFunction("and" -> Casts.OneOrMore(Casts.Boolean), AndFunction, APIVersion.removedOn(APIVersion.V3))
    forms.addFunction("or" -> Casts.OneOrMore(Casts.Boolean), OrFunction, APIVersion.removedOn(APIVersion.V3))
    forms.addFunction("not" -> Casts.Boolean, NotFunction)

    // Refs

    forms.addFunction(
      "ref" -> Casts.Any,
      "id" -> Casts.Any,
      "scope" -> Option(Casts.DatabaseRef),
      RefFunction)
    forms.addFunction("database" -> Casts.String, "scope" -> Option(Casts.DatabaseRef), DatabaseRefFunction)
    forms.addFunction("index" -> Casts.String, "scope" -> Option(Casts.DatabaseRef), IndexRefFunction)
    forms.addFunction("class" -> Casts.String, "scope" -> Option(Casts.DatabaseRef),  ClassRefFunction)
    forms.addFunction("collection" -> Casts.String, "scope" -> Option(Casts.DatabaseRef),  ClassRefFunction)
    forms.addFunction("function" -> Casts.String, "scope" -> Option(Casts.DatabaseRef), UserFunctionRefFunction)
    forms.addFunction("role" -> Casts.String, "scope" -> Option(Casts.DatabaseRef), RoleRefFunction)
    forms.addFunction("access_provider" -> Casts.String,
      "scope" -> Option(Casts.DatabaseRef),
      AccessProviderRefFunction,
      APIVersion.introducedOn(APIVersion.V4))

    forms.addFunction("next_id" -> Casts.Null, NewIDFunction) // deprecated; use new_id
    forms.addFunction("new_id" -> Casts.Null, NewIDFunction)

    // Native classref constructors

    forms.addFunction("databases" -> Casts.Nullable(Casts.DatabaseRef), DatabasesNativeCollectionConstructFun)
    forms.addFunction("indexes" -> Casts.Nullable(Casts.DatabaseRef), IndexesNativeCollectionConstructFun)
    forms.addFunction("classes" -> Casts.Nullable(Casts.DatabaseRef),  CollectionsNativeCollectionConstructFun)
    forms.addFunction("collections" -> Casts.Nullable(Casts.DatabaseRef),  CollectionsNativeCollectionConstructFun)
    forms.addFunction("keys" -> Casts.Nullable(Casts.DatabaseRef), KeysNativeCollectionConstructFun)
    forms.addFunction("tokens" -> Casts.Nullable(Casts.DatabaseRef), TokensNativeCollectionConstructFun)
    forms.addFunction("credentials" -> Casts.Nullable(Casts.DatabaseRef), CredentialsNativeCollectionConstructFun)
    forms.addFunction("functions" -> Casts.Nullable(Casts.DatabaseRef), UserFunctionsNativeCollectionConstructFun)
    forms.addFunction("roles" -> Casts.Nullable(Casts.DatabaseRef), RolesNativeCollectionConstructFun)
    forms.addFunction("access_providers" -> Casts.Nullable(Casts.DatabaseRef),
      AccessProvidersNativeCollectionConstructFun,
      APIVersion.introducedOn(APIVersion.V4))

    // Set ref constructors
    forms.addFunction("singleton" -> Casts.Ref, SingletonFunction)
    forms.addFunction("events" -> Casts.Identifier, EventsFunction)
    forms.addFunction("documents" -> Casts.ClassRef, DocumentsFunction)

    forms.addFunction("match" -> Casts.ZeroOrMore(Casts.MatchTerm), "index" -> Casts.StringOrIndexRef, MatchFunction)
    forms.addFunction("match" -> Casts.StringOrIndexRef, "terms" -> Casts.ZeroOrMore(Casts.MatchTerm), MatchFunction)
    forms.addFunction("match" -> Casts.StringOrIndexRef, MatchFunction)

    forms.addFunction("union" -> Casts.OneOrMore(Casts.SetOrArray), UnionFunction)
    forms.addFunction("intersection" -> Casts.OneOrMore(Casts.SetOrArray), IntersectionFunction)
    forms.addFunction("difference" -> Casts.OneOrMore(Casts.SetOrArray), DifferenceFunction)
    forms.addFunction("distinct" -> Casts.Iterable, DistinctFunction)
    forms.addFunction("range" -> Casts.Set, "from" -> Casts.Any, "to" -> Casts.Any, FunctionAdapters.Range)

    forms.add("join", "with") {
      case (join, with_, (auth, pos), apiVersion) =>
        val joinE = parseExpr(auth, join, apiVersion, pos at "join")
        val withE = parseLambda(auth, with_, apiVersion, pos at "with") flatMap {
          case s @ PResult.Success(_, _) => Query.value(s)
          case _                         => parseExpr(auth, with_, apiVersion, pos at "with")
        }

        (joinE, withE) parT { (joinE, withE) =>
          PResult.successfulQ(NativeCall(
            { ec =>
              GlobalTracer.instance.activeSpan foreach { _.setOperation("eval.native_call.join") }
              val joinL = ec.eval(joinE) map { _.flatMap { Casts.Set(_, pos at "join") } }
              val withL = ec.eval(withE) map { _.flatMap { Casts.Any(_, pos at "with") } }

              (joinL, withL) parT { (join, with_) =>
                JoinFunction(join, with_, ec, pos)
              }
            },
            () => ObjectL("join" -> joinE.literal, "with" -> withE.literal),
            withE match {
              case l: LambdaE => joinE.maxEffect + l.body.maxEffect
              case l          => joinE.maxEffect + l.maxEffect
            },
            JoinFunction.effect,
            pos))
        }
    }

    forms.addFunction("reverse" -> Casts.Iterable, ReverseFunction)

    // Reads

    forms.addFunction("exists" -> Casts.Unresolved, "ts" -> Option(Casts.TransactionTimestamp), ExistsFunction)
    forms.addFunction("get" -> Casts.Identifier, "ts" -> Option(Casts.TransactionTimestamp), GetFunction)
    forms.addFunction("first" -> Casts.SetOrArray, FirstFunction, APIVersion.introducedOn(APIVersion.V4))
    forms.addFunction("last" -> Casts.SetOrArray, LastFunction, APIVersion.introducedOn(APIVersion.V4))

    forms.addFunction("key_from_secret" -> Casts.String, KeyFromSecretFunction)

    forms.addFunction(
      "paginate" -> Casts.Identifier,
      "after" -> Option(Casts.Any),
      "ts" -> Option(Casts.TransactionTimestamp),
      "size" -> Option(Casts.Integer),
      "events" -> Option(Casts.Boolean),
      "sources" -> Option(Casts.Boolean), PaginateFunction.PaginateAfter)

    forms.addFunction(
      "paginate" -> Casts.Identifier,
      "before" -> Casts.Any,
      "ts" -> Option(Casts.TransactionTimestamp),
      "size"-> Option(Casts.Integer),
      "events" -> Option(Casts.Boolean),
      "sources" -> Option(Casts.Boolean), PaginateFunction.PaginateBefore)

    forms.addFunction(
      "paginate" -> Casts.Identifier,
      "cursor" -> Casts.Any,
      "ts" -> Option(Casts.TransactionTimestamp),
      "size"-> Option(Casts.Integer),
      "events" -> Option(Casts.Boolean),
      "sources" -> Option(Casts.Boolean), PaginateFunction.PaginateCursor)

    // Writes

    forms.addFunction(
      "create" -> Casts.StringOrRef,
      "params" -> Option(Casts.Object), CreateFunction)

    forms.addFunction("update" -> Casts.Ref,
      "params" -> Option(Casts.Object), UpdateFunction)

    forms.addFunction("replace" -> Casts.Ref,
      "params" -> Option(Casts.Object), ReplaceFunction)

    forms.addFunction("delete" -> Casts.Ref, DeleteFunction)

    forms.addFunction("create_class" -> Casts.Object, CreateClassFunction)
    forms.addFunction("create_collection" -> Casts.Object, CreateCollectionFunction)
    forms.addFunction("create_database" -> Casts.Object, CreateDatabaseFunction)
    forms.addFunction("create_index" -> Casts.Object, CreateIndexFunction)
    forms.addFunction("create_key" -> Casts.Object, CreateKeyFunction)
    forms.addFunction("create_function" -> Casts.Object, CreateUserFunctionFunction)
    forms.addFunction("create_role" -> Casts.Object, CreateRoleFunction)
    forms.addFunction("create_access_provider" -> Casts.Object,
      CreateAccessProviderFunction, APIVersion.introducedOn(APIVersion.V4))

    forms.addFunction(
      "insert" -> Casts.Ref,
      "ts" -> Casts.Timestamp,
      "action" -> Casts.DocAction,
      "params" -> Option(Casts.Object), InsertVersionFunction)

    forms.addFunction(
      "remove" -> Casts.Ref,
      "ts" -> Casts.Timestamp,
      "action" -> Casts.DocAction, RemoveVersionFunction)

    forms.addFunction(
      "move_database" -> Casts.DatabaseRef,
      "to" -> Casts.DatabaseRef,
      MoveDatabaseFunction
    )

    // Auth

    forms.addFunction("identity" -> Casts.Null, CurrentIdentityFunction, APIVersion.deprecatedOn(APIVersion.V4))
    forms.addFunction("has_identity" -> Casts.Null, HasCurrentIdentityFunction, APIVersion.deprecatedOn(APIVersion.V4))

    forms.addFunction(
      "current_identity" -> Casts.Null,
      CurrentIdentityFunction,
      APIVersion.introducedOn(APIVersion.V4)
    )

    forms.addFunction(
      "has_current_identity" -> Casts.Null,
      HasCurrentIdentityFunction,
      APIVersion.introducedOn(APIVersion.V4)
    )

    forms.addFunction(
      "identify" -> Casts.Identifier,
      "password" -> Casts.String, IdentifyFunction)

    forms.addFunction(
      "login" -> Casts.Identifier,
      "params" -> Casts.Diff, LoginFunction)

    forms.addFunction("logout" -> Casts.Boolean, LogoutFunction)

    forms.addFunction(
      "issue_access_jwt" -> Casts.Ref,
      "expire_in" -> Option(Casts.Integer),
      IssueAccessJWTFunction, APIVersion.introducedOn(APIVersion.Unstable))

    forms.addFunction(
      "current_token" -> Casts.Null,
      CurrentTokenFunction,
      APIVersion.introducedOn(APIVersion.V4)
    )

    forms.addFunction(
      "has_current_token" -> Casts.Null,
      HasCurrentTokenFunction,
      APIVersion.introducedOn(APIVersion.V4)
    )

    // Time and Date

    forms.addFunction("time" -> Casts.String, TimeFunction)
    forms.addFunction("now" -> Casts.Null, NowFunction)
    forms.addFunction(
      "epoch" -> Casts.Integer,
      "unit" -> Casts.MidstUnit, EpochFunction)

    forms.addFunction("date" -> Casts.String, DateFunction)

    forms.addFunction("to_micros" -> Casts.Timestamp, ToMicrosFunction)
    forms.addFunction("to_millis" -> Casts.Timestamp, ToMillisFunction)
    forms.addFunction("to_seconds" -> Casts.Timestamp, ToSecondsFunction)

    forms.addFunction("second" -> Casts.Timestamp, SecondFunction)
    forms.addFunction("minute" -> Casts.Timestamp, MinuteFunction)
    forms.addFunction("hour" -> Casts.Timestamp, HourFunction)
    forms.addFunction("day_of_month" -> Casts.Timestamp, DayOfMonthFunction)
    forms.addFunction("day_of_week" -> Casts.Timestamp, DayOfWeekFunction)
    forms.addFunction("day_of_year" -> Casts.Timestamp, DayOfYearFunction)
    forms.addFunction("month" -> Casts.Timestamp, MonthFunction)
    forms.addFunction("year" -> Casts.Timestamp, YearFunction)

    forms.addFunction(
      "time_add" -> Casts.Temporal,
      "offset" -> Casts.Integer,
      "unit" -> Casts.MidstUnit,
      TimeAddFunction)

    forms.addFunction(
      "time_subtract" -> Casts.Temporal,
      "offset" -> Casts.Integer,
      "unit" -> Casts.MidstUnit,
      TimeSubtractFunction)

    forms.addFunction(
      "time_diff" -> Casts.Temporal,
      "other" -> Casts.Temporal,
      "unit" -> Casts.MidstUnit,
      TimeDiffFunction)

    // Conversions

    forms.addFunction("to_string" -> Casts.Any, ToStringFunction)
    forms.addFunction("to_number" -> Casts.Any, ToNumberFunction)
    forms.addFunction("to_double" -> Casts.Any, ToDoubleFunction)
    forms.addFunction("to_integer" -> Casts.Any, ToIntegerFunction)
    forms.addFunction("to_time" -> Casts.Any, ToTimeFunction)
    forms.addFunction("to_date" -> Casts.Any, ToDateFunction)
    forms.addFunction("to_object" -> Casts.Any, ToObjectFunction)
    forms.addFunction("to_array" -> Casts.Any, ToArrayFunction)

    // Type check
    forms.addFunction("is_number" -> Casts.Any, IsNumberFunction)
    forms.addFunction("is_double" -> Casts.Any, IsDoubleFunction)
    forms.addFunction("is_integer" -> Casts.Any, IsIntegerFunction)
    forms.addFunction("is_boolean" -> Casts.Any, IsBooleanFunction)
    forms.addFunction("is_null" -> Casts.Any, IsNullFunction)
    forms.addFunction("is_bytes" -> Casts.Any, IsBytesFunction)
    forms.addFunction("is_timestamp" -> Casts.Any, IsTimestampFunction)
    forms.addFunction("is_date" -> Casts.Any, IsDateFunction)
    forms.addFunction("is_uuid" -> Casts.Any, IsUUIDFunction)
    forms.addFunction("is_string" -> Casts.Any, IsStringFunction)
    forms.addFunction("is_array" -> Casts.Any, IsArrayFunction)
    forms.addFunction("is_object" -> Casts.Any, IsObjectFunction)
    forms.addFunction("is_ref" -> Casts.Any, IsRefFunction)
    forms.addFunction("is_set" -> Casts.Any, IsSetFunction)
    forms.addFunction("is_doc" -> Casts.Any, IsDocFunction)
    forms.addFunction("is_lambda" -> Casts.Any, IsLambdaFunction)
    forms.addFunction("is_collection" -> Casts.Any, IsCollectionFunction)
    forms.addFunction("is_database" -> Casts.Any, IsDatabaseFunction)
    forms.addFunction("is_index" -> Casts.Any, IsIndexFunction)
    forms.addFunction("is_function" -> Casts.Any, IsFunctionFunction)
    forms.addFunction("is_key" -> Casts.Any, IsKeyFunction)
    forms.addFunction("is_token" -> Casts.Any, IsTokenFunction)
    forms.addFunction("is_credentials" -> Casts.Any, IsCredentialsFunction)
    forms.addFunction("is_role" -> Casts.Any, IsRoleFunction)
  }
}

object FunctionAdapters {

  sealed abstract class FunctionAdapter(source: QFunction) extends QFunction {
    def effect: Effect = source.effect
  }

  object Range extends FunctionAdapter(RangeFunction) {

    def apply(
      source: EventSet,
      fromL: Literal,
      toL: Literal,
      ec: EvalContext,
      pos: Position): Query[R[SetL]] = Range(source, fromL, toL, ec.scopeID, ec.apiVers, pos)

    def apply(
      source: EventSet,
      fromL: Literal,
      toL: Literal,
      scope: ScopeID,
      apiVersion: APIVersion,
      pos: Position): Query[R[SetL]] = {

      val fromR = CursorParser.lowerBoundCursor(source, fromL, scope, apiVersion, pos at "from")
      val toR = CursorParser.upperBoundCursor(source, toL, scope, apiVersion, pos at "to")

      (fromR, toR) match {
        case (Right(f), Right(t)) => RangeFunction(source, f, t, scope, pos)
        case (e1, e2)             => Query.value(Parser.collectErrors(e1, e2))
      }
    }
  }
}

/**
  * Parses a literal into a cursor that can be used as a pagination cursor or as a
  * bound in Range(). `floor` controls whether or not the returned event is
  * appropriate for use as a lower (default) or upper bound.
  */
object CursorParser {

  private val EventCursorExpectedTypes =
    List(Type.Object, Type.Integer, Type.Time, Type.Date)

  def lowerBoundCursor(
    set: EventSet,
    cursor: Literal,
    scope: ScopeID,
    apiVersion: APIVersion,
    pos: Position): R[Cursor] = parse(set, cursor, scope, apiVersion, pos, floor = true)

  def upperBoundCursor(
    set: EventSet,
    cursor: Literal,
    scope: ScopeID,
    apiVersion: APIVersion,
    pos: Position): R[Cursor] = parse(set, cursor, scope, apiVersion, pos, floor = false)

  def parseCursorObj(
    set: EventSet,
    cursor: Literal,
    scope: ScopeID,
    apiVersion: APIVersion,
    pos: Position,
    floor: Boolean = true): R[(Option[Cursor], Boolean)] = {

    cursor match {
      case ObjectL(elems) =>
        ObjectCursors(elems, (set, scope, pos, floor), apiVersion) match {
          case Some(Right((cur, ascending))) => Right((Some(cur), ascending))
          case Some(Left(errors))            => Left(errors)
          case None                          => Left(List(InvalidCursorObject(pos)))
        }
      case NullL => Right((None, true))
      case _     => Left(List(InvalidCursorObject(pos)))
    }
  }

  private def parse(
    set: EventSet,
    cursor: Literal,
    scope: ScopeID,
    apiVersion: APIVersion,
    pos: Position,
    floor: Boolean): R[Cursor] = {

    cursor match {
      case UnresolvedRefL(ref) =>
        Left(List(UnresolvedRefError(ref, pos)))

      case _ =>
        if (set.shape.isHistorical) {
          parseEvent(cursor, scope, apiVersion, pos)
        } else {
          parseArray(set, cursor, scope, pos, floor)
        }
    }
  }

  private def parseEvent(
    cursor: Literal,
    scope: ScopeID,
    apiVersion: APIVersion,
    pos: Position): R[Cursor] = {

    def setEvent(ts: Timestamp) =
      SetEvent(ts, scope, DocID.MinValue, SetAction.MinValue)

    val eventR = cursor match {
      case ObjectL(elems) =>
        EventCursors(elems, (scope, pos), apiVersion) getOrElse {
          Left(List(InvalidEventCursorForm(elems map { _._1 }, pos)))
        }
      case LongL(ts)   => Right(setEvent(Timestamp.ofMicros(ts)))
      case TimeL(ts)   => Right(setEvent(ts))
      case DateL(date) => Right(setEvent(Timestamp(date)))
      case NullL       => Right(setEvent(Timestamp.MaxMicros))
      case elems =>
        Left(List(InvalidArgument(EventCursorExpectedTypes, elems.rtype, pos)))
    }

    eventR map { ev =>
      Cursor(CursorL(EventL(ev)), ev)
    }
  }

  private def parseArray(
    set: EventSet,
    cursor: Literal,
    scope: ScopeID,
    pos: Position,
    floor: Boolean): R[Cursor] = {

    ArrayCursor(cursor, pos) match {
      case Right(terms) =>
        val ts = if (floor) Timestamp.Epoch else Timestamp.MaxMicros
        val id = if (floor) DocID.MinValue else DocID.MaxValue

        // for this cursor, the set's shape excludes the ref
        val event = terms.toVector splitAt (set.shape.values - 1) match {
          case (terms, cursor) if cursor.isEmpty => // prefix cursor
            val values = terms zip set.shape.reversed map {
              case (t, r) => Literal.toIndexTerm(t, r)
            }
            SetEvent(ts, scope, id, Add, values)

          case (t, c) => // complete cursor
            val (values, cursor) = if (set.shape eq EventSet.Shape.Zero) {
              val (terms, cursor) = c splitAt (c.size - 1)
              (terms map { Literal.toIndexTerm(_) }, cursor)
            } else {
              val values = t zip set.shape.reversed map {
                case (t, r) => Literal.toIndexTerm(t, r)
              }
              (values, c)
            }

            Casts.Ref(cursor.head, pos) match {
              case Right(RefL(scope, id)) =>
                SetEvent(ts, scope, id, Add, values)

              case Left(_) =>
                val id = Literal.toIndexTerm(cursor.head)
                val a = CBOR.encode(id.value)
                val b = CBOR.encode(DocIDV(DocID.MinValue))

                if (CBOROrdering.compare(a, b) > 0) {
                  SetEvent(ts, scope, DocID.MaxValue, Add, values)
                } else {
                  SetEvent(ts, scope, DocID.MinValue, Remove, values)
                }
            }
        }
        Right(Cursor(CursorL(terms), event))

      case Left(errs) => Left(errs)
    }
  }

  private val ArrayCursor = Casts.ZeroOrMore(Casts.Any)

  private val ObjectCursors = {

    type Context = (EventSet, ScopeID, Position, Boolean)

    def mk(
      cursor: Literal,
      context: Context,
      cursorPath: String,
      apiVersion: APIVersion,
      ascending: Boolean): R[(Cursor, Boolean)] = {

      val (set, scope, pos, floor) = context
      parse(set, cursor, scope, apiVersion, pos at cursorPath, floor) map { (_, ascending) }
    }

    MapRouter.build[Literal, R[(Cursor, Boolean)], Context] { forms =>
      forms.add("after") {
        case (cursor, context, apiVersion) =>
          mk(cursor, context, cursorPath = "after", apiVersion, ascending = true)
      }
      forms.add("before") {
        case (cursor, context, apiVersion) =>
          mk(cursor, context, cursorPath = "before", apiVersion, ascending = false)
      }
    }
  }

  private val EventCursors = {
    def mk(
      ts: Literal,
      action: Option[Literal],
      instance: Option[Literal],
      instField: String,
      data: Option[Literal],
      dataField: String,
      scope: ScopeID,
      pos: Position): R[Event] = {

      val tsE = Casts.Integer(ts, pos at "ts") map { Timestamp.ofMicros(_) }

      val actionE: R[Action] =
        Casts.SetAction.opt(action, pos at "action") match {
          case Right(Some(act)) => Right(act)
          case Left(_) | Right(None) =>
            Casts.DocAction.orElse(action, SetAction.MinValue, pos at "action")
        }

      val documentE =
        Casts.Ref.orElse(instance, RefL(scope, DocID.MaxValue), pos at instField)

      val dataE = ArrayCursor.opt(data, pos at dataField)

      (tsE, documentE, actionE, dataE) match {
        case (Right(ts), Right(RefL(scope, id)), Right(act: DocAction), Right(_)) =>
          Right(DocEvent(ts, scope, id, act, MapV.empty))

        case (Right(ts),
              Right(RefL(scope, id)),
              Right(act: SetAction),
              Right(None)) =>
          Right(SetEvent(ts, scope, id, act))

        case (Right(ts),
              Right(RefL(scope, id)),
              Right(act: SetAction),
              Right(Some(vs))) =>
          val b = Vector.newBuilder[IndexTerm]
          b.sizeHint(vs.size)
          vs foreach { b += Literal.toIndexTerm(_) }
          Right(SetEvent(ts, scope, id, act, b.result()))

        case (e1, e2, e3, e4) =>
          Parser.collectErrors(e1, e2, e3, e4)
      }
    }

    MapRouter.build[Literal, R[Event], (ScopeID, Position)] { cursors =>
      cursors.add("ts") {
        case (ts, (scope, pos), _) =>
          mk(ts, None, None, "", None, "", scope, pos)
      }

      cursors.add("ts", "action") {
        case (ts, ac, (scope, pos), _) =>
          mk(ts, Some(ac), None, "", None, "", scope, pos)
      }

      cursors.add("ts", "action", "resource") {
        case (ts, ac, id, (scope, pos), _) =>
          mk(ts, Some(ac), Some(id), "resource", None, "", scope, pos)
      }

      cursors.add("ts", "action", "resource", "values") {
        case (ts, ac, id, vs, (scope, pos), _) =>
          mk(ts, Some(ac), Some(id), "resource", Some(vs), "values", scope, pos)
      }

      cursors.add("ts", "action", "instance") {
        case (ts, ac, id, (scope, pos), _) =>
          mk(ts, Some(ac), Some(id), "instance", None, "", scope, pos)
      }

      cursors.add("ts", "action", "document") {
        case (ts, ac, id, (scope, pos), _) =>
          mk(ts, Some(ac), Some(id), "document", None, "", scope, pos)
      }

      cursors.add("ts", "action", "instance", "data") {
        case (ts, ac, id, data, (scope, pos), _) =>
          mk(ts, Some(ac), Some(id), "instance", Some(data), "data", scope, pos)
      }

      cursors.add("ts", "action", "document", "data") {
        case (ts, ac, id, data, (scope, pos), _) =>
          mk(ts, Some(ac), Some(id), "document", Some(data), "data", scope, pos)
      }
    }
  }
}
