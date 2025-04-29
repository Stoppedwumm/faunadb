package fauna.ast

import fauna.atoms._
import fauna.auth.EvalAuth
import fauna.codex.json2._
import fauna.lang.Timestamp
import fauna.lang.syntax.option._
import fauna.model._
import fauna.model.runtime.Effect
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc.Diff
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.storage.{ Action, DocEvent, Event, SetEvent }
import io.netty.buffer.ByteBuf
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.unused
import scala.collection.mutable.{ Set => MSet }
import scala.util.hashing.MurmurHash3

sealed trait AbstractExpr {
  // in order to return exprs to clients
  def literal: Literal

  // The maximum possible effect the expression may have. This is the effect it
  // will have, except for UDF calls, which have a maxEffect of Write,
  // regardless of their actual effect.
  def maxEffect: Effect
}

sealed abstract class LambdaPat {
  def literal: Literal = this match {
    case LambdaNullPat      => StringL("_")
    case LambdaScalarPat(l) => StringL(l)
    case LambdaArrayPat(es) => ArrayL(es map { _.literal })
  }

  def params: Set[String] = this match {
    case LambdaNullPat => Set.empty
    case LambdaScalarPat(l) => Set(l)
    case LambdaArrayPat(es) => (es foldLeft Set.empty[String]) { _ | _.params }
  }

  def arity: Int = this match {
    case LambdaNullPat        => 0
    case LambdaScalarPat(_)   => 1
    case LambdaArrayPat(subs) => (subs foldLeft 0) { _ + _.arity }
  }
}

case object LambdaNullPat extends LambdaPat
case class LambdaScalarPat(label: String) extends LambdaPat
case class LambdaArrayPat(subs: List[LambdaPat]) extends LambdaPat

sealed trait UnresolvedIdentifierL
sealed trait IdentifierL extends UnresolvedIdentifierL

sealed trait Expression extends AbstractExpr

sealed trait PureE extends Expression { val maxEffect = Effect.Pure }

object Literal {
  implicit object JSONDecoder extends JSON.SwitchDecoder[Literal] {
    def readInt(l: Long, stream: JSON.In) = LongL(l)
    def readBoolean(b: Boolean, stream: JSON.In) = BoolL(b)
    def readNull(stream: JSON.In) = NullL
    def readDouble(d: Double, stream: JSON.In) = DoubleL(d)
    def readString(s: String, stream: JSON.In) = StringL(s)

    def readBytes(b: ByteBuf, @unused stream: JSON.In) = BytesL(b)

    def readArrayStart(stream: JSON.In) = {
      val b = List.newBuilder[Literal]
      while (!stream.skipArrayEnd) b += stream.read(this)
      ArrayL(b.result())
    }

    def readObjectStart(stream: JSON.In) = {
      val seen = MSet.empty[String]
      val b = List.newBuilder[(String, Literal)]

      while (!stream.skipObjectEnd) {
        val k = stream.read(JSONParser.ObjectFieldNameSwitch)
        val v = stream.read(this)

        if (!seen(k)) {
          seen += k
          b += k -> v
        }
      }

      ObjectL(b.result())
    }
  }

  def apply(scope: ScopeID, ir: IRValue): Literal =
    ir match {
      case TransactionTimeV(true) => TransactionTimeMicrosL
      case TransactionTimeV(false) => TransactionTimeL
      case LongV(value) => LongL(value)
      case DoubleV(value) => DoubleL(value)
      case TrueV => TrueL
      case FalseV => FalseL
      case NullV => NullL
      case StringV(value) => StringL(value)
      case DocIDV(id) => RefL(scope, id)
      case TimeV(value) => TimeL(value)
      case DateV(value) => DateL(value)
      case UUIDV(id) => UUIDL(id)
      case BytesV(value) => BytesL(value)
      case ArrayV(elems) =>
        ArrayL(elems.iterator map { Literal(scope, _) } toList)
      case MapV(elems) =>
        ObjectL(elems.iterator map { case (k, v) => k -> Literal(scope, v) } toList)
      case q: QueryV =>
        LambdaL(scope, q)
    }

  def unapply(e: Expression): Option[Literal] =
    e match {
      case l: Literal => Some(l)
      case _          => None
    }

  def toIndexTerm(lit: Literal): IndexTerm =
    toIndexTerm(lit, false)

  def toIndexTerm(lit: Literal, reverse: Boolean): IndexTerm =
    IndexTerm(lit.irValue, reverse)

  def fromIndexTerm(scope: ScopeID, term: IndexTerm): Literal =
    Literal(scope, term.value)

  def fromIndexTerms(scope: ScopeID, terms: Vector[IndexTerm]): Literal =
    if (terms.size == 1) {
      Literal(scope, terms.head.value)
    } else {
      ArrayL(terms.iterator.map { t => Literal(scope, t.value) } toList)
    }
}

/**
  * Literals in our expression language represent all scalar values,
  * arrays, objects, and query literals.
  */
sealed trait Literal extends PureE {
  def literal = this
  def rtype: Type = Type.getType(this)
  def irValue: IRValue
}

sealed abstract class ScalarL extends Literal

case object TransactionTimeMicrosL extends ScalarL {
  def irValue = TransactionTimeV.Micros
}

sealed abstract class NumericL extends ScalarL
case class LongL(value: Long) extends NumericL {
  def irValue = LongV(value)
}

case class DoubleL(value: Double) extends NumericL {
  def irValue = DoubleV(value)
}

object BoolL {
  def apply(bool: Boolean): BoolL =
    if (bool) {
      TrueL
    } else {
      FalseL
    }

  def unapply(v: BoolL): Option[Boolean] = {
    if (v.value) {
      SomeTrue
    } else {
      SomeFalse
    }
  }
}

sealed abstract class BoolL(val value: Boolean) extends ScalarL
case object TrueL extends BoolL(true) {
  val irValue = TrueV
}

case object FalseL extends BoolL(false) {
  val irValue = FalseV
}

case object NullL extends ScalarL {
  val irValue = NullV
}

case class StringL(value: String) extends ScalarL {
  def irValue = StringV(value)

  override def equals(o: Any): Boolean = o match {
    case a: ActionL   => a equals this
    case StringL(str) => str equals value
    case _            => false
  }
}

case class BytesL(value: ByteBuf) extends ScalarL {
  def irValue = BytesV(value)
}

sealed abstract class AbstractTimeL extends ScalarL

case class TimeL(value: Timestamp) extends AbstractTimeL {
  def irValue = TimeV(value)
}

case object TransactionTimeL extends AbstractTimeL {
  def irValue = TransactionTimeV.Timestamp
}

case class DateL(value: LocalDate) extends ScalarL {
  def irValue = DateV(value)
}

case class UUIDL(value: UUID) extends ScalarL {
  def irValue = UUIDV(value)
}

sealed abstract class IterableL extends Literal

case class ArrayL(elems: List[Literal]) extends IterableL {
  def irValue = {
    val b = Vector.newBuilder[IRValue]
    b.sizeHint(elems)
    elems foreach { e => b += e.irValue }
    ArrayV(b.result())
  }
}

object ArrayL {
  def apply(elems: Literal*): ArrayL =
    ArrayL(elems.toList)
}

object ObjectL {
  val empty = apply()
  def apply(elems: (String, Literal)*): ObjectL =
    ObjectL(elems.toList)
}

case class ObjectL(elems: List[(String, Literal)]) extends Literal {
  def irValue = MapV(elems map { case (s, l) => s -> l.irValue })
  def toDiff: Diff = Diff(irValue)

  override def equals(other: Any) = other match {
    case other: ObjectL =>
      (this eq other) ||
      ((other canEqual this) && (AList(elems) hasSameElementsAs AList(other.elems)))
    case _              => false
  }

  override def hashCode = MurmurHash3.unorderedHash(elems, 29231)
}

/**
  * A page is very similar to an array literal, with the exception
  * that it maintains extra state for rendering cursors. They are only
  * created by `paginate()`, but may be used as parameters to
  * AbstractArray functions where an array would otherwise be used. They
  * cannot be expressed literally within the language, e.g. by
  * clients.
  */
case class PageL(
  elems: List[Literal],
  unmapped: List[Literal],
  before: Option[Literal],
  after: Option[Literal]) extends IterableL {

  def irValue = {
    val b = List.newBuilder[(String, IRValue)]

    before foreach { v => b += ("before" -> v.irValue) }
    after foreach { v => b += ("after" -> v.irValue) }
    b += ("data" -> ArrayL(elems).irValue)

    MapV(b.result())
  }
}

object EventL {
  def apply(event: Event): EventL =
    event match {
      case i: DocEvent      => DocEventL(i)
      case s: SetEvent      => SetEventL(s)
    }
}

/**
  * Events represent a change to a document or set at a point in
  * time. They are created by read and write functions, and may be
  * returned to us by clients as pagination cursors.
  */
sealed abstract class EventL(val event: Event) extends Literal

case class DocEventL(e: DocEvent) extends EventL(e) {
  def irValue = {
    val b = List.newBuilder[(String, IRValue)]

    b += ("ts" -> e.validTSV)
    b += ("action" -> StringV(e.action.toString))
    b += ("resource" -> DocIDV(e.docID))

    MapV(b.result())
  }
}

case class SetEventL(e: SetEvent) extends EventL(e) {
  def irValue = {
    val b = List.newBuilder[(String, IRValue)]

    b += ("ts" -> e.validTSV)
    b += ("action" -> StringV(e.action.toString))
    b += ("resource" -> DocIDV(e.docID))

    if (e.values.nonEmpty) {
      b += ("values" -> ArrayV(e.values map { _.value }))
    }

    MapV(b.result())
  }
}

/**
  * Actions describe how a version of a document or index entry
  * relates to the document's history.
  *
  * A version may be a create, update, or delete action, implying the
  * presence or absence of a preceeding version in the document's
  * history.
  *
  * An index entry may be an add or remove action, indicating whether
  * the source document is entering or exiting the index at that point
  * in the index's history.
  */
case class ActionL(action: Action) extends ScalarL {
  def irValue = StringV(action.toString)

  def soundEquals(o: Any): Boolean = o match {
    case StringL(str)      => action.toString == str
    case ActionL(a)        => a == action
    case _                 => false
  }

  override def equals(o: Any): Boolean = o match {
    case StringL("create") => action.isCreate
    case StringL("delete") => action.isDelete
    case StringL(str)      => action.toString == str
    case ActionL(a)        => a == action
    case _                 => false
  }
}

case class ElemL(value: Literal, sources: List[EventSet]) extends Literal {
  def irValue =
    MapV("value" -> value.irValue,
      "sources" -> ArrayL(sources map { SetL(_) }).irValue)
}

sealed trait StreamableL extends Literal

/**
  * A version is a document at point in time. They are created by
  * read or write functions, but aren't otherwise expressable in the
  * language.
  */
case class VersionL(version: Version, isNew: Boolean = false) extends StreamableL {
  def irValue = {
    val data = ReadAdaptor.readableData(version)
    val b = List.newBuilder[(String, IRValue)]
    b.sizeHint(data.fields.elems, 3)

    b += ("ref" -> DocIDV(version.docID))
    b += ("class" -> DocIDV(version.collID.toDocID))
    b += ("collection" -> DocIDV(version.collID.toDocID))
    b += ("ts" -> version.validTSV)
    b ++= data.fields.elems

    MapV(b.result())
  }

  def fields(apiVersion: APIVersion): List[(String, Literal)] = {
    val data = ReadAdaptor.readableData(version)
    val b = List.newBuilder[(String, IRValue)]
    b.sizeHint(data.fields.elems, 3)

    b += ("ref" -> DocIDV(version.docID))

    if (apiVersion <= APIVersion.V20) {
      b += ("class" -> DocIDV(version.collID.toDocID))
    }

    b += ("ts" -> version.validTSV)

    if (version.action.isDelete) {
      b += ("action" -> "delete")
    }

    b ++= data.fields.elems

    b.result().map {
      case (k, v) =>
        k -> Literal.apply(version.parentScopeID, v)
    }
  }
}

object CursorL {
  def apply(event: EventL): CursorL = new CursorL(Left(event))
  def apply(elems: List[Literal]): CursorL = new CursorL(Right(ArrayL(elems)))
}

/**
  * Cursors are rendered to clients by paginating a set, and may be
  * returned back to us to continue pagination.
  */
case class CursorL(cursor: Either[EventL, ArrayL]) extends Literal {
  def irValue =
    cursor match {
      case Left(e)  => e.irValue
      case Right(a) => a.irValue
    }
}

/**
  * An anonymous lambda value. It may have closed over values within its
  * environment. Captured values will be serialzed along with the lambda on
  * return from a query or if saved on a record.
  *
  * FIXME: use `@lambda` type tag in new wire protocol, not `@query`.
  *
  * Lambdas are generated in three places: evaluating a LambdaE, from a tagged
  * `@query` literal, or from a stored QueryV. The first two cases are handled
  * similarly in Parser.scala. Since parsing is tied to the Query monad, parsing
  * of a QueryV eagerly is difficult in the contexts were a LambdaL is
  * constructed from one. For this reason it is performed lazily and cached on
  * the LambdaL itself when needed. This also saves other parts of the codebase
  * from implementing their own caching.
  *
  * A LambdaL's internal state is stored as an AtomicReference. If the current
  * state is Unparsed, the current Query thread will parse the QueryV and set
  * the state to Parsed via a CAS operation, then recurse.
  *
  * FIXME: This allows redundant parsing when multiple coroutines are accessing
  * the Lambda's parsedState. This could be avoided by parsing once and having
  * other coroutines wait on the result, but this causes Query evaluation to
  * deadlock.
  */
object LambdaL {
  def apply(expr: LambdaE): LambdaL =
    new LambdaL(Parsed(expr, Map.empty))

  def apply(expr: LambdaE, captured: Map[String, Literal]): LambdaL =
    new LambdaL(Parsed(expr, captured))

  def apply(scope: ScopeID, ir: QueryV): LambdaL =
    new LambdaL(Unparsed(scope, ir))

  // constructor for internal statically constructed lambdas
  def apply(params: String*)(expr: Expression): LambdaL = {
    val pat = params match {
      case Seq(p) => LambdaScalarPat(p)
      case ps => LambdaArrayPat(ps.iterator map LambdaScalarPat toList)
    }

    LambdaL(LambdaE(pat, expr, FreeVars.empty, APIVersion.LambdaDefaultVersion, RootPosition))
  }

  sealed trait State

  final case class Unparsed(scope: ScopeID, ir: QueryV) extends State
  final case class Parsed(expr: LambdaE, captured: Map[String, Literal]) extends State
  final case class Failed(scope: ScopeID, es: List[ParseError], unparsed: Unparsed) extends State
}

final class LambdaL(state: LambdaL.State) extends AtomicReference[LambdaL.State](state) with Literal {

  // Lambdas are equal if they have the same AST and captured values. This will
  // mean that semantically equivalent lambdas have no equality guarantee.
  override def equals(other: Any) = other match {
    case other: LambdaL if this eq other => true
    case other: LambdaL if other canEqual this =>
      (get, other.get) match {
        case (a: LambdaL.Parsed, b: LambdaL.Parsed) =>
          a.expr.pattern == b.expr.pattern &&
          a.expr.body == b.expr.body &&
          a.captured == b.captured

        case (_, _) => irValue == other.irValue
      }
    case _ => false
  }

  def canEqual(other: Any) = other.isInstanceOf[LambdaL]

  override def hashCode = irValue.hashCode

  def parsedState: Query[(LambdaE, Map[String, Literal])] =
    get match {
      case LambdaL.Parsed(expr, captured) =>
        Query.value((expr, captured))

      // Parsing a previously stored lambda should never fail. If that happens
      // this will cause eval to throw an uncaught exception and reported to the
      // error log.
      case LambdaL.Failed(scope, es, unparsed) =>
        Query.fail(new RuntimeException(s"Failed to deserialize stored QueryV in scope $scope. Unparsed: $unparsed Errors: $es"))

      case unparsed @ LambdaL.Unparsed(scope, q) =>
        EscapesParser.parseLambda(EvalAuth(scope), Literal(scope, q.expr), q.apiVersion, RootPosition) flatMap { pr =>
          val s = pr.toEither match {
            case Right(l) => l.get
            case Left(e)  => LambdaL.Failed(scope, e, unparsed)
          }

          compareAndSet(unparsed, s)
          parsedState
        }
    }

  def callMaxEffect: Query[Effect] = parsedState map { _._1.body.maxEffect }

  def estimatedCallMaxEffect: Effect =
    withState({ (expr, _) =>
      expr.maxEffect
    }, { (_, _) =>
      Effect.Write // expect the worst
    })

  def arity: Int =
    withState({ (expr, _) =>
      expr.pattern.arity
    }, { (_, ir) =>
      ir.expr.elems collectFirst {
        case ("lambda", ArrayV(elems)) => elems.length
        case ("lambda", _)             => 1
      } getOrElse 0
    })

  def paramNames: Seq[String] =
    withState({ (expr, _) =>
      expr.pattern.params.toSeq
    }, { (_, ir) =>
      ir.expr.elems collectFirst {
        case ("lambda", ArrayV(elems)) => elems collect { case StringV(str) => str }
        case ("lambda", StringV(p))    => Seq(p)
      } getOrElse Seq.empty
    })

  def isArrayPat: Boolean =
    withState(
      { (expr, _) =>
        expr.pattern.isInstanceOf[LambdaArrayPat]
      },
      { (_, ir) =>
        ir.expr.elems exists {
          case ("lambda", ArrayV(_)) => true
          case _                     => false
        }
      })

  def apiVersion: APIVersion =
    withState((expr, _) => expr.apiVersion, (_, q) => q.apiVersion)

  // A renderable form of the lambda
  // Include the api_version tag if the lambda version or ec version is 3+
  def renderableLiteral(ecVersion: APIVersion): Literal = {
    val vers = apiVersion
    val raw = withState(objectLit, (s, q) => Literal(s, q.expr))

    if ((vers max ecVersion) <= APIVersion.LambdaDefaultVersion) {
      raw
    } else {
      ObjectL(("api_version" -> StringL(vers.toString)) :: raw.asInstanceOf[ObjectL].elems)
    }
  }

  def irValue: QueryV =
    withState((e, c) => QueryV(e.apiVersion, objectLit(e, c).irValue), (_, q) => q)

  private def objectLit(expr: LambdaE, captured: Map[String, Literal]): ObjectL = {
    val body = if (expr.free.isEmpty) {
      expr.body
    } else {
      val b = List.newBuilder[(String, Literal)]
      expr.free.names foreach { n =>
        b += (n -> captured(n))
      }
      LetE(b.result(), expr.body)
    }

    ObjectL("lambda" -> expr.pattern.literal, "expr" -> body.literal)
  }

  private def withState[T](onParsed: (LambdaE, Map[String, Literal]) => T, onUnparsed: (ScopeID, QueryV) => T): T =
    get match {
      case LambdaL.Parsed(expr, c)     => onParsed(expr, c)
      case LambdaL.Unparsed(scope, ir) => onUnparsed(scope, ir)
      case LambdaL.Failed(_, _, s)     => onUnparsed(s.scope, s.ir)
    }
}

object RefL {
  def apply[T <: ID[T]: CollectionIDTag](scope: ScopeID, id: ID[T]): RefL =
    RefL(scope, id.toDocID)
}

/**
  * A Ref is the primary key for a document.
  */
case class RefL(scope: ScopeID, id: DocID)
    extends ScalarL
    with IdentifierL
    with StreamableL {
  def irValue = DocIDV(id)
}

object UnresolvedRefL {
  case class IRException(original: RefParser.RefScope.Ref)
      extends Exception(s"Unresolved ref '$original' cannot be encoded")
}

case class UnresolvedRefL(original: RefParser.RefScope.Ref)
    extends ScalarL
    with UnresolvedIdentifierL {
  def irValue = throw UnresolvedRefL.IRException(original)
}

/** A set literal describes a query plan used to retrieve documents
  * from an index. `paginate()` follows the query plan to materialize
  * data from the index into pages.
  */
case class SetL(set: EventSet) extends IterableL with IdentifierL with StreamableL {
  def irValue: IRValue =
    set match {
      case SchemaSet(_, id) => DocIDV(id)
      case DocSet(_, id)    => DocIDV(id)

      case DocumentsSet(_, id, _) =>
        MapV("@set" -> MapV("documents" -> DocIDV(id.toDocID)))

      case s: IndexSet if s.terms.isEmpty =>
        MapV("@set" -> MapV("match" -> DocIDV(s.config.id.toDocID)))

      case s: IndexSet =>
        val ts = if (s.terms.size == 1) {
          s.terms.head.value
        } else {
          val b = Vector.newBuilder[IRValue]
          b.sizeHint(s.terms)
          s.terms foreach { t => b += t.value }
          ArrayV(b.result())
        }

        MapV("@set" -> MapV("match" -> DocIDV(s.config.id.toDocID), "terms" -> ts))

      case Union(sets) =>
        MapV("@set" -> MapV("union" -> ArrayV(sets.iterator map { s => SetL(s).irValue } toVector)))

      case Intersection(sets) =>
        MapV("@set" -> MapV("intersection" -> ArrayV(sets.iterator map { s => SetL(s).irValue } toVector)))

      case Difference(sets) =>
        MapV("@set" -> MapV("difference" -> ArrayV(sets.iterator map { s => SetL(s).irValue } toVector)))

      case Distinct(set) => MapV("@set" -> MapV("distinct" -> SetL(set).irValue ))

      case LambdaJoin(src, lambda, _) =>
        MapV("@set" -> MapV("join" -> SetL(src).irValue, "with" -> lambda.irValue))

      case IndexJoin(src, idx) =>
        MapV("@set" -> MapV("join" -> SetL(src).irValue, "with" -> DocIDV(idx.id.toDocID)))

      case HistoricalSet(set) => MapV("@set" -> MapV("events" -> SetL(set).irValue))

      case LambdaFilterSet(source, lambda, _) =>
        MapV("@set" -> MapV("filter" -> lambda.irValue, "collection" -> SetL(source).irValue))

      case set => throw new IllegalStateException(s"Unknown set $set.")
    }
}

/**
  * An expression which, when evaluated, will return an anonymous function
  * value, which may have closed over variables within its environment.
  */
object LambdaE {
  def apply(
    pattern: LambdaPat,
    body: Expression,
    free: FreeVars,
    apiVersion: APIVersion,
    pos: Position): LambdaE = {
    val vers = apiVersion max APIVersion.LambdaDefaultVersion
    new LambdaE(pattern, body, free, vers, pos) {}
  }
}

sealed abstract case class LambdaE(
  pattern: LambdaPat,
  body: Expression,
  free: FreeVars,
  apiVersion: APIVersion,
  pos: Position) extends PureE {
  def literal = ObjectL(List("lambda" -> pattern.literal, "expr" -> body.literal))
}

/**
  * A `var` expression is replaced by the value of the binding at
  * `name` in the lexical environment.
  */
case class VarE(name: String) extends PureE {
  def literal = ObjectL("var" -> StringL(name))
}

/**
  * A native callsite applies a Scala thunk from the query language,
  * typically these are QFunctions but they are also used for
  * lambda-joins.
  */
case class NativeCall(eval: EvalContext => Query[R[Literal]], lit: () => Literal, maxEffect: Effect, fnEffect: Effect, pos: Position) extends Expression {
  def literal = lit()
}

/**
  * A `call` expression applies the lambda at `ref` to the provided `args`.
  */
case class CallE(ref: Expression, args: Option[Expression], pos: Position) extends Expression {
  val maxEffect = Effect.Write
  def literal = args match {
    case Some(args) => ObjectL("call" -> ref.literal, "arguments" -> args.literal)
    case None       => ObjectL("call" -> ref.literal)
  }
}

/**
  * An `at` expression sets the valid time of `expr` to `ts`.
  */
case class AtE(ts: Expression, expr: Expression, pos: Position) extends Expression {
  lazy val maxEffect = ts.maxEffect + expr.maxEffect
  def literal = ObjectL("at" -> ts.literal, "expr" -> expr.literal)
}

/**
  * `let` expression semantics follow LET* semantics:
  *
  * let(a -> expr, b -> expr, c -> expr, body) is semantically equivilant to
  * let(a -> expr,
  *   let(b -> expr,
  *     let(c -> expr, body)))
  *
  */
case class LetE(bindings: Bindings, body: Expression) extends Expression {
  lazy val maxEffect = (bindings foldLeft body.maxEffect) { case (eff, (_, v)) => eff + v.maxEffect }
  def literal = ObjectL("let" -> ObjectL(bindings map { case (k, v) => k -> v.literal }), "in" -> body.literal)
}

case class IfE(condExpr: Expression, thenExpr: Expression, elseExpr: Expression, pos: Position) extends Expression {
  lazy val maxEffect = condExpr.maxEffect + thenExpr.maxEffect + elseExpr.maxEffect
  def literal = ObjectL("if" -> condExpr.literal, "then" -> thenExpr.literal, "else" -> elseExpr.literal)
}

case class SelectE(name: String, pathExpr: Expression, fromExpr: Expression, allExpr: Option[Expression], defaultExpr: Option[Expression], pos: Position)
  extends Expression {

  lazy val maxEffect = pathExpr.maxEffect +
    fromExpr.maxEffect +
    allExpr.fold(Effect.Pure: Effect) { _.maxEffect } +
    defaultExpr.fold(Effect.Pure: Effect) { _.maxEffect }

  def literal = {
    val b = List.newBuilder[(String, Literal)]

    b += name -> pathExpr.literal
    b += "from" -> fromExpr.literal

    if (name == "select") {
      allExpr foreach { b += "all" -> _.literal }
    }

    defaultExpr foreach { b += "default" -> _.literal }

    ObjectL(b.result())
  }
}

/**
  * A `do` expression evaluates each expression in its body, returning
  * the value of the final expression.
  */
case class DoE(expr: Expression, exprs: List[Expression]) extends Expression {
  lazy val maxEffect = (exprs foldLeft expr.maxEffect) { (eff, v) => eff + v.maxEffect }
  def literal = ObjectL("do" -> ArrayL(expr.literal :: (exprs map { _.literal })))
}

/**
  * The `object` form is necessary due to JSON encoding: we use JSON
  * object syntax for function calls, so we need this form to stop
  * evaluation and express an object literal.
  */
case class ObjectE(bindings: Bindings) extends Expression {
  lazy val maxEffect = bindings.foldLeft(Effect.Pure: Effect) { case (eff, (_, v)) => eff + v.maxEffect }
  def literal = ObjectL("object" -> ObjectL(bindings map { case (k, v) => k -> v.literal }))
}

/**
  * The array form is encoded as a JSON literal, but may contain
  * arbitrary expressions. An array _literal_ must contain only
  * literal expressions. This distinction allows us to constrain
  * rendering to literal values.
  */
case class ArrayE(value: List[Expression]) extends Expression {
  lazy val maxEffect = value.foldLeft(Effect.Pure: Effect) { (eff, v) => eff + v.maxEffect }
  def literal = ArrayL(value map { _.literal })
}

/**
  * A `and` expression evaluates each expression in its body from left-to-right,
  * stopping the evaluation as soon it finds a false value.
  *
  * @since 3
  */
case class AndE(exprs: List[Expression], pos: Position) extends Expression {
  lazy val maxEffect = exprs.foldLeft(Effect.Pure: Effect) {
    case (acc, expr) => acc + expr.maxEffect
  }
  def literal = ObjectL("and" -> ArrayL(exprs map { _.literal }))
}

/**
  * A `or` expression evaluates each expression in its body left-to-right,
  * stopping the evaluation as soon it finds a true value.
  *
  * @since 3
  */
case class OrE(exprs: List[Expression], pos: Position) extends Expression {
  lazy val maxEffect = exprs.foldLeft(Effect.Pure: Effect) {
    case (acc, expr) => acc + expr.maxEffect
  }
  def literal = ObjectL("or" -> ArrayL(exprs map { _.literal }))
}
