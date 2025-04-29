package fauna.model.runtime.fql2

import fauna.atoms._
import fauna.lang.{ Page, SpinedSeq, Timestamp }
import fauna.lang.syntax._
import fauna.model.runtime.fql2.stdlib.CollectionCompanion
import fauna.model.runtime.fql2.ValueSet.IndexSetHintLimit
import fauna.model.runtime.Effect
import fauna.model.schema.index.CollectionIndex
import fauna.model.schema.NativeIndex
import fauna.model.Cache
import fauna.repo.{ IndexConfig, PagedQuery, Store }
import fauna.repo.query.{ Query, ReadCache }
import fauna.repo.schema.Path
import fauna.repo.values._
import fauna.storage.index.{ IndexTerm, IndexTuple, IndexValue, NativeIndexID }
import fauna.storage.ir.{ DocIDV, IRValue, NullV }
import fql.ast.{ Expr, Literal, Name, Span }
import scala.collection.immutable.ArraySeq
import scala.util.hashing.MurmurHash3

object ValueSet {

  /** The default user-facing page size. */
  val DefaultPageSize = 16

  /** The maximum amount of elements allowed to be materialized at once in eager APIs,
    * such as `toArray`, and `distinct`. Similarly, this value is also used as an
    * internal page size for full set iterations, like `fold`, `reduce`, `foreach`,
    * etc.
    *
    * NOTE: C* has an upper limit of ~32KB per index row. A page size of 16K should
    * allow folding functions to bring ~490MB into memory at each iteration in the
    * worst case scenario.
    */
  val MaximumElements = 16_000

  /** Filtering sets can discard many elements until filling their requested page size.
    * Using page sizes that are too small is known to increase IO and degrade
    * performance. This constant is meant to ensure that filtering sets start with a
    * reasonable initial page size that, in conjunction with `ScaleFactor` finds the
    * resulting elements faster. Be aware that choosing a minimum size too large can
    * increase cost in terms of read-ops charged.
    */
  val MinimumElementsForFiltering = 16

  /** If filtering sets require multiple pages to return the requested number of
    * elements, increase the size of the next page exponentially to speed up finding
    * the desired result set (bounded by `MaximumElements`).
    */
  val ScaleFactor = 4

  /** If an IndexSet is limited to <= this limit (via a TakeSet) then we will not emit performance hints for methods
    * that force full set materialization. The reason for this is that the TakeSet will limit how many elements of
    * the set that we need to materialize.
    */
  val IndexSetHintLimit = 100

  sealed trait Cursor
  case object InitialPage extends Cursor
  final case class Continuation(token: IndexedSeq[Value]) extends Cursor

  final case class PageTok(set: ValueSet, cur: Cursor) {
    def mapSet(f: ValueSet => ValueSet) = copy(set = f(set))
  }
  object PageTok {
    def apply(set: ValueSet, token: IndexedSeq[Value]): PageTok =
      PageTok(set, Continuation(token))
    def apply(set: ValueSet): PageTok = PageTok(set, InitialPage)
  }

  sealed trait PageElem {
    def value: Value
    def tok: () => PageTok
    def withValue(value: Value): PageElem
    def withTok(tok: () => PageTok): PageElem
    def evalPartials[A](fn: Value => Query[A]): Query[A]
    def feedPartialsAndUnpin(): Query[Value]

    def mapSet(f: ValueSet => ValueSet) = {
      val tok0 = tok
      withTok(() => tok0().mapSet(f))
    }
  }

  object PageElem {

    final class ValueElem(val value: Value, val tok: () => PageTok)
        extends PageElem {
      def withValue(value: Value) = new ValueElem(value, tok)
      def withTok(tok: () => PageTok) = new ValueElem(value, tok)
      def evalPartials[A](fn: Value => Query[A]): Query[A] = fn(value)
      def feedPartialsAndUnpin(): Query[Value] = Query.value(value)
    }

    sealed trait IndexElem extends PageElem {
      def docID: DocID
    }

    final class DocElem(val value: Value.Doc, val tok: () => PageTok)
        extends IndexElem {
      def withValue(value: Value) = new ValueElem(value, tok)
      def withTok(tok: () => PageTok) = new DocElem(value, tok)
      def evalPartials[A](fn: Value => Query[A]): Query[A] = fn(value)
      def feedPartialsAndUnpin(): Query[Value] = Query.value(value)
      def docID: DocID = value.id
    }

    final class PartialElem(
      src: IndexSet,
      scopeID: ScopeID,
      override val docID: DocID,
      validTS: Option[Timestamp],
      prefix: ReadCache.Prefix,
      partials: ReadCache.Partials,
      val tok: () => PageTok)
        extends IndexElem {

      def value = Value.Doc(docID, None, validTS, srcHint = Some(srcHint))
      def withValue(value: Value) = new ValueElem(value, tok)
      def withTok(tok: () => PageTok) =
        new PartialElem(src, scopeID, docID, validTS, prefix, partials, tok)

      def evalPartials[A](fn: Value => Query[A]): Query[A] =
        Store.feedPartial(
          srcHint,
          prefix,
          scopeID,
          docID,
          validTS,
          partials
        ) flatMap { cachedDoc =>
          fn(value) ensure {
            cachedDoc.unpin()
            Query.unit
          }
        }

      def feedPartialsAndUnpin(): Query[Value] =
        Store
          .feedPartial(
            srcHint,
            prefix,
            scopeID,
            docID,
            validTS,
            partials
          )
          .map { cachedDoc =>
            cachedDoc.unpin()
            value
          }

      private lazy val srcHint: ReadCache.CachedDoc.SetSrcHint =
        ReadCache.CachedDoc.SetSrcHint(
          scopeID = src.config.scopeID,
          indexID = src.config.id,
          collectionName = src.parent.name,
          collectionId = src.parent.collID,
          indexName = src.name,
          /** Array paths aren't stored in partials so we remove any covered values for array paths.
            */
          coveredFieldPaths = src.coveredValues.collect {
            case cv if !cv.entry.fieldPath.exists(_.isLeft) =>
              cv.entry.fieldPath.collect { case Right(field) =>
                field
              }
          },
          span = src.span
        )
    }

    def apply(value: Value, tok: () => PageTok): ValueElem = {
      new ValueElem(value, tok)
    }

    def apply(doc: Value.Doc, tok: () => PageTok): DocElem = {
      new DocElem(doc, tok)
    }

    def apply(
      src: IndexSet,
      scopeID: ScopeID,
      docID: DocID,
      validTS: Option[Timestamp],
      prefix: ReadCache.Prefix,
      partials: ReadCache.Partials,
      tok: () => PageTok) =
      new PartialElem(src, scopeID, docID, validTS, prefix, partials, tok)
  }

  final case class Page(elems: Iterable[PageElem], after: Option[PageTok]) {

    def mapSet(f: ValueSet => ValueSet) =
      Page(
        elems.iterator.map(_.mapSet(f)).to(Iterable),
        after.map(_.mapSet(f))
      )

    def toValue(ctx: FQLInterpCtx): Query[Value.Struct] = {
      val elemsQ = elems.map(_.feedPartialsAndUnpin()).sequence

      elemsQ.map { elems =>
        val b = Value.Struct.newBuilder
        b += ("data" -> elems.to(Value.Array))
        after.foreach { tok =>
          b += ("after" -> setCursor(
            tok.set,
            Some(tok),
            ctx.systemValidTime,
            elems.size
          ))
        }
        b.result()
      }
    }
  }

  object Page {
    val empty = Page(Nil, None)
  }

  def setCursor(set: ValueSet): Value.SetCursor =
    setCursor(set, None, None, set.initialPageSize)

  def setCursor(
    set: ValueSet,
    tok: Option[PageTok],
    systemValidTS: Option[Timestamp],
    size: Int) = {
    tok match {
      case None =>
        val (expr, closure) = ValueReification.reify(set)
        Value.SetCursor(expr, closure, None, systemValidTS, size)
      case Some(PageTok(set, cur)) =>
        val (expr, closure) = ValueReification.reify(set)
        val ords = cur match {
          case ValueSet.InitialPage => None
          case ValueSet.Continuation(c) =>
            Some(c.map(Value.toIR(_).toOption.get).toVector)
        }
        Value.SetCursor(expr, closure, ords, systemValidTS, size)
    }
  }

  private[fql2] def timeToValue(time: Option[Timestamp]) =
    time match {
      case Some(t) => Value.Time(t)
      case None    => Value.Null(Span.Null)
    }
  private[fql2] def timeFromValue(value: Value): Option[Timestamp] =
    value match {
      case Value.Time(t) => Some(t)
      case Value.Null(_) => None
      case _             => throw new IllegalStateException(s"invalid time: $value")
    }

  private[fql2] def disallowWrites(ctx: FQLInterpCtx): FQLInterpCtx =
    ctx.withEffectLimit(Effect.Limit(Effect.Read, "set functions"))
}

sealed trait ValueSet extends Value.Set with Product {
  protected def `Only fauna.model.runtime.fql2.ValueSet may extend Value.Set`() = {}

  // We define equals/hashCode in Value, but we want to implement set structural
  // equality per case-class, hence the next two method overrides.

  override def equals(other: Any): Boolean = {
    if (getClass != other.getClass) {
      return false
    }

    val iter = productIterator
    val oiter = other.asInstanceOf[Product].productIterator
    iter.corresponds(oiter) { _ == _ }
  }

  override def hashCode = MurmurHash3.productHash(this)

  // usage interface

  def initialPageSize: Int

  /** Get a page from a set.
    *
    * paginate() is evaluated in the context of FQL, hence it takes an
    * interpreter context and a span pointing to the calling function
    * (paginate(), take(), toArray, etc.)
    *
    * It also takes a page count, and the elements from the original cursor: The
    * starting token, direction, and the cursor's snapshot time, if the call
    * originates from a serialized cursor. See SetCursor.scala
    */
  def paginate(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    // JVM problems...
    require(count > 0, s"count must be > 0, but received $count")
    paginateImpl(ctx, count, cursor)
  }

  def reverse: ValueSet = ReverseSet(this)

  /** Called by FQL value materialization to transform a set into its first
    * page.
    */
  def materializedPage(
    ctx: FQLInterpCtx,
    count: Option[Int]): Query[Result[ValueSet.Page]] =
    paginate(ctx, count.getOrElse(initialPageSize), ValueSet.InitialPage)

  /** Implementation of the FQL any() method. */
  def any(
    ctx: FQLInterpCtx,
    predicate: Value.Func
  ): Query[Result[Value.Boolean]] =
    anyImpl(ctx, predicate, Value.False)

  /** Implementation of the FQL every() method. */
  def every(
    ctx: FQLInterpCtx,
    predicate: Value.Func
  ): Query[Result[Value.Boolean]] =
    anyImpl(ctx, predicate, Value.True)

  /** Implements `any` and `every`. `anyEveryImpl` sounds worse :P
    */
  private def anyImpl(
    ctx: FQLInterpCtx,
    predicate: Value.Func,
    default: Value.Boolean): Query[Result[Value.Boolean]] =
    Result.guardM {
      if (!predicate.arity.accepts(1)) {
        Result.fail(
          QueryRuntimeFailure
            .InvalidFuncParamArity("predicate", 1, predicate.arity, ctx.stackTrace))
      }

      // Annotated to guide inference on the foldLeft below.
      val seed: Query[Result[Value.Boolean]] =
        Query.value(Result.Ok(default))

      def any0(
        set: ValueSet,
        cursor: ValueSet.Cursor): Query[Result[Value.Boolean]] =
        set.paginateImpl(ctx, ValueSet.MaximumElements, cursor) flatMapT { page =>
          page.elems
            .foldLeft(seed) { case (q, e) =>
              q flatMap {
                case Result.Ok(`default`) =>
                  e.evalPartials { value =>
                    ctx.evalApply(predicate, ArraySeq(value)) flatMapT {
                      case v @ Value.Boolean(_) => Result.Ok(v).toQuery
                      case Value.Null(_) => Result.Ok(Value.Boolean(false)).toQuery
                      case v =>
                        Result.fail(QueryRuntimeFailure
                          .InvalidType(ValueType.BooleanType, v, ctx.stackTrace))
                    }
                  }
                case value => Query.value(value)
              }
            }
            .flatMapT {
              case `default` if page.after.isDefined =>
                val tok = page.after.get
                any0(tok.set, tok.cur)
              case res => Result.Ok(res).toQuery
            }
        }

      any0(this, ValueSet.InitialPage)
    }

  /** Implementation of the FQL first() method. */
  def first(ctx: FQLInterpCtx): Query[Result[Value]] =
    paginate(ctx, 1, ValueSet.InitialPage)
      .flatMapT {
        _.elems.headOption match {
          case Some(head) => head.feedPartialsAndUnpin().map(_.toResult)
          case None =>
            Value.Null
              .noSuchElement("empty set", ctx.stackTrace.currentStackFrame)
              .toQuery
        }
      }

  /** Implementation of the FQL isEmpty() method. */
  def isEmpty(ctx: FQLInterpCtx): Query[Result[Value.Boolean]] =
    paginate(ctx, 1, ValueSet.InitialPage)
      .mapT { v => Value.Boolean(v.elems.isEmpty) }

  /** Implementation of the FQL includes() method. */
  def includes(ctx: FQLInterpCtx, value: Value): Query[Result[Value.Boolean]] = {
    def includes0(
      set: ValueSet,
      cursor: ValueSet.Cursor): Query[Result[Value.Boolean]] =
      set.paginateImpl(ctx, ValueSet.MaximumElements, cursor) flatMapT { page =>
        if (page.elems.exists(e => e.value == value)) {
          Result.Ok(Value.True).toQuery
        } else {
          page.after match {
            case None      => Result.Ok(Value.False).toQuery
            case Some(tok) => includes0(tok.set, tok.cur)
          }
        }
      }

    includes0(this, ValueSet.InitialPage)
  }

  /** Implementation of the FQL nonEmpty() method. */
  def nonEmpty(ctx: FQLInterpCtx): Query[Result[Value.Boolean]] =
    isEmpty(ctx).mapT { !_ }

  /** Implementation of the FQL foreach() method. */
  def foreach(
    ctx: FQLInterpCtx,
    fn: Value.Func
  ): Query[Result[Value.Null]] = {
    def foreach0(set: ValueSet, cursor: ValueSet.Cursor): Query[Result[Value.Null]] =
      set.paginateImpl(ctx, ValueSet.MaximumElements, cursor) flatMapT { page =>
        val callQ = page.elems map { e =>
          e.evalPartials { value =>
            ctx.evalApply(fn, ArraySeq(value))
          }
        }

        callQ.joinT flatMapT { _ =>
          page.after match {
            case None =>
              Query.value(Value.Null(ctx.stackTrace.currentStackFrame).toResult)
            case Some(tok) => foreach0(tok.set, tok.cur)
          }
        }
      }

    foreach0(this, ValueSet.InitialPage)
  }

  def aggregate(
    ctx: FQLInterpCtx,
    seed: Value,
    combiner: Value.Func
  ): Query[Result[Value]] = {

    def aggregate0(
      agg: Value,
      set: ValueSet,
      cursor: ValueSet.Cursor): Query[Result[Value]] =
      set.paginateImpl(ctx, ValueSet.MaximumElements, cursor) flatMapT { page =>
        if (page.elems.isEmpty) {
          Result.Ok(agg).toQuery
        } else {
          aggregate1(page.elems, page.after)(
            (elemA, elemB) =>
              elemA.evalPartials { a =>
                elemB.evalPartials { b =>
                  ctx.evalApply(combiner, ArraySeq(a, b))
                }
              },
            _.value
          ).flatMapT { value =>
            ctx.evalApply(combiner, ArraySeq(value, agg))
          }
        }
      }

    def aggregate1[A](elems: Iterable[A], after: Option[ValueSet.PageTok])(
      combiner0: (A, A) => Query[Result[Value]],
      toValue: A => Value): Query[Result[Value]] = {
      elems.iterator
        .grouped(2)
        .map {
          case Seq(a, b) => combiner0(a, b)
          case Seq(a)    => Query.value(Result.Ok(toValue(a)))
          case _         => sys.error("grouped(2) gave an invalid result")
        }
        .toSeq
        .sequenceT
        .flatMapT { elems =>
          if (elems.lengthIs == 1) {
            after match {
              case None      => Result.Ok(elems.head).toQuery
              case Some(tok) => aggregate0(elems.head, tok.set, tok.cur)
            }
          } else {
            aggregate1(elems, after)(
              (a, b) => ctx.evalApply(combiner, ArraySeq(a, b)),
              identity
            )
          }
        }
    }

    aggregate0(seed, this, ValueSet.InitialPage)
  }

  def foldLeft(
    ctx: FQLInterpCtx,
    seed: Value,
    fn: Value.Func
  ): Query[Result[Value]] =
    foldPage(ctx, this, seed, ValueSet.InitialPage) { case (acc, elem) =>
      elem.evalPartials { value =>
        ctx.evalApply(fn, ArraySeq(acc, value))
      }
    }

  def foldLeft(ctx: FQLInterpCtx, seed: Value)(
    combiner: (Value, Value) => Query[Result[Value]]): Query[Result[Value]] =
    foldPage(ctx, this, seed, ValueSet.InitialPage) { case (acc, elem) =>
      elem.evalPartials { combiner(acc, _) }
    }

  def foldRight(
    ctx: FQLInterpCtx,
    seed: Value,
    fn: Value.Func
  ): Query[Result[Value]] =
    reverse.foldLeft(ctx, seed, fn)

  def reduceLeft(
    ctx: FQLInterpCtx,
    fn: Value.Func
  ): Query[Result[Value]] = {
    val count = ValueSet.MaximumElements
    val cursor = ValueSet.InitialPage
    paginateImpl(ctx, count, cursor) flatMapT { page =>
      if (page.elems.isEmpty) {
        Value.Null
          .noSuchElement("empty set", ctx.stackTrace.currentStackFrame)
          .toQuery
      } else if (page.elems.sizeIs == 1) {
        Query.value(Result.Ok(page.elems.head.value))
      } else {
        val seed = page.elems.head.value
        foldAccumulate(
          ctx,
          seed,
          Query.value(Page(page.elems.tail)),
          page.after
        ) { case (acc, elem) =>
          elem.evalPartials { value =>
            ctx.evalApply(fn, ArraySeq(acc, value))
          }
        }
      }
    }
  }

  def reduceRight(
    ctx: FQLInterpCtx,
    fn: Value.Func
  ): Query[Result[Value]] =
    reverse.reduceLeft(ctx, fn)

  def toArray(ctx: FQLInterpCtx) = {
    materialize(ctx) flatMapT { elems =>
      val arrQ = elems.map(_.feedPartialsAndUnpin()).sequence

      arrQ.flatMap { arrSeq =>
        val arr = arrSeq.to(ArraySeq)
        Query.addCompute(arr.size).map { _ =>
          Value.Array(arr).toResult
        }
      }
    }
  }

  final def toEventSource(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    forceSnapshotBarrier(ctx.scopeID) {
      toEventSourceImpl(ctx, method)
    }

  // NB. An empty `watchedFields` means "watch all".
  final def changesOn(
    ctx: FQLInterpCtx,
    watchedFields: Seq[Path],
    method: String
  ): Query[Result[Value.EventSource]] =
    forceSnapshotBarrier(ctx.scopeID) {
      toEventSourceImpl(ctx, method) flatMapT { stream =>
        this match {
          // NB. Can only watch for changes at the source set.
          case set: IndexSet =>
            validateWatchedFields(ctx, set, watchedFields) flatMapT { _ =>
              stream.copy(watchedFields = watchedFields).toQuery
            }

          case _: SingletonSet =>
            stream.copy(watchedFields = watchedFields).toQuery

          case _ =>
            QueryRuntimeFailure
              .InvalidReceiver(
                ".changesOn()",
                "it was not called on the source set.",
                ctx.stackTrace)
              .toQuery
        }
      }
    }

  // NOTE: Streams are often created in a pure context, such as:
  // `Foo.all().toStream()`. Since no uncached reads are performed, the resulting
  // query's ts is a snapshot time chosen by the coordinator without a LAT wait.
  // Subsequent writes may complete at lower transaction times, making their effects
  // invisible to the stream. As a result, we've observed streams that were unable to
  // resume from a document created after the stream value since it had a ts lower
  // than the stream's start time.
  //
  // This function forces a snapshot time barrier on streams by issuing a read so
  // that a LAT wait occurs for its snapshot time. A schema version refresh was
  // chosen as the read here so that the query may discover stale cache entries, such
  // as truncated collection where, if deleted and recreated with the same name, may
  // cause the cache to hand over an outdated collection ID for the stream value.
  private def forceSnapshotBarrier(scope: ScopeID)(
    streamQ: Query[Result[Value.EventSource]]) =
    Query.state flatMap { state =>
      if (state.isPure) {
        Cache.refreshLastSeenSchema(scope) flatMap { _ =>
          streamQ
        }
      } else {
        streamQ
      }
    }

  private def validateWatchedFields(
    ctx: FQLInterpCtx,
    set: IndexSet,
    watchedFields: Seq[Path]
  ): Query[Result[Unit]] =
    // .all() can watch anything.
    if (
      set.config.id == NativeIndexID.DocumentsByCollection.id ||
      set.config.id == NativeIndexID.ChangesByCollection.id
    ) {
      Result.Ok(()).toQuery
    } else {
      val coveredPaths = set.coveredValues.view.map { _.entry.fieldPath }.toSet
      // Any watched fields are allowed when index covers the ts field.
      if (coveredPaths.contains(Right("ts") :: Nil)) {
        Result.Ok(()).toQuery
      } else {
        // Id field is always coverd by any index.
        val nonCoveredFields =
          watchedFields.view
            .map { _.elements }
            .filter { _ != Right("id") :: Nil }
            .filterNot { coveredPaths(_) }

        if (nonCoveredFields.isEmpty) {
          Result.Ok(()).toQuery
        } else {
          val fieldsStr =
            nonCoveredFields.view
              .map { elems => new Path(elems) }
              .map { p => s"`$p`" }
              .mkString(", ")

          QueryRuntimeFailure
            .InvalidArgument(
              "fields",
              s"$fieldsStr not covered by the `${set.name}` index.",
              ctx.stackTrace)
            .toQuery
        }
      }
    }

  def concat(other: Value.Set): Value.Set = {
    ConcatSet(this, other)
  }

  // internal abstract API

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]]

  protected[fql2] def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]]

  // Pages through this set, recursing with `foldAccumulate` to produce
  // an accumulated result.
  private def foldPage(
    ctx: FQLInterpCtx,
    set: ValueSet,
    acc: Value,
    cursor: ValueSet.Cursor
  )(combiner: (Value, ValueSet.PageElem) => Query[Result[Value]])
    : Query[Result[Value]] =
    set.paginateImpl(ctx, ValueSet.MaximumElements, cursor) flatMapT { page =>
      if (page.elems.isEmpty) {
        Result.Ok(acc).toQuery
      } else {
        foldAccumulate(ctx, acc, Query.value(Page(page.elems)), page.after)(combiner)
      }
    }

  // Accumulates intermediate results for a page, recursing with
  // `foldPage` if any additonal pages should be read.
  private def foldAccumulate(
    ctx: FQLInterpCtx,
    acc: Value,
    page: PagedQuery[Iterable[ValueSet.PageElem]],
    after: Option[ValueSet.PageTok]
  )(combiner: (Value, ValueSet.PageElem) => Query[Result[Value]])
    : Query[Result[Value]] = {
    val accQ = page.foldLeftValuesMT(Result.Ok(acc): Result[Value]) {
      case (Result.Ok(acc), e) => combiner(acc, e)
      // Propagate the error through this page; no further pages will be read.
      case (ex: Result.Err, _) => Query.value(ex)
    }

    accQ flatMapT { acc =>
      after match {
        case None      => acc.toQuery
        case Some(tok) => foldPage(ctx, tok.set, acc, tok.cur)(combiner)
      }
    }
  }

  /** Materializes the whole set, failing if its size exceeds the given limit.
    *
    * NOTE: This is an internal API made public for testing. Prefer `paginate`
    * instead.
    */
  def materialize(
    ctx: FQLInterpCtx,
    limit: Int = ValueSet.MaximumElements
  ): Query[Result[Seq[ValueSet.PageElem]]] = {

    // NOTE: over fetch so that we fail on large sets rather than capping them.
    val count = limit + 1

    def materialize0(
      set: ValueSet,
      cursor: ValueSet.Cursor,
      elems: Seq[ValueSet.PageElem],
      remaining: Int): Query[Result[Seq[ValueSet.PageElem]]] = {

      val pageSize = remaining.min(ValueSet.MaximumElements)

      set.paginateImpl(ctx, pageSize, cursor) flatMapT { page =>
        val acc = elems ++ page.elems
        // Limit exceeded. Fail this query.
        if (acc.sizeIs > limit) {
          Result
            .Err(
              QueryRuntimeFailure.ValueTooLarge(
                s"exceeded maximum number of elements (limit=$limit)",
                ctx.stackTrace
              ))
            .toQuery
        } else {
          page.after
            .map(t => materialize0(t.set, t.cur, acc, count - acc.size))
            .getOrElse(Result.Ok(acc).toQuery)
        }
      }
    }

    materialize0(this, ValueSet.InitialPage, SpinedSeq.empty, count)
  }

  /** Used to determine if a set should include performance hints.
    * Performance hints are emitted for a set if a method that requires full materialization of an IndexSet is invoked.
    * This method will return true if an unbound IndexSet is included anywhere in the set hierarchy.
    * If the IndexSet is wrapped in a TakeSet and the TakeSet limit is <= 100, hints will not be required. This is
    * because the set materialization will be limited to <= 100 elements.
    */
  def shouldIncludePerformanceHints: Boolean = {
    this match {
      case _: IndexSet                      => true
      case DropSet(inner, _)                => inner.shouldIncludePerformanceHints
      case _: ArraySet                      => false
      case _: SingletonSet                  => false
      case ReverseSet(inner)                => inner.shouldIncludePerformanceHints
      case DistinctSet(inner)               => inner.shouldIncludePerformanceHints
      case WhereFilterSet(inner, _, _, _)   => inner.shouldIncludePerformanceHints
      case FlatMapSet(inner, _, _, _, _, _) => inner.shouldIncludePerformanceHints
      case ConcatSet(left, right) =>
        left.shouldIncludePerformanceHints || right.shouldIncludePerformanceHints
      case OrderedSet(inner, _, _)      => inner.shouldIncludePerformanceHints
      case PagedSet(inner, _, _)        => inner.shouldIncludePerformanceHints
      case ProjectedSet(inner, _, _, _) => inner.shouldIncludePerformanceHints
      case _: SequenceSet               => false
      case TakeSet(_, limit) if limit < IndexSetHintLimit => false
      case TakeSet(inner, _) => inner.shouldIncludePerformanceHints
    }
  }
}

final case class DistinctSet(inner: ValueSet) extends ValueSet {

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) =
    Expr.MethodChain(
      ctx.save(inner),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("distinct", Span.Null),
          Seq.empty,
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )

  override def reverse = copy(inner = inner.reverse)

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    require(
      cursor == ValueSet.InitialPage,
      s"redundant materialization during a continuation. (cursor=$cursor)")

    inner.materialize(ctx) flatMapT { elems =>
      val uniq = elems.map(_.value).sorted.distinct

      ArraySet(Value.Array(uniq.to(ArraySeq))).paginateImpl(ctx, count, cursor)
    }
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on distinct sets.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

final case class DropSet(inner: ValueSet, elements: Int) extends ValueSet {

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(inner),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("drop", Span.Null),
          Seq(ctx.save(Value.Int(elements))),
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    def drop(
      set: ValueSet,
      cursor: ValueSet.Cursor,
      acc: Iterable[ValueSet.PageElem],
      remaining: Int): Query[Result[ValueSet.Page]] = {
      val size = (remaining + count) min ValueSet.MaximumElements
      set.paginateImpl(ctx, size, cursor) flatMapT { page =>
        page.after match {
          case Some(after) if remaining + count > ValueSet.MaximumElements =>
            // There are more elements to read, and more elements were requested. So
            // we need to keep paginating.
            drop(
              after.set,
              after.cur,
              acc ++ page.elems.drop(remaining),
              remaining - page.elems.size)
          case _ =>
            // We've got all the elements requested (or, the inner set is empty), so
            // return that page.
            Result
              .Ok(ValueSet.Page(acc ++ page.elems.drop(remaining), page.after))
              .toQuery
        }
      }
    }

    drop(inner, cursor, Nil, elements)
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on sets returned from `.drop()`.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

object IndexSet {

  /** A prefix of covered values to restrict the set pagination. */
  final case class Prefix(values: Vector[IRValue] = Vector.empty) extends AnyVal {

    private[IndexSet] def validate(cfg: IndexConfig): Unit = {
      require(
        values.size <= (cfg.values.size + 1),
        s"prefix had size ${values.size} but index has only ${cfg.values.size + 1} covered values (including docID)")

      if (values.size == (cfg.values.size + 1)) {
        require(
          values.last.isInstanceOf[DocIDV],
          s"received non docID value ${values.last} in docID parameter slot.")
      }
    }

    private[IndexSet] def merge(cfg: IndexConfig, tuple: IndexTuple): IndexTuple = {
      val tupleVals = Vector.newBuilder[IndexTerm]
      val valIter = values.iterator
      val cfgIter = cfg.reverseFlags.iterator

      while (cfgIter.hasNext && valIter.hasNext) {
        tupleVals += IndexTerm(valIter.next(), cfgIter.next())
      }

      val docIDVal = if (valIter.hasNext) Some(valIter.next()) else None

      docIDVal match {
        case None             => tuple.copy(values = tupleVals.result())
        case Some(DocIDV(id)) => tuple.copy(values = tupleVals.result(), docID = id)
        case _ =>
          sys.error(
            s"Unexpected invalid state! validate() was not called previously.")
      }
    }
  }

  /** A range of covered values to to restrict set pagination. */
  object Range {
    val Unbounded = Range(left = None, right = None)
  }

  final case class Range(left: Option[Prefix], right: Option[Prefix]) {

    def from(cfg: IndexConfig, reversed: Boolean): IndexTuple =
      merge(cfg, left, if (reversed) IndexTuple.MaxValue else IndexTuple.MinValue)

    def to(cfg: IndexConfig, reversed: Boolean): IndexTuple =
      merge(cfg, right, if (reversed) IndexTuple.MinValue else IndexTuple.MaxValue)

    private def merge(
      cfg: IndexConfig,
      prefix: Option[Prefix],
      value: IndexTuple): IndexTuple =
      prefix.fold(value) { _.merge(cfg, value) }

    private[IndexSet] def validate(cfg: IndexConfig): Unit = {
      left.foreach(_.validate(cfg))
      right.foreach(_.validate(cfg))
    }
  }

  final case class CoveredValue(
    entry: CollectionIndex.Entry,
    partialReadPath: Option[ReadCache.Path])

  def extractPartials(
    coveredValues: Seq[CoveredValue],
    terms: Vector[IndexTerm],
    values: Vector[IndexTerm]): ReadCache.Partials =
    coveredValues.view
      .map { _.partialReadPath }
      .zip(terms.view ++ values.view)
      .collect {
        // Elide null values from partials in the read cache. This avoids partials
        // pointing to fields in nested documents (which will always be null).
        case (Some(path), value) if value.value != NullV =>
          path -> Value.fromIR(value.value, Span.Null)
      }
      .toMap
}

final case class IndexSet(
  parent: CollectionCompanion,
  name: String,
  args: IndexedSeq[Value],
  config: IndexConfig,
  terms: Vector[IndexTerm],
  validTime: Option[Timestamp],
  /** Used to emit hints if this IndexSet is materialized into documents. */
  span: Span,
  range: IndexSet.Range = IndexSet.Range.Unbounded,
  coveredValues: Seq[IndexSet.CoveredValue] = Seq.empty,
  filter: Option[(IndexConfig, ValueSet.PageElem.IndexElem) => Query[Boolean]] =
    None,
  reversed: Boolean = false,
  omitSetsReadCount: Boolean = false)
    extends ValueSet {

  /** Override equals and hashcode to not include the span
    */
  override def equals(other: Any): Boolean = other match {
    case that: IndexSet =>
      parent == that.parent &&
      name == that.name &&
      args == that.args &&
      config == that.config &&
      terms == that.terms &&
      validTime == that.validTime &&
      range == that.range &&
      coveredValues == that.coveredValues &&
      filter == that.filter &&
      reversed == that.reversed &&
      omitSetsReadCount == that.omitSetsReadCount
    case _ => false
  }

  /** Override equals and hashcode to not include the span
    */
  override def hashCode(): Int = {
    val state = Seq[Any](
      parent,
      name,
      args,
      config,
      terms,
      validTime,
      range,
      coveredValues,
      filter,
      reversed,
      omitSetsReadCount
    )
    MurmurHash3.seqHash(state)
  }

  override def initialPageSize: Int = ValueSet.DefaultPageSize

  private def omitNextSetReadsCount() =
    if (omitSetsReadCount) this else copy(omitSetsReadCount = true)

  range.validate(config)

  def reify(ctx: ValueReification.ReifyCtx) = {
    val reverseSeq = if (reversed) {
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("reverse", Span.Null),
          Seq.empty,
          selectOptional = false,
          applyOptional = None,
          Span.Null))
    } else {
      Seq.empty
    }

    val chain =
      Expr.MethodChain(
        ctx.save(parent),
        Seq(
          Expr.MethodChain
            .MethodCall(
              Span.Null,
              Name(name, Span.Null),
              args.map(ctx.save),
              selectOptional = false,
              applyOptional = None,
              Span.Null)
        ) ++ reverseSeq,
        Span.Null
      )

    validTime match {
      case Some(t) =>
        // reconstruct an at() surrounding the index set construction.
        Expr.At(
          Expr.MethodChain(
            Expr.Id("Time", Span.Null),
            Seq(
              Expr.MethodChain
                .Apply(
                  Seq(Expr.Lit(Literal.Str(t.toString()), Span.Null)),
                  None,
                  Span.Null)),
            Span.Null),
          chain,
          Span.Null
        )
      case None => chain
    }
  }

  override def reverse =
    copy(reversed = true, range = range.copy(left = range.right, right = range.left))

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    require(count > 0, s"cannot paginate IndexSet with count of $count <= 0")

    val ts = validTime.getOrElse(ctx.readValidTime)

    val pageQ = cursor match {
      case ValueSet.InitialPage =>
        firstPage(ctx, count, ts)

      case ValueSet.Continuation(tok) if reversed =>
        prevPage(ctx, count, tok, ts)

      case ValueSet.Continuation(tok) =>
        nextPage(ctx, count, tok, ts)
    }

    pageQ map { Result.Ok(_) }
  }

  private def maybeFilter(
    ctx: FQLInterpCtx,
    q: PagedQuery[Iterable[ValueSet.PageElem.IndexElem]]) = {
    val filtered = filter match {
      case Some(fn) => q.selectMT { v => v.evalPartials { _ => fn(config, v) } }
      case None     => q
    }
    validTime match {
      case Some(_) =>
        filtered selectMT { v =>
          v.evalPartials { _ =>
            ctx.auth.checkHistoryReadPermission(ctx.scopeID, v.docID)
          }
        }
      case None =>
        filtered selectMT { v =>
          v.evalPartials { _ =>
            ctx.auth.checkReadPermission(ctx.scopeID, v.docID)
          }
        }
    }
  }

  private def getPage(
    ctx: FQLInterpCtx,
    count: Int,
    q: PagedQuery[Iterable[IndexValue]]): Query[Seq[ValueSet.PageElem]] = {

    val nextSet = this.omitNextSetReadsCount()

    def tok(value: IndexValue)() = {
      val vs = value.tuple.values.map(v => Value.fromIR(v.value, Span.Null))
      ValueSet.PageTok(nextSet, vs :+ Value.Doc(value.docID))
    }

    def buildPageElem(value: IndexValue): Query[ValueSet.PageElem.IndexElem] =
      if (coveredValues.isEmpty) {
        Query.value(
          ValueSet.PageElem(
            Value.Doc(value.docID, None, validTime),
            tok(value) _
          ))
      } else {
        Store.writePrefix(value.scopeID, value.docID) map { prefix =>
          ValueSet.PageElem(
            this,
            value.scopeID,
            value.docID,
            validTime,
            prefix,
            IndexSet.extractPartials(
              coveredValues,
              terms,
              value.tuple.values
            ),
            tok(value) _
          )
        }
      }

    val setsRead = if (omitSetsReadCount) 0 else 1 // omit sets read after first page
    Query.readAndIncrStats(Query.addSets(setsRead, config.partitions.toInt, _)) {
      maybeFilter(ctx, q.mapValuesMT { buildPageElem(_) })
        .takeT(count + 1)
        .flattenT
    }
  }

  private def firstPage(ctx: FQLInterpCtx, count: Int, snapshotTS: Timestamp) = {
    val query =
      Store.collection(
        config,
        terms map { _.value },
        snapshotTS,
        range.from(config, reversed),
        range.to(config, reversed),
        count + 1, // +1 for a cursor
        ascending = !reversed
      )

    getPage(ctx, count, query) map { page =>
      val after = page.lift(count) map { _.tok() }
      ValueSet.Page(page.take(count), after)
    }
  }

  private def mkIndexTuple(base: IndexTuple, tok: IndexedSeq[Value]) = {
    def oops() = throw new IllegalStateException(s"invalid ord token $tok")
    val values = tok.init
      .zip(config.reverseFlags)
      .map { case (v, rev) =>
        val ir = Value.toIR(v).toOption match {
          case Some(v) => v
          case None    => oops()
        }
        IndexTerm(ir, rev)
      }
      .toVector
    val doc = tok.last match {
      case v: Value.Doc => v.id
      case _            => oops()
    }

    base.copy(scopeID = config.scopeID, docID = doc, values = values)
  }

  private def prevPage(
    ctx: FQLInterpCtx,
    count: Int,
    token: IndexedSeq[Value],
    snapshotTS: Timestamp) = {
    require(reversed, s"Cannot iterate in reverse on a forward set.")
    val from = mkIndexTuple(IndexTuple.MaxValue, token)

    val pageQ =
      Store.collection(
        config,
        terms map { _.value },
        snapshotTS,
        from,
        range.to(config, reversed),
        count + 1, // +1 for a cursor
        ascending = false
      )

    getPage(ctx, count, pageQ) map { page =>
      val after = page.lift(count) map { _.tok() }
      ValueSet.Page(page.take(count), after)
    }
  }

  private def nextPage(
    ctx: FQLInterpCtx,
    count: Int,
    token: IndexedSeq[Value],
    snapshotTS: Timestamp) = {
    require(!reversed, s"Cannot iterate forward on a reversed set.")
    val from = mkIndexTuple(IndexTuple.MinValue, token)

    val pageQ =
      Store.collection(
        config,
        terms map { _.value },
        snapshotTS,
        from,
        range.to(config, reversed),
        count + 1, // +1 for a cursor
        ascending = true
      )

    getPage(ctx, count, pageQ) map { page =>
      val after = page.lift(count) map { _.tok() }
      ValueSet.Page(page.take(count), after)
    }
  }

  private def isValidConfig =
    config.id match {
      case NativeIndexID(
            NativeIndexID.DocumentsByCollection |
            NativeIndexID.ChangesByCollection) =>
        terms match {
          // Disallow `Database.all()`, for example.
          case Vector(IndexTerm(DocIDV(CollectionID(UserCollectionID(_))), _)) =>
            true
          case _ =>
            false
        }
      case UserIndexID(_) => true
      case _              => false
    }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    if (validTime.isDefined) {
      QueryRuntimeFailure
        .InvalidReceiver(
          method = method,
          reason =
            "streaming is not supported for sets created within an `at()` expression.",
          stackTrace = ctx.stackTrace)
        .toQuery
    } else if (reversed) {
      QueryRuntimeFailure
        .InvalidReceiver(
          method = method,
          reason = "streaming is not supported on sets that have been reversed.",
          stackTrace = ctx.stackTrace)
        .toQuery
    } else if (range != IndexSet.Range.Unbounded) {
      QueryRuntimeFailure
        .InvalidReceiver(
          method = method,
          reason =
            "range queries are not supported yet, use `.where()` method instead.",
          stackTrace = ctx.stackTrace)
        .toQuery
    } else if (!isValidConfig) {
      QueryRuntimeFailure
        .InvalidReceiver(
          method = method,
          reason = "only collections and user defined indexes are streamable.",
          stackTrace = ctx.stackTrace)
        .toQuery
    } else {
      val config0 =
        config.id match {
          case NativeIndexID(NativeIndexID.DocumentsByCollection) =>
            // NB. Replace the documents index with the changes index so that the
            // stream captures changes to all documents in the source collection.
            NativeIndex.ChangesByCollection(ctx.scopeID)
          case _ =>
            config
        }

      // FIXME: The `.all()` function in FQL does not check for read permission. We
      // should move this check there so the behaviour is consistent across query
      // and stream implementations.
      ctx.auth.checkReadCollectionPermission(ctx.scopeID, parent.collID) map {
        case true =>
          Result.Ok(
            Value.EventSource(
              this.copy(config = config0),
              ctx.systemValidTime map {
                Value.EventSource.Cursor(_)
              }
            ))

        case false =>
          QueryRuntimeFailure
            .PermissionDenied(
              ctx.stackTrace,
              s"Insufficient privileges to read from collection ${parent.collName}.")
            .toResult
      }
    }
}

/** Represents a set of a single element. */
final case class SingletonSet(value: Value) extends ValueSet {

  override def initialPageSize: Int = ValueSet.DefaultPageSize

  def reify(ctx: ValueReification.ReifyCtx): Expr =
    Expr.MethodChain(
      Expr.Id("Set", Span.Null),
      Seq(
        Expr.MethodChain
          .MethodCall(
            Span.Null,
            Name("single", Span.Null),
            Seq(ctx.save(value)),
            selectOptional = false,
            applyOptional = None,
            Span.Null
          )),
      Span.Null
    )

  protected[fql2] def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor
  ): Query[Result[ValueSet.Page]] = {
    val page =
      cursor match {
        case ValueSet.InitialPage =>
          def tok = ValueSet.PageTok(this, IndexedSeq.empty)
          val element = ValueSet.PageElem(value, () => tok)
          ValueSet.Page(Iterable.single(element), after = None)

        case ValueSet.Continuation(_) =>
          ValueSet.Page(Iterable.empty, after = None)
      }

    Result.Ok(page).toQuery
  }

  private def isAtExpression =
    value match {
      case doc: Value.Doc => doc.readTS.nonEmpty
      case _              => false
    }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    value match {
      case _: Value.Doc =>
        if (isAtExpression) {
          QueryRuntimeFailure
            .InvalidReceiver(
              method = method,
              reason =
                "streaming is not supported for sets created within an `at()` expression.",
              stackTrace = ctx.stackTrace)
            .toQuery
        } else {
          ctx.auth.checkReadPermission(ctx.scopeID, value.as[DocID]) flatMap {
            case true =>
              val cursor = ctx.systemValidTime map { Value.EventSource.Cursor(_) }
              Result.Ok(Value.EventSource(this, cursor)).toQuery
            case false =>
              QueryRuntimeFailure
                .PermissionDenied(
                  ctx.stackTrace,
                  "Insufficient privileges to create a stream on the source document.")
                .toQuery
          }
        }
      case _ =>
        QueryRuntimeFailure
          .InvalidReceiver(
            method = method,
            reason =
              "streaming on `Set.single()` is only supported if its element is a document.",
            stackTrace = ctx.stackTrace)
          .toQuery
    }
}

/** Takes `limit` elements from `inner`. `limit` must be positive.
  */
final case class TakeSet(inner: ValueSet, limit: Int) extends ValueSet {

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(inner),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("take", Span.Null),
          Seq(ctx.save(Value.Int(limit))),
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {

    val needed = count min limit

    if (needed == 0) {
      Result.Ok(ValueSet.Page.empty).toQuery
    } else {
      inner.paginateImpl(ctx, needed, cursor) mapT { page =>
        // NB. `inner` may return more than requested.
        val elems0 = page.elems.view
          .zip(0.until(limit).reverse)
          .map { case (e, nl) => e.mapSet(TakeSet(_, nl)) }

        val nextLimit = (limit - page.elems.size) max 0

        if (nextLimit > 0) {
          page.copy(
            elems = elems0.to(Iterable),
            after = page.after.map(_.mapSet(TakeSet(_, nextLimit))))
        } else {
          page.copy(elems = elems0.take(needed).to(Iterable), after = None)
        }
      }
    }
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on sets returned from `.take()`.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

/** constructor is private because the way to get a reverse set should be the reverse method on a set.
  * ex: inner.reverse
  */
final case class ReverseSet private (inner: ValueSet) extends ValueSet {

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(inner),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("reverse", Span.Null),
          Seq(),
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def reverse = inner

  // FIXME: This materializing implementation can disappear once range
  // support lands, and drop() and take() can compose properly with reverse().
  def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    inner.materialize(ctx) mapT { elems =>
      val elems0 = elems.reverse.zipWithIndex.map { case (e, i) =>
        e.withTok(() => ValueSet.PageTok(this, Vector(Value.Int(i))))
      }
      val elems1 = cursor match {
        case ValueSet.InitialPage => elems0
        case ValueSet.Continuation(Seq(tok)) =>
          elems0.dropWhile { e =>
            // This conversion is safe because we recreate the element tokens
            // above as part of `elems0`
            val ord = e.tok().cur.asInstanceOf[ValueSet.Continuation].token.head
            ord <= tok
          }
        case _ => throw new IllegalStateException(s"invalid cursor $cursor")
      }

      val (page, after) = {
        val (page, a) = elems1.splitAt(count)
        val after = Option.when(a.nonEmpty) { page.lastOption.map(_.tok()) }.flatten
        (page, after)
      }
      ValueSet.Page(page, after)
    }
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on sets that have been reversed.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

final case class ArraySet(array: Value.Array) extends ValueSet {

  override def initialPageSize: Int = ValueSet.DefaultPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(array),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("toSet", Span.Null),
          Seq.empty,
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def reverse = ArraySet(Value.Array(array.elems.reverse))

  private def mkToken(idx: Int) = ValueSet.PageTok(this, Vector(Value.Int(idx)))

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    val (page, after, offset) = cursor match {
      case ValueSet.InitialPage =>
        val after = array.elems.lift(count) map { _ =>
          mkToken(count)
        }

        (array.elems.take(count), after, 0)

      case ValueSet.Continuation(tok) =>
        val idx = tok match {
          case Seq(Value.Int(idx)) => idx
          case _ => throw new IllegalStateException(s"Unexpected values: $tok")
        }

        val after = array.elems.lift(idx + count) map { _ =>
          mkToken(idx + count)
        }

        (array.elems.slice(idx, idx + count), after, idx)
    }

    val elems = page.zipWithIndex map { case (elem, i) =>
      ValueSet.PageElem(elem, () => mkToken(i + offset))
    }

    Result.Ok(ValueSet.Page(elems, after)).toQuery
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on array sets.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

/** Is a sequence of numbers from `from` (inclusive) to `until` (exclusive).
  */
final case class SequenceSet(from: Int, until: Int, reversed: Boolean = false)
    extends ValueSet {

  override def initialPageSize: Int = ValueSet.DefaultPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    val chain = Seq.newBuilder[Expr.MethodChain.Component]
    chain +=
      Expr.MethodChain.MethodCall(
        Span.Null,
        Name("sequence", Span.Null),
        Seq(
          Expr.Lit(Literal.Int(from), Span.Null),
          Expr.Lit(Literal.Int(until), Span.Null)),
        selectOptional = false,
        applyOptional = None,
        Span.Null
      )
    if (reversed) {
      chain +=
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("reverse", Span.Null),
          Seq.empty,
          selectOptional = false,
          applyOptional = None,
          Span.Null)
    }
    Expr.MethodChain(
      Expr.Id("Set", Span.Null),
      chain.result(),
      Span.Null
    )
  }

  override def reverse = copy(reversed = !reversed)

  private def mkToken(idx: Int) = ValueSet.PageTok(this, Vector(Value.Int(idx)))

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    val range = from.until(until)
    val ordered = if (reversed) range.reverse else range

    val dropped =
      cursor match {
        case ValueSet.InitialPage                            => ordered
        case ValueSet.Continuation(IndexedSeq(Value.Int(i))) => ordered.drop(i)
        case other => throw new IllegalStateException(s"Unexpected cursor: $other")
      }

    val taken = dropped.take(count)
    val offset = ordered.size - dropped.size

    val after =
      Option.when(dropped.sizeIs > count)(mkToken(offset + taken.size))

    val elems =
      taken.view.zipWithIndex.map { case (n, i) =>
        ValueSet.PageElem(Value.Int(n), () => mkToken(offset + i))
      }

    val page = ValueSet.Page(elems, after)
    Result.Ok(page).toQuery
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on sets created from `Set.sequence()`.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

final case class ConcatSet(first: ValueSet, last: ValueSet) extends ValueSet {

  override def initialPageSize: Int = ValueSet.DefaultPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(first),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("concat", Span.Null),
          Seq(ctx.save(last)),
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def reverse = copy(first = last.reverse, last = first.reverse)

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] =
    first.paginateImpl(ctx, count, cursor) flatMapT { page =>
      if (page.after.isDefined) {
        // First set has not been drained yet
        Result.Ok(page.mapSet(ConcatSet(_, last))).toQuery

      } else {
        // First set has been drained, degrade to the second.

        // NB. `page.elems` may return more than `count`.
        val size = count - page.elems.size

        if (size > 0) {
          val firstElems = page.elems.map(_.mapSet(ConcatSet(_, last)))
          last.paginateImpl(ctx, size, ValueSet.InitialPage) mapT { cont =>
            cont.copy(elems = firstElems ++ cont.elems)
          }
        } else {
          val nextPage = page
            .mapSet(ConcatSet(_, last))
            .copy(after = Some(ValueSet.PageTok(last)))
          Result.Ok(nextPage).toQuery
        }
      }
    }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on concatenated sets.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

object WhereFilterSet {
  private[runtime] def evalPredicate[A](
    ctx: FQLInterpCtx,
    pred: Value.Func,
    value: Value,
    returnValue: A,
    stackTrace: FQLInterpreter.StackTrace): Query[Result[Option[A]]] =
    ctx.evalApply(pred, ArraySeq(value)) flatMapT {
      case Value.True                  => Result.Ok(Some(returnValue)).toQuery
      case Value.False | Value.Null(_) => Result.Ok(None).toQuery
      case v                           =>
        // FIXME: this error is not super helpful as if we were
        // able to point to the return expression of `predicate` and
        // point out it's not a boolean. Type checking will do this,
        // but it would be reaaally nice to do it at runtime, too.
        Result
          .Err(
            QueryRuntimeFailure
              .InvalidType(ValueType.BooleanType, v, stackTrace))
          .toQuery
    }

}

// FIXME: `span` will point to the `filter()` call site. how does the span
// here get set when decoding an opaque cursor?
final case class WhereFilterSet(
  inner: ValueSet,
  predicate: Value.Func,
  validTime: Option[Timestamp],
  stackTrace: FQLInterpreter.StackTrace)
    extends ValueSet {
  override def equals(other: Any): Boolean = other match {
    case that: WhereFilterSet =>
      inner == that.inner &&
      predicate == that.predicate &&
      validTime == that.validTime
    case _ => false
  }

  override def hashCode(): Int = {
    MurmurHash3.productHash((inner, predicate, validTime))
  }

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(inner),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("$where", Span.Null),
          Seq(ctx.save(predicate), ctx.save(ValueSet.timeToValue(validTime))),
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def reverse = copy(inner = inner.reverse)

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {

    def filter(
      set: ValueSet,
      cursor: ValueSet.Cursor,
      acc: Iterable[ValueSet.PageElem],
      pageSize: Int): Query[Result[ValueSet.Page]] =
      set.paginateImpl(ctx, pageSize, cursor) flatMapT { page =>
        if (page.elems.isEmpty && page.after.isEmpty) {
          // Inner set is drained.
          Result.Ok(ValueSet.Page.empty).toQuery
        } else {
          val elemsQ = page.elems map { e =>
            ctx.withUserValidTime(validTime).flatMap { ctx =>
              e.evalPartials { value =>
                WhereFilterSet.evalPredicate(
                  ValueSet.disallowWrites(ctx).prependStackTrace(stackTrace),
                  predicate,
                  value,
                  e,
                  stackTrace
                )
              }
            }
          }

          elemsQ.sequenceT flatMapT { elems =>
            val es = acc ++ elems.flatten

            if (es.sizeIs < count && page.after.isDefined) {
              // Continue accumulating; the paginate() contract stipulates either a
              // full page or no after cursor. Moreover, increase page size to speed
              // up the search since current iterations were not sufficient to fill
              // the requested page.
              val nextPageSize =
                (pageSize * ValueSet.ScaleFactor) min ValueSet.MaximumElements
              val tok = page.after.get
              filter(tok.set, tok.cur, es, nextPageSize)
            } else {
              val (elems, after) = if (es.sizeIs > count) {
                (es.take(count), Some(es.drop(count).head.tok()))
              } else {
                (es, page.after)
              }
              val p = ValueSet.Page(elems, after)
              val p0 = p.mapSet(WhereFilterSet(_, predicate, validTime, stackTrace))
              Result.Ok(p0).toQuery
            }
          }
        }
      }

    filter(inner, cursor, Nil, count max ValueSet.MinimumElementsForFiltering)
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    if (validTime.isDefined) {
      QueryRuntimeFailure
        .InvalidReceiver(
          method = method,
          reason =
            "streaming is not supported for sets created within an `at()` expression.",
          stackTrace = ctx.stackTrace)
        .toQuery
    } else {
      inner.toEventSourceImpl(ctx, method) mapT { stream =>
        stream.copy(set = this.copy(inner = stream.set))
      }
    }
}

// The terms 'spline' and 'leaf' are used to avoid confusion with other sets.
//
// Because a FlatMapSet needs to paginate the spline set, it stores the token
// for the next spline element internally to create the next leaf set.
//
// $flatMap serializes everything like this:
//
// <spline>.$flatMap(<fn>, <validTime>, <reversed>, <spline continuation cursor>)
final case class FlatMapSet(
  spline: ValueSet,
  splineCur: ValueSet.Cursor,
  fn: Value.Func,
  validTime: Option[Timestamp],
  reversed: Boolean,
  stackTrace: FQLInterpreter.StackTrace
) extends ValueSet {
  override def equals(other: Any): Boolean = other match {
    case that: FlatMapSet =>
      spline == that.spline &&
      splineCur == that.splineCur &&
      fn == that.fn &&
      validTime == that.validTime &&
      reversed == that.reversed
    case _ => false
  }

  override def hashCode(): Int = {
    MurmurHash3.productHash((spline, splineCur, fn, validTime, reversed))
  }

  override def initialPageSize: Int = spline.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    val args = Seq(
      ctx.save(fn),
      ctx.save(ValueSet.timeToValue(validTime)),
      ctx.save(Value.Boolean(reversed)),
      splineCur match {
        case ValueSet.Continuation(ords) => ctx.save(Value.Array(ords.to(ArraySeq)))
        case ValueSet.InitialPage        => ctx.save(Value.Null(Span.Null))
      }
    )

    Expr.MethodChain(
      ctx.save(spline),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("$flatMap", Span.Null),
          args,
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )),
      Span.Null
    )
  }

  override def reverse = copy(spline = spline.reverse, reversed = true)

  def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    assert(cursor == ValueSet.InitialPage)

    spline.paginateImpl(ctx, 1, splineCur).flatMapT { page =>
      if (page.elems.isEmpty) {
        // spline is exhausted, be done!
        Result.Ok(ValueSet.Page.empty).toQuery
      } else {
        ctx.withUserValidTime(validTime).flatMap { ctx =>
          page.elems.head.evalPartials { value =>
            val c = ValueSet.disallowWrites(ctx)
            c.prependStackTrace(stackTrace).evalApply(fn, ArraySeq(value)).flatMapT {
              case v: ValueSet =>
                val leaf = if (reversed) v.reverse else v
                val set = page.after match {
                  // spline has no pages left, so degrade to leaf
                  case None => leaf
                  // otherwise concat the leaf + continuation together
                  case Some(tok) =>
                    ConcatSet(
                      leaf,
                      FlatMapSet(
                        tok.set,
                        tok.cur,
                        fn,
                        validTime,
                        reversed,
                        stackTrace))
                }
                set.paginateImpl(ctx, count, ValueSet.InitialPage)

              case v =>
                Result
                  .Err(QueryRuntimeFailure
                    .InvalidType(ValueType.AnySetType, v, stackTrace))
                  .toQuery
            }
          }
        }
      }
    }
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on `.flatMap()` sets.",
        stackTrace = ctx.stackTrace)
      .toQuery
}

final case class ProjectedSet(
  inner: ValueSet,
  fn: Value.Func,
  validTime: Option[Timestamp],
  stackTrace: FQLInterpreter.StackTrace)
    extends ValueSet {
  override def equals(other: Any): Boolean = other match {
    case that: ProjectedSet =>
      inner == that.inner &&
      fn == that.fn &&
      validTime == that.validTime
    case _ => false
  }

  override def hashCode(): Int = {
    MurmurHash3.productHash((inner, fn, validTime))
  }

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    Expr.MethodChain(
      ctx.save(inner),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("$map", Span.Null),
          Seq(ctx.save(fn), ctx.save(ValueSet.timeToValue(validTime))),
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def reverse = copy(inner = inner.reverse)

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] =
    inner.paginateImpl(ctx, count, cursor) flatMapT { page =>
      val mapQ = page.elems map { e =>
        ctx.withUserValidTime(validTime).flatMap { ctx =>
          e.evalPartials { value =>
            val c = ValueSet.disallowWrites(ctx)
            c.prependStackTrace(stackTrace).evalApply(fn, ArraySeq(value)) mapT {
              v0 =>
                e.withValue(v0)
            }
          }
        }
      }

      mapQ.sequenceT mapT { elems =>
        page.copy(elems = elems).mapSet(ProjectedSet(_, fn, validTime, stackTrace))
      }
    }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    if (validTime.isDefined) {
      QueryRuntimeFailure
        .InvalidReceiver(
          method = method,
          reason =
            "streaming is not supported for sets created within an `at()` expression.",
          stackTrace = ctx.stackTrace)
        .toQuery
    } else {
      inner.toEventSourceImpl(ctx, method) mapT { stream =>
        stream.copy(set = this.copy(inner = stream.set))
      }
    }
}

object OrderedSet {
  final case class Ordering(accessor: Value.Func, ascending: Boolean)
  def Asc(accessor: Value.Func) = Ordering(accessor, true)
  def Desc(accessor: Value.Func) = Ordering(accessor, false)

  case class SortElem(v: Value, ascending: Boolean) {
    def compare(o: SortElem) = {
      val (a, b) = if (ascending) (v, o.v) else (o.v, v)
      a.compare(b)
    }
  }
  object SortElem {
    implicit val ord = new scala.math.Ordering[Seq[SortElem]] {
      def compare(a: Seq[SortElem], b: Seq[SortElem]): Int = {
        val iterA = a.iterator
        val iterB = b.iterator

        while (iterA.hasNext && iterB.hasNext) {
          val cmp = iterA.next() compare iterB.next()
          if (cmp != 0) return cmp
        }

        if (iterA.hasNext) 1 else if (iterB.hasNext) -1 else 0
      }
    }
  }

  def sortElems(
    ctx: FQLInterpCtx,
    orderings: Seq[Ordering],
    a: Value
  ): Query[Result[Seq[SortElem]]] =
    if (orderings.isEmpty) {
      Result.Ok(Seq(SortElem(a, true))).toQuery
    } else {
      orderings.map { ord =>
        val c = ValueSet.disallowWrites(ctx)
        c.evalApply(ord.accessor, ArraySeq(a)).mapT(SortElem(_, ord.ascending))
      }.sequenceT
    }
}

final case class OrderedSet(
  inner: ValueSet,
  ords: Seq[OrderedSet.Ordering],
  stackTrace: FQLInterpreter.StackTrace)
    extends ValueSet {
  override def equals(other: Any): Boolean = other match {
    case that: OrderedSet =>
      inner == that.inner &&
      ords == that.ords
    case _ => false
  }

  override def hashCode(): Int = {
    MurmurHash3.productHash((inner, ords))
  }

  override def initialPageSize: Int = inner.initialPageSize

  def reify(ctx: ValueReification.ReifyCtx) = {
    val args = ords.map { ord =>
      val acc = ctx.save(ord.accessor)
      if (ord.ascending) {
        acc
      } else {
        Expr.Object(Seq(Name("desc", Span.Null) -> acc), Span.Null)
      }
    }

    Expr.MethodChain(
      inner.reify(ctx),
      Seq(
        Expr.MethodChain.MethodCall(
          Span.Null,
          Name("order", Span.Null),
          args,
          selectOptional = false,
          applyOptional = None,
          Span.Null
        )
      ),
      Span.Null
    )
  }

  override def reverse =
    copy(ords = ords map { case OrderedSet.Ordering(a, asc) =>
      OrderedSet.Ordering(a, !asc)
    })

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    require(
      cursor == ValueSet.InitialPage,
      s"redundant materialization during a continuation. (cursor=$cursor)")

    // todo (chase): I think I want to use my stack trace in the ctx here
    inner.materialize(ctx) flatMapT { elems =>
      elems.zipWithIndex
        .map { case (e, i) =>
          e.evalPartials { v =>
            OrderedSet.sortElems(ctx.prependStackTrace(stackTrace), ords, v).mapT {
              is =>
                (v, is :+ OrderedSet.SortElem(Value.Int(i), true))
            }
          }
        }
        .sequenceT
        .flatMapT { elems =>
          val sorted = elems.sortBy(_._2).map(_._1).to(ArraySeq)
          ArraySet(Value.Array(sorted)).paginateImpl(ctx, count, cursor)
        }
    }
  }

  protected[fql2] def toEventSourceImpl(
    ctx: FQLInterpCtx,
    method: String): Query[Result[Value.EventSource]] =
    QueryRuntimeFailure
      .InvalidReceiver(
        method = method,
        reason = "streaming is not supported on sets returned from `.order()`.",
        stackTrace = ctx.stackTrace
      )
      .toQuery
}

final case class PagedSet(
  inner: ValueSet,
  count: Int,
  stackTrace: FQLInterpreter.StackTrace)
    extends ValueSet {

  override def initialPageSize: Int = count

  override def reverse = PagedSet(inner.reverse, count, stackTrace)

  override def reify(ctx: ValueReification.ReifyCtx): Expr = inner.reify(ctx)

  override def paginateImpl(
    ctx: FQLInterpCtx,
    count: Int,
    cursor: ValueSet.Cursor): Query[Result[ValueSet.Page]] = {
    inner.paginateImpl(ctx, count, cursor)
  }

  override protected[fql2] def toEventSourceImpl(ctx: FQLInterpCtx, method: String) =
    inner.toEventSourceImpl(ctx, method) mapT { stream =>
      stream.copy(set = this.copy(inner = stream.set))
    }
}
