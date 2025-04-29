package fauna.model.runtime.stream

import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpCtx, IndexSet }
import fauna.model.RuntimeEnv
import fauna.repo.doc.Version
import fauna.repo.query.{ Query, ReadCache }
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.{ Add, Create, Delete, DocAction, Remove, Update }
import fauna.storage.index.{ IndexTerm, IndexValue }
import scala.collection.mutable.{ SeqMap => MSeqMap }

/** Stream values are extracted from transaction results on live streams, or from
  * doc/set history when replaying events.
  *
  * Most decisions in v10 streaming are based on the pairwise comparison between
  * current and previous state of the observed document. It's imperative that
  * `evalCurr` and/or `evalPrev` are called so to reset the interpreter context to
  * the correct read time as well as feed detected partials to the read cache before
  * evaluating a query on the desired state.
  */
trait StreamValue {

  /** This document ID that this stream value represents. */
  def docID: DocID

  /** The resulting event's cursor. */
  def cursor: Value.EventSource.Cursor

  /** The derived document action (Create, Update, or Delete) that produced this
    * value based on the observed historical entry or transaction result.
    */
  def action: DocAction

  /** Returns true if this value represents a deleted doc in its CURRENT state. */
  @inline final def isDelete = action.isDelete

  /** Resets the given context to the correct read time in order to evaluate `fn` on
    * the CURRENT state of the observed document.
    */
  def evalCurr[A](ctx: FQLInterpCtx)(
    fn: (FQLInterpCtx, Value.Doc) => Query[A]): Query[A]

  /** Resets the given context to the correct read time in order to evaluate `fn` on
    * the PREVIOUS state of the observed document.
    */
  def evalPrev[A](ctx: FQLInterpCtx)(
    fn: (FQLInterpCtx, Value.Doc) => Query[A]): Query[A]
}

object StreamValue {

  /** An unresolved value is produced when the correct stream value can't be
    * determined by looking at a txn result or set history. In those cases, an
    * additional read is required to produce the correct stream value.
    *
    * Since resolving to a stream value may require reads, `resolve` must be called
    * from read aware code paths so that its bytes are accounted for the event's read
    * ops.
    */
  trait Unresolved {
    def docID: DocID
    def cursor: Value.EventSource.Cursor
    def resolve(): Query[Option[StreamValue]]
  }

  /** An unresolved document used as a fallback when set history or txn results can't
    * be used to determine the correct stream value to process.
    */
  final case class UnresolvedDoc(
    scopeID: ScopeID,
    docID: DocID,
    cursor: Value.EventSource.Cursor)
      extends Unresolved {
    def resolve() =
      Query.readAndIncrStats(Query.incrDocuments(_)) {
        RuntimeEnv.Default.Store(scopeID).getVersion(docID, cursor.ts) mapT {
          DocVersion(_, cursor)
        }
      }
  }

  /** An observed fully resolved document version. Usually derived from single
    * document streams.
    */
  final case class DocVersion(version: Version, cursor: Value.EventSource.Cursor)
      extends StreamValue
      with Unresolved {

    def docID = version.id
    def resolve() = Query.some(this)

    // NB. `Update` vanishes during CBOR encoding. Thus, updates are detected based
    // on the presence of diffs.
    def action =
      (version.action, version.diff) match {
        case (Delete, _)  => Delete
        case (_, Some(_)) => Update
        case (_, None)    => Create
      }

    // NB. Do not feed partials. Rely on version override to prevent reads.
    def evalCurr[A](ctx: FQLInterpCtx)(
      fn: (FQLInterpCtx, Value.Doc) => Query[A]): Query[A] =
      ctx.atSystemValidTime(cursor.ts) flatMap {
        fn(_, Value.Doc(version.id, versionOverride = Some(version)))
      }

    // NB. Do not feed partials. Rely on version override to prevent reads.
    def evalPrev[A](ctx: FQLInterpCtx)(
      fn: (FQLInterpCtx, Value.Doc) => Query[A]): Query[A] =
      ctx.atSystemValidTime(cursor.ts.prevNano) flatMap {
        fn(_, Value.Doc(version.id, versionOverride = version.prevVersion))
      }
  }

  /** An observed set row. Usually derived from set streams such as
    * `<Collection>.all().toStream()` or streams on user-defined indexes.
    *
    * Deriving the document operation that triggered the observed set event relies on
    * the following assumptions (MVAs are addressed below):
    *
    * 1. Set writes are mergeable, therefore no double ADDs or REMOVEs should occur;
    * 2. A single ADD represents a document being moved INTO the set;
    * 3. A single REMOVE represents a document being moved OUT of the set;
    * 4. A pair of REMOVE and ADD for the same `docID` represents a document being
    * updated but REMAINING in the set.
    *
    * MVAs break assumption 1. If the row contains a MVA value, an `UnresolvedDoc`
    * should be used instead.
    */
  final case class IndexRow(
    terms: Vector[IndexTerm],
    value: IndexValue,
    cursor: Value.EventSource.Cursor,
    coveredValues: Seq[IndexSet.CoveredValue],
    prevRow: Option[IndexRow] = None)
      extends StreamValue
      with Unresolved {

    require(
      prevRow forall { prev => !isDelete && prev.isDelete },
      s"incorrect index row detected: $this"
    )

    def docID = value.docID
    def resolve() = Query.some(this)

    def action =
      (value.action, prevRow) match {
        case (Add, None)    => Create
        case (Add, Some(_)) => Update
        case (Remove, _)    => Delete
      }

    def withPrev(prev: IndexRow): IndexRow =
      copy(prevRow = Some(prev))

    def evalCurr[A](ctx: FQLInterpCtx)(
      fn: (FQLInterpCtx, Value.Doc) => Query[A]): Query[A] =
      withRowPartialsAt(cursor.ts, Some(this)) {
        ctx.atSystemValidTime(cursor.ts) flatMap {
          fn(_, Value.Doc(docID))
        }
      }

    def evalPrev[A](ctx: FQLInterpCtx)(
      fn: (FQLInterpCtx, Value.Doc) => Query[A]): Query[A] = {
      val readTS = cursor.ts.prevNano
      // NB. If the observed value is a REMOVE, the observed index value contains the
      // partials at the previous state of the document. Otherwise, rely on the
      // previous row detected, if any.
      val row = Option.when(isDelete)(this) orElse prevRow

      withRowPartialsAt(readTS, row) {
        ctx.atSystemValidTime(readTS) flatMap {
          fn(_, Value.Doc(docID))
        }
      }
    }

    private def withRowPartialsAt[A](readTS: Timestamp, row: Option[IndexRow])(
      fn: => Query[A]): Query[A] =
      row match {
        case None => fn
        case Some(row) =>
          val partials =
            IndexSet.extractPartials(
              coveredValues,
              row.terms,
              row.value.tuple.values
            )

          val feedQ =
            Store.feedPartial(
              srcHint = ReadCache.CachedDoc.StreamSrcHint(row.value),
              prefix = ReadCache.Prefix.empty,
              scopeID = row.value.scopeID,
              docID = row.value.docID,
              validTS = Some(readTS),
              partials = partials
            )

          feedQ flatMap { cachedDoc =>
            fn ensure {
              cachedDoc.unpin()
              Query.unit
            }
          }
      }
  }

  /** Used by the live and replay event streaming paths to reduce the index rows into the actual stream/feed event
    * values for a given ts.
    * It is REQUIRED that all index values provided share the same ts, this method will throw if this isn't the case.
    */
  def reduceIndexRows(
    terms: Vector[IndexTerm],
    coveredValues: Seq[IndexSet.CoveredValue],
    indexRows: Iterable[IndexValue]): Seq[StreamValue.Unresolved] = {
    val ts = indexRows.headOption.map(_.ts.validTS)
    var cursor = Value.EventSource.Cursor.MinValue
    indexRows
      .foldLeft(MSeqMap.empty[DocID, StreamValue.Unresolved]) { case (docMap, iv) =>
        if (!ts.contains(iv.ts.validTS)) {
          throw new IllegalStateException(
            s"All index values must share the same ts to be reduced. Different ts values found $ts ${iv.ts.validTS}, IndexValue=$iv")
        }
        cursor = cursor.next(iv.ts.validTS)
        val streamVal =
          StreamValue.IndexRow(
            terms,
            iv,
            cursor,
            coveredValues
          )
        val combinedStreamVal = docMap
          .get(iv.docID)
          .map { prev =>
            (prev, streamVal) match {
              case (prev: IndexRow, streamVal: IndexRow) =>
                (prev.action, streamVal.action) match {
                  case (Create | Update, Delete) => prev.withPrev(streamVal)
                  case (Delete, Create | Update) =>
                    // maintain the cursor order we see each doc id in
                    streamVal.copy(prevRow = Some(prev), cursor = prev.cursor)
                  /** If we see a double remove, we fallback to an unresolved doc. This scenario occurs when there is
                    * an update and delete of the same document within a transaction. In this scenario, it is possible
                    * that the partial cache was filled with the incorrect remove event. In order to ensure that we
                    * return the correct data, we use the unresolved doc to force a read in this specific case.
                    */
                  case (Delete, Delete) =>
                    StreamValue.UnresolvedDoc(
                      prev.value.scopeID,
                      prev.docID,
                      prev.cursor)
                  case _ => streamVal
                }
              case (prev: UnresolvedDoc, _) => prev
              case (prev, streamVal) =>
                throw new IllegalStateException(
                  s"Unexpected type combination while reducing acc: $prev next: $streamVal")

            }
          }
          .getOrElse(streamVal)
        docMap.update(iv.docID, combinedStreamVal)
        docMap
      }
      .values
      .toSeq
  }
}
