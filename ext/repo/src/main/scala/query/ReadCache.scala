package fauna.repo.query

import fauna.atoms._
import fauna.exec.ImmediateExecutionContext
import fauna.lang._
import fauna.repo.doc.{ Version => DocVersion }
import fauna.repo.schema.migration.MigrationList
import fauna.repo.values.{ Value => RValue }
import fauna.stats.{ QueryMetrics, StatsRecorder }
import fauna.storage.api.version.{ DocSnapshot, StorageVersion }
import fauna.storage.api.Read
import fauna.storage.index.IndexValue
import fauna.storage.ops.Write
import scala.annotation.tailrec
import scala.collection.{ Factory, MapView }
import scala.collection.mutable.{ Builder, Map => MMap }
import scala.concurrent.{ Future, Promise }
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A per-query, document aware, read cache. The read cache is implemented using 2
  * separate LRUs: the IO cache which stores all reads sent to storage and their
  * results; the documents cache which stores only document snapshots read from
  * storage OR document partials discovered during query evaluation.
  *
  * The read cache API expects a prefix of writes on every call. The prefix is a
  * sequence of all writes to the same row key and it is part of the cached entry's
  * identity since different prefixes indicate cache staleness.
  *
  * Partials can be discovered by reading an index with covered values, for example.
  * If fed to the read cache via `.put(..)`, subsequent projections can locate them
  * via `.peek(..)` and skip a document read if only projecting on known partials. If
  * the source document is read, it's result is placed into the documents cache,
  * therefore overriding any existing partials and making subsequent projections
  * always succeed.
  *
  * Cached documents/partials are pinned to the document's cache so that they are
  * kept for the duration of the computation they were cached to support. This is a
  * best effort attempt to prevent concurrent branches in query evaluation to
  * invalidate partials that are needed withing an enclosing scope, like when caching
  * an index covered value before calling a mapping function on it, for example. Note
  * that it is the call-site responsibility to unpin cached documents/partials after
  * it's no longer needed.
  */
object ReadCache {

  type Field = String
  type Path = List[Field]
  object Path { @inline def empty: Path = List.empty }

  type Prefix = Vector[Write]
  object Prefix extends Factory[Write, Prefix] {
    @inline def empty: Prefix = Vector.empty
    def fromSpecific(it: IterableOnce[Write]): Prefix = Vector.from(it)
    def newBuilder: Builder[Write, Prefix] = Vector.newBuilder
  }

  type Partials = Map[Path, RValue]
  object Partials { @inline def empty: Partials = Map.empty }

  type View = MapView[(Prefix, Read[_]), Future[Read.Result]]

  private type IOCache = LRU[(Prefix, Read[_]), Promise[Read.Result]]
  private type DocsCache = LRU[(Prefix, ScopeID, DocID, Timestamp), CachedDoc]

  /** A simple pin count that gets shared upon cached partials promition. */
  private final class Pins(private var _count: Int) {
    def incr(): Unit = _count += 1
    def decr(): Unit = _count = (_count - 1).max(0)
    def count: Int = _count
  }

  /** A shared, pinneable, LRU cache entry. Cached documents can assume three forms: a
    * known version, a partial document, or an empty document in case it's not found
    * via a document snapshot read.
    */
  sealed abstract class CachedDoc(private val pins: Pins)
      extends LRU.Pinnable
      with AutoCloseable {

    import CachedDoc._

    def srcHints: SrcHints

    def pin(): CachedDoc = {
      pins.incr()
      this
    }

    def unpin(): CachedDoc = {
      pins.decr()
      this
    }

    def isPinned =
      pins.count > 0

    def close() =
      unpin()

    private[ReadCache] final def promote(
      version: Option[StorageVersion]): CachedDoc =
      version match {
        case Some(vs) => new CachedDoc.Version(vs, pins, srcHints = srcHints)
        case None     => new CachedDoc.DocNotFound(pins)
      }

    private[ReadCache] def merge(partials: Partials, srcHint: SrcHint): CachedDoc
  }

  object CachedDoc {

    final case class SrcHints(val hints: Seq[SrcHint]) extends AnyVal {
      def indexSrcHints: Seq[SetSrcHint] = hints.collect { case s: SetSrcHint =>
        s
      }
    }

    object SrcHints {
      val Empty = SrcHints(Nil)
    }

    sealed trait SrcHint

    final case class SetSrcHint(
      scopeID: ScopeID,
      indexID: IndexID,
      collectionName: String,
      collectionId: CollectionID,
      indexName: String,
      coveredFieldPaths: Seq[Path],
      /** The span where the set was created. */
      span: fql.ast.Span
    ) extends SrcHint

    final case class StreamSrcHint(indexRow: IndexValue) extends SrcHint

    /** Represents a missing document result from a document snapshot read. */
    final class DocNotFound private[CachedDoc] (pins: Pins) extends CachedDoc(pins) {
      private[ReadCache] def merge(partials: Partials, srcHint: SrcHint) = this

      override def srcHints: SrcHints = SrcHints.Empty

      override def toString = "DocNotFound"
    }

    /** Represents a fully materialized document obtained by a snapshot read. */
    final class Version private[CachedDoc] (
      private val version: StorageVersion,
      pins: Pins,
      override val srcHints: SrcHints = SrcHints.Empty
    ) extends CachedDoc(pins) {
      private[ReadCache] def merge(partials: Partials, srcHint: SrcHint) = this

      def decode(migrations: MigrationList) =
        DocVersion
          .fromStorage(version, migrations)
          .asInstanceOf[DocVersion.Live]

      override def toString = s"Version($version)"
    }

    /** Represents a partial document built from fragments discovered during query
      * evaluation.
      */
    final class Partial private[CachedDoc] (
      private val mSrcHints: MMap[AnyRef, SrcHint],
      private val fragment: Fragment.Struct,
      pins: Pins)
        extends CachedDoc(pins) {

      override private[ReadCache] def merge(partials: Partials, srcHint: SrcHint) = {
        fragment.merge(partials)

        /** For partials, we only want one src hint per index. */
        val dedupeKey: AnyRef = srcHint match {
          case s: SetSrcHint    => s.indexName
          case s: StreamSrcHint => s.indexRow.docID
        }

        if (!mSrcHints.contains(dedupeKey)) {
          mSrcHints += dedupeKey -> srcHint
        }

        this
      }

      def srcHints: SrcHints = SrcHints(mSrcHints.values.toSeq)

      /** Projects the given path out of the partial document. Note that a `None` return
        * means that the field is not known. It does not mean that the field is
        * missing.
        */
      def project(path: Path): Option[Fragment] =
        fragment.project(path)

      override def toString =
        s"Partial($fragment)"
    }

    private[ReadCache] def emptyPinnedPartial: CachedDoc =
      new Partial(MMap.empty, new Fragment.Struct(), pins = new Pins(1))

    private[ReadCache] def apply(
      version: Option[StorageVersion]
    ): CachedDoc =
      version match {
        case Some(vs) =>
          new Version(vs, pins = new Pins(0))
        case None => new DocNotFound(pins = new Pins(0))
      }
  }

  /** A tree like data-structure representing a known document fragment: leafs are
    * known values while branhes are each known field paths. Note that array indexes
    * are not supported as fragments (only as leafs).
    */
  sealed trait Fragment

  object Fragment {

    final class Value private[Fragment] (val unwrap: RValue) extends Fragment {
      override def toString = s"Value($unwrap)"
    }

    final class Struct private[ReadCache] (
      private val unsafeMap: MMap[Field, Fragment] = MMap.empty)
        extends Fragment {

      def project(path: Path) = {
        @tailrec
        def project0(path: Path, fragment: Fragment): Option[Fragment] =
          path match {
            case Nil => Some(fragment)
            case field :: rest =>
              fragment match {
                case _: Value => None
                case s: Struct =>
                  s.unsafeMap.get(field) match {
                    case Some(fragment0) => project0(rest, fragment0)
                    case None            => None
                  }
              }
          }
        project0(path, this)
      }

      def merge(partials: Partials): Struct = {
        @tailrec
        def merge0(struct: Struct, path: Path, value: RValue): Unit =
          path match {
            case Nil          => ()
            case field :: Nil => struct.unsafeMap += field -> new Value(value)
            case field :: rest =>
              struct.unsafeMap.getOrElseUpdate(field, new Struct()) match {
                case next: Struct => merge0(next, rest, value)
                case _            => ()
              }
          }
        partials foreachEntry { (path, value) =>
          merge0(this, path, value)
        }
        this
      }

      override def toString =
        s"Struct($unsafeMap)"
    }
  }

  private final class Stats {
    @volatile var hits = 0
    @volatile var misses = 0
    @volatile var bytesLoaded = 0L

    def report(rec: StatsRecorder): Unit = {
      rec.count(QueryMetrics.ReadCacheHits, hits)
      rec.count(QueryMetrics.ReadCacheMisses, misses)
      rec.count(QueryMetrics.ReadCacheBytesLoaded, bytesLoaded)
    }
  }
}

final class ReadCache(docsCacheSize: Int = 128, ioCacheSize: Int = 64) {
  import ReadCache._

  private lazy val docsCache: DocsCache = LRU.pinneable(docsCacheSize)
  private lazy val ioCache: IOCache = LRU.unpinneable(ioCacheSize)
  private lazy val stats: Stats = new Stats

  def get[A <: Read.Result](prefix: Prefix, read: Read[A]): Option[Try[A]] =
    ioCache.synchronized { ioCache.get((prefix, read)) } match {
      case null => None
      case p =>
        if (p.isCompleted) stats.synchronized { stats.hits += 1 }
        p.future.value.asInstanceOf[Option[Try[A]]]
    }

  /** Get the read's cached result, if any. Otherwise run the given `runIO` function to
    * compute and cache it.
    */
  def getOrLoad[A <: Read.Result](prefix: Prefix, read: Read[A])(
    runIO: => Future[A]): Future[A] = {
    var miss = false
    val promise =
      ioCache.synchronized {
        ioCache
          .computeIfAbsent(
            (prefix, read),
            _ => {
              miss = true
              Promise()
            })
          .asInstanceOf[Promise[A]]
      }

    if (miss) {
      val io = runIO
      implicit val ec = ImmediateExecutionContext
      io.foreach(maybeFillDocsCache(prefix, read, _))
      promise.completeWith(io)
    }

    implicit val ec = ImmediateExecutionContext
    promise.future map { result =>
      stats.synchronized {
        if (miss) {
          stats.misses += 1
          stats.bytesLoaded += result.bytesRead
        } else {
          stats.hits += 1
        }
      }
      result
    }
  }

  private[query] def maybeFillDocsCache[A](
    prefix: Prefix,
    read: Read[A],
    res: A
  ): Unit = {

    read match {
      case r: DocSnapshot =>
        docsCache.synchronized {
          docsCache.compute(
            (prefix, r.scopeID, r.docID, r.versionID.validTS),
            {
              case (_, null)  => CachedDoc(res.version)
              case (_, other) => other.promote(res.version)
            }
          )
        }
      case _ => ()
    }
  }

  /** Put the given document partials in the cache. Note that the returned cached
    * document is pinned and it is the caller's responsibility to unpin it.
    */
  def put(
    prefix: Prefix,
    scopeID: ScopeID,
    docID: DocID,
    validTS: Timestamp,
    partials: Partials,
    srcHint: CachedDoc.SrcHint): CachedDoc = {

    val cachedDoc =
      docsCache.synchronized {
        docsCache.compute(
          (prefix, scopeID, docID, validTS),
          {
            case (_, null)  => CachedDoc.emptyPinnedPartial
            case (_, other) => other.pin()
          })
      }

    cachedDoc.merge(partials, srcHint)
  }

  /** Lookup a cached document if any. Note that the pin/unpin live-cycle is put's
    * caller responsibility. Callers of `peek` are not expected to call `pin` or
    * `unpin`.
    */
  def peek(
    prefix: Prefix,
    scopeID: ScopeID,
    docID: DocID,
    validTS: Timestamp): Option[CachedDoc] = {

    docsCache.synchronized {
      Option(docsCache.get((prefix, scopeID, docID, validTS)))
    }
  }

  /** Safely inspect the state of the underlying cached reads and their results. */
  def inspect[A](fn: View => A): A =
    ioCache.synchronized {
      fn(ioCache.asScala.view.mapValues { _.future })
    }

  /** Report the per-query cache stats. */
  def reportStats(rec: StatsRecorder): Unit =
    stats.report(rec)
}
