package fauna

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.lang.{ Page, Timestamp }
import fauna.repo.doc.Version
import fauna.repo.schema.ConstraintFailure
import fauna.stats.QueryMetrics
import fauna.storage.{ Event, Tables }
import fauna.storage.index._
import fauna.storage.ir.IRValue
import fauna.storage.ops.Write
import io.netty.buffer.ByteBuf
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.duration._
import scala.util.{ Failure, Try }
import scala.util.control.NoStackTrace

package repo {

  private[repo] case class ByInstanceID(event: Event) extends Ordered[ByInstanceID] {
    def compare(other: ByInstanceID) = event.compareByDocID(other.event)
  }

  // Write failure enum

  sealed trait WriteFailure
  object WriteFailure {
    final case class DeleteConstraintViolation(scope: ScopeID, id: DocID, inbound: Iterable[DocID])
        extends WriteFailure
    final case class SchemaConstraintViolation(failures: Iterable[ConstraintFailure])
        extends WriteFailure
    final case class MaxIDExceeded(scope: ScopeID, coll: CollectionID) extends WriteFailure
    final case class InvalidID(scope: ScopeID, id: DocID) extends WriteFailure
    final case class CreateDocIDExists(scope: ScopeID, id: DocID) extends WriteFailure
    final case class DocNotFound(scope: ScopeID, id: DocID) extends WriteFailure
  }

  // Exception classes

  // Subclasses are not transient, retryable conditions.
  abstract class UnretryableException(msg: String) extends Exception(msg)

  case class UninitializedException(service: String)
      extends UnretryableException(s"$service is uninitialized")

  case class RangeArgumentException(msg: String)
      extends UnretryableException(msg)

  final case class WriteFailureException(failure: WriteFailure)
      extends UnretryableException(s"Document write failed: $failure") with NoStackTrace

  case class UniqueConstraintViolation(indexes: List[(ScopeID, IndexID, DocID, IndexRow)])
      extends UnretryableException(s"Instance is not unique in ${indexes}")

  sealed abstract class ContentionException extends Exception {
    def info: String
    def msg: String
    def newTS: Timestamp
    def source: ContentionException.Source
    override def getMessage = msg
  }

  object ContentionException {

    sealed trait Source
    object Source {
      final case class Schema(scope: ScopeID) extends Source
      final case class Document(scope: ScopeID, id: DocID) extends Source
      final case class Unknown(cf: String, rowKey: ByteBuf) extends Source
      final case class Multiple(sources: Seq[Source]) extends Source
      final case class Index(scope: ScopeID, id: IndexID, terms: Vector[IRValue])
          extends Source
    }

    def apply(cf: String, rk: ByteBuf, newTS: Timestamp) =
      cf match {
        case Tables.Versions.CFName =>
          val (s, d) = Tables.Versions.decodeRowKey(rk)
          DocContentionException(s, d, newTS)
        case Tables.SortedIndex.CFName | Tables.HistoricalIndex.CFName =>
          val (s, idx, terms) = Tables.Indexes.decode(rk)
          IndexContentionException(s, idx, terms, newTS)
        case Tables.SchemaVersions.CFName =>
          SchemaContentionException(
            Tables.SchemaVersions.decodeRowKey(rk),
            SchemaVersion(newTS))
        case cf => UnknownContentionException(cf, rk, newTS)
      }

    def aggregate(errors: List[ContentionException]): ContentionException = {
      require(errors.nonEmpty, "must provided a non-empty list of contention errors")
      val bySource = MMap.empty[Source, (ContentionException, Int)]

      errors foreach { ex =>
        bySource.updateWith(ex.source) {
          case None                                     => Some((ex, 1))
          case Some((ex0, cnt)) if ex0.newTS > ex.newTS => Some((ex0, cnt + 1))
          case Some((_, cnt))                           => Some((ex, cnt + 1))
        }
      }

      if (bySource.sizeIs > 1) {
        val byErrorCount = bySource.values.toMap
        AggregateContentionException(byErrorCount)
      } else {
        val (_, (err, _)) = bySource.head
        err
      }
    }
  }

  case class DocContentionException(scope: ScopeID, docID: DocID, newTS: Timestamp)
      extends ContentionException with NoStackTrace {
    def info = s"scope $scope document $docID"
    def msg = s"Concurrent write detected to $scope document $docID"
    def source = ContentionException.Source.Document(scope, docID)
  }

  case class IndexContentionException(
    scope: ScopeID,
    idx: IndexID,
    terms: Vector[IRValue],
    newTS: Timestamp)
      extends ContentionException with NoStackTrace {
    def info = s"scope $scope index $idx terms $terms"
    def msg = s"Concurrent write detected to $scope $idx $terms"
    def source = ContentionException.Source.Index(scope, idx, terms)
  }

  case class SchemaContentionException(scope: ScopeID, schemaVersion: SchemaVersion)
      extends ContentionException {
    def info = s"scope $scope schema"
    def msg = s"Concurrent schema update detected to $scope at schema version $schemaVersion"
    def source = ContentionException.Source.Schema(scope)
    def newTS = schemaVersion.ts
  }

  case class UnknownContentionException(cf: String, rk: ByteBuf, newTS: Timestamp)
      extends ContentionException with NoStackTrace {
    def info = s"CF $cf key ${CBOR.showBuffer(rk)}"
    def msg = s"Concurrent write detected to $cf. key: ${CBOR.showBuffer(rk)}"
    def source = ContentionException.Source.Unknown(cf, rk)
  }

  case class AggregateContentionException(exns: Map[ContentionException, Int])
      extends ContentionException with NoStackTrace{
    def newTS = exns.keys.foldLeft(Timestamp.Min) { (ts, e) => ts.max(e.newTS) }
    def info = exns.map { case (e, i) => s"${e.info} ($i)" }.mkString(", ")
    def msg = exns.map { case (e, i) => s"${e.msg} ($i times)" }.mkString(", ")
    def source = ContentionException.Source.Multiple(exns.keys.map(_.source).toSeq)
  }

  case class FutureReadException(snapshotTS: Timestamp)
      extends UnretryableException(s"Cannot read at $snapshotTS, it is too far in the future.")

  case class TransactionKeyTooLargeException(bytes: Int)
      extends UnretryableException(
    s"Key bytes ($bytes) exceeds threshold (${IndexKey.KeyBytesThreshold})")

  case class VersionTooLargeException(bytes: Int)
      extends UnretryableException(
    s"Version bytes ($bytes) exceeds threshold (${Write.VersionBytesThreshold})")

  case class TxnTooLargeException(size: Long, limit: Long, unit: String)
      extends UnretryableException(s"Transaction exceeded limit: got $size $unit, limit $limit $unit")

  case class TxnTooManyComputeOpsException(count: Int, limit: Int)
      extends UnretryableException(TxnTooManyComputeOpsException.tooManyOpsMsg(count, limit))

  object TxnTooManyComputeOpsException {
    private def tooManyOpsMsg(count: Int, limit: Int): String = {
      val baseline = QueryMetrics.BaselineCompute
      val nLimit = (limit / baseline).toInt
      val nCount = Math.ceil(count / baseline).toInt
      s"Transaction does too much compute: got $nCount, limit: $nLimit"
    }
  }

  final case class MaxQueryWidthExceeded(max: Int, width: Int)
    extends Exception(
      s"Query exceeds max width. Max: $max, current: $width.",
      null /* cause */,
      false /* enableSuppression */,
      false /* writableStackTrace */ )

  // An index task error that occurs when the index entries for a version are cumulatively too large.
  // This can occur either when the index exists and the version is created, or when the
  // version exists and the index is built. This exception will cause version creation or index building
  // to fail, respectively.
  case class VersionIndexEntriesTooLargeException(
    scope: ScopeID,
    id: DocID,
    idx: IndexID,
    size: BigInt,
    limit: Long)
      extends Exception(
        s"Index entries for doc $id in scope $scope and index $idx have "
          + s"estimated byte size $size that exceeds limit $limit")
      with NoStackTrace
}

package object repo {
  import fauna.repo.query.Query

  val Everything = 100000 // Very large, but not the largest.
  val IndexRecoveryGracePeriod = 2.hours

  // FIXME: correct sizes should be chosen via profiling.
  val DefaultPageSize = 64
  val BulkPageSize = 4096

  type PagedQuery[+A] = Query[Page[Query, A]]

  object PagedQuery {
    def empty[A]: PagedQuery[Iterable[A]] =
      Query.value(Page[Query, Iterable[A]](Nil))

    def apply[A](elems: Iterable[A]): PagedQuery[Iterable[A]] =
      Query.value(Page[Query, Iterable[A]](elems))

    def apply[A](
      elems: Iterable[A],
      next: PagedQuery[Iterable[A]]): PagedQuery[Iterable[A]] =
      Query.value(Page[Query, Iterable[A]](elems, next))
  }

  type TermBindings = Seq[(String, Vector[IRValue])]
  type TermExtractor = (Version, TermBindings) => Query[Vector[IndexTerm]]
  type TermBinder = (String, Version => Query[Vector[IRValue]])

  // Helpers

  private[repo] def assertValidRange[T](start: T, end: T, reverse: Boolean)(implicit ev: T => Ordered[T]): Try[Unit] =
    reverse match {
      case false if ev(start) < end => Failure(RangeArgumentException(s"$start is < $end for descending range"))
      case true if ev(start) > end => Failure(RangeArgumentException(s"$start is > $end for ascending range"))
      case _ => Try(())
    }
}
