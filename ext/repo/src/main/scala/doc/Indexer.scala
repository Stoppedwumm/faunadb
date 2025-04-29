package fauna.repo.doc

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.{ CartesianSeq, ConsoleControl, Timestamp }
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.index._
import fauna.storage.ir._
import scala.annotation.unused
import scala.collection.{ SeqView, View }
import scala.collection.immutable.ArraySeq
import scala.util.hashing.MurmurHash3

sealed abstract class Indexer {
  /**
    * Emits new index rows for a `version`.
    */
  def rows(version: Version): Query[Seq[IndexRow]]

  /** Check if any of indexer's tuples for the given version at its current state
    * matches the given terms.
    */
  def matches(version: Version, terms: Vector[IndexTerm]): Query[Boolean]

  final def append(other: Indexer) =
    (this, other) match {
      case (Indexer.Empty, rhs) => rhs
      case (lhs, Indexer.Empty) => lhs
      case (AggregateIndexer(lhs), AggregateIndexer(rhs)) =>
        AggregateIndexer(lhs ++ rhs)
      case (lhs: BasicIndexer, AggregateIndexer(rhs)) => AggregateIndexer(lhs +: rhs)
      case (AggregateIndexer(lhs), rhs: BasicIndexer) => AggregateIndexer(lhs :+ rhs)
      case (lhs: BasicIndexer, rhs: BasicIndexer) =>
        AggregateIndexer(lhs :: rhs :: Nil)
    }

  final def +(other: Indexer) = this append other
}

sealed abstract class BasicIndexer extends Indexer {
  def config: IndexConfig
  def binders: CollectionID => Option[List[TermBinder]] = config.binders
  def terms: Vector[(TermExtractor, Boolean)] = config.terms
  def values: Vector[(TermExtractor, Boolean)] = config.values
  def constraint: Constraint = config.constraint
  def partitions: Long = config.partitions

  // Computes a lower-bound estimate for the total serialized size of the index entries
  // from lower bound estimates for the sizes of the terms and values.
  private def entriesSerializedSizeLowerBound(ts: SeqView[Vector[IndexTerm]], vs: SeqView[Vector[IndexTerm]]): BigInt = {
    def combosSize(xs: View[Vector[BigInt]]) = {
      val counts = xs.map(_.size)
      val sizes = xs.map(_.sum)

      def multiplier(index: Int): BigInt = {
        counts.zipWithIndex.foldLeft(BigInt(1)) { case (acc, (count, index2)) =>
          acc * (if (index == index2) 1 else count)
        }
      }

      sizes.zipWithIndex.foldLeft(BigInt(0)) { case (acc, (size, index)) =>
        // Size per term position should be multiplied with the total number of
        // combinations possible from all other term positions.
        size * multiplier(index) + acc
      }
    }

    val tsSizes = ts map { _ map { t => BigInt(t.serializedSizeLowerBound()) } }
    val vsSizes = vs map { _ map { t => BigInt(t.serializedSizeLowerBound()) } }
    val totalSizeIncludingNulls = combosSize(tsSizes ++ vsSizes)

    val termsNullsCount = if (ts.isEmpty) 0 else ts map { _ count { _.value == NullV } } product
    val valuesNullsCount = if (vs.isEmpty) 0 else vs map { _ count { _.value == NullV } } product

    def lazyMul(a: BigInt, b: => BigInt) =
      if (a == 0) a else a * b

    val nullVSize = BigInt(IRValue.serializedSizeLowerBound(NullV))
    totalSizeIncludingNulls -
    // Subtract the size for all the nulls that would be filtered out.
    lazyMul(termsNullsCount, combosSize(vsSizes) + ts.size * nullVSize) -
    lazyMul(valuesNullsCount, combosSize(tsSizes) + vs.size * nullVSize) +
    // Don't filter the terms=null&value=null cases out twice.
    (valuesNullsCount * termsNullsCount * ((vs.size + ts.size) * nullVSize))
  }

  private def combos[T](list: Seq[IndexedSeq[T]], predicate: Seq[T] => Boolean): Seq[Seq[T]] =
    CartesianSeq(list.toIndexedSeq).filter(predicate)

  private def extractTerms(
    version: Version,
    bindings: TermBindings,
    extract: TermExtractor,
    reverse: Boolean): Query[Vector[IndexTerm]] =
    extract(version, bindings) map { terms =>
      if (terms.isEmpty) {
        Vector(IndexTerm(NullV, reverse))
      } else {
        // This seemingly-innocuous Set conversion reduces the cardinality of the cartesian product in combos().
        terms.distinct
      }
    }

  private def tuples(version: Version): Query[Set[Tuple]] =
    Query.repo flatMap { repo =>
      Query.state flatMap { state =>
        version match {
          case _: Version.Deleted => Query.value(Set.empty)
          case version: Version.Live =>
            binders(version.collID) match {
              case None => Query.value(Set.empty)
              case Some(bindings) =>
                val bindingsQ = bindings map { case (name, binding) =>
                  binding(version) map { name -> _ }
                } sequence

                bindingsQ flatMap { bindings =>
                  val tqs = terms map { case (e, r) =>
                    extractTerms(version, bindings, e, r)
                  } sequence

                  val vqs = values map { case (e, r) =>
                    extractTerms(version, bindings, e, r)
                  } sequence

                  (tqs, vqs) par { case (ts, vs) =>
                    val estimatedSize =
                      entriesSerializedSizeLowerBound(ts.view, vs.view)
                    val tooBig = estimatedSize > state.txnSizeLimitBytes
                    if (tooBig) {
                      repo.stats.incr("Indexer.SizeLimitSkipped")
                    }
                    if (tooBig && repo.enforceIndexEntriesSizeLimit) {
                      // Very unusual case: an index bomb.
                      Query.fail(
                        VersionIndexEntriesTooLargeException(
                          config.scopeID,
                          version.id,
                          config.id,
                          estimatedSize,
                          state.txnSizeLimitBytes))
                    } else {
                      // Common case: all is well.
                      Query.value {
                        // If it is a v10 index, we want index entries to show up as
                        // long as any of the terms are present. This differs from
                        // v4 in that if all covered values are null in v4, it is
                        // not present in the index.
                        val vcs = if (config.isCollectionIndex) {
                          combos[IndexTerm](vs, { _ => true })
                        } else {
                          combos[IndexTerm](vs, { !isNull(_) })
                        }
                        val tcs = combos[IndexTerm](ts, { !isNull(_) })
                        val ttl = version.ttl
                        tcs.iterator flatMap { t =>
                          vcs.iterator map { v =>
                            Tuple(t.toVector, v.toVector, ttl)
                          }
                        } toSet
                      }
                    }
                  }
                }
            }
        }
      }
    }

  private def isNull(terms: Iterable[IndexTerm]): Boolean = {
    if (terms.isEmpty) {
      false
    } else {
      terms forall {
        case IndexTerm(NullV, _) => true
        case _ => false
      }
    }
  }

  private case class Tuple(terms: Vector[IndexTerm], values: Vector[IndexTerm], ttl: Option[Timestamp])

  def hashForConsole(id: DocID)(implicit @unused ctl: ConsoleControl): IndexTerm =
    hash(id)

  private def hash(id: DocID): IndexTerm = {
    val bytes = CBOR.encode(id).array
    val hash = MurmurHash3.bytesHash(bytes)
    val part = hash.abs % partitions
    IndexTerm(LongV(part))
  }

  def rows(version: Version): Query[Seq[IndexRow]] = {
    val rows = ArraySeq.newBuilder[IndexRow]

    lazy val docIDHashTerm = hash(version.docID)

    def addEntry(tuple: Tuple, action: SetAction): Unit = {
      val terms = if (partitions > 1) {
        tuple.terms :+ docIDHashTerm
      } else {
        tuple.terms
      }
      val key = IndexKey(config.scopeID, config.id, terms)
      val t = IndexTuple(
        version.parentScopeID,
        version.docID,
        tuple.values,
        tuple.ttl
      )

      rows += IndexRow(key, IndexValue(t, version.ts, action), this)
    }

    val oldTuples = tuples(version.patch(version.diff))
    val newTuples = tuples(version)

    (oldTuples, newTuples) par { case (os, ns) =>
      os diff ns foreach {
        addEntry(_, Remove)
      }
      ns diff os foreach {
        addEntry(_, Add)
      }
      Query.value(rows.result())
    }
  }

  def matches(version: Version, terms: Vector[IndexTerm]): Query[Boolean] =
    tuples(version) map { _ exists { _.terms == terms } }
}

object Indexer {
  object Empty extends BasicIndexer {
    def config = IndexConfig.Empty
    override def rows(version: Version): Query[Seq[IndexRow]] = Query.value(Nil)
    override def matches(version: Version, terms: Vector[IndexTerm]) = Query.False
  }
}

final case class AggregateIndexer(indexers: Seq[BasicIndexer]) extends Indexer {
  def rows(version: Version): Query[Seq[IndexRow]] =
    indexers.map { _.rows(version) }.sequence map { _.flatten }

  def matches(version: Version, terms: Vector[IndexTerm]): Query[Boolean] =
    indexers.foldLeft(Query.False) { case (accQ, indexer) =>
      accQ flatMap { if (_) Query.True else indexer.matches(version, terms) }
    }
}

/**
  * Emits index rows for values of the fields at the given paths,
  * eliding entirely-null values by default.
  */
final case class FieldIndexer(config: IndexConfig) extends BasicIndexer
