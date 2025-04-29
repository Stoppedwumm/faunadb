package fauna.repo

import fauna.atoms._
import fauna.lang.syntax.option.SomeNil
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.Selector
import org.github.jamm.Unmetered

sealed abstract class Constraint
object Unconstrained extends Constraint

/** Applies a uniqueness constraint on an index's terms: only a single
  * document may exist for a set of terms.
  */
object UniqueTerms extends Constraint

/** Applies a uniqueness constraint on an index's terms _and_ values:
  * only a single document may exist with any terms/values
  * combination.
  */
object UniqueValues extends Constraint

/** Abstraction over the underlying source of an index's configuration,
  * which may be "native" (i.e. static) or user-defined. Index users
  * are encouraged not to rely on the specific source of index
  * configuration, and instead rely on this.
  */
// NB: jamm on JDK17 cannot measure classes in this hierarchy due to how Scala
// compiles `TermExtractor` and `binders` functions. Leave it unmetered until jamm is
// removed.
@Unmetered
trait IndexConfig {

  def scopeID: ScopeID
  def id: IndexID
  def sources: IndexSources
  def binders: CollectionID => Option[List[TermBinder]]
  def terms: Vector[(TermExtractor, Boolean)]
  def values: Vector[(TermExtractor, Boolean)]
  def constraint: Constraint
  def isSerial: Boolean
  def partitions: Long = 1
  def isCollectionIndex: Boolean = false

  def isPartitioned: Boolean = partitions > 1

  def partitions(terms: Vector[IndexTerm]): Seq[Vector[IndexTerm]] =
    partitionTerms(terms) { IndexTerm(_) }

  def partitionTerms[A](terms: Vector[A])(fn: LongV => A): Seq[Vector[A]] =
    if (isPartitioned) {
      for (part <- 0L until partitions)
        yield terms :+ fn(LongV(part))
    } else {
      Seq(terms)
    }

  def indexer: BasicIndexer = FieldIndexer(this)

  def reverseFlags = values.view map { case (_, reverse) => reverse }

  // Whether or not reading the index affects transaction success.
  def serialReads = (constraint != Unconstrained) || isSerial

  def pad(tuple: IndexTuple, floor: Boolean): IndexTuple =
    if (tuple.values.sizeIs >= values.size) {
      tuple
    } else {
      val paddedValues = IndexConfig.pad(tuple.values, reverseFlags, floor)
      IndexTuple(tuple.scopeID, tuple.docID, paddedValues, tuple.ttl)
    }

  def pad(value: IndexValue, floor: Boolean): IndexValue =
    value.withTuple(pad(value.tuple, floor))

  def selector: Selector =
    sources match {
      case IndexSources.All          => Selector.from(scopeID)
      case IndexSources.Custom       => Selector.UserCollections(scopeID)
      case IndexSources.Limit(colls) => Selector.from(scopeID, colls)
    }
}

object IndexConfig {

  def pad(
    values: Vector[IndexTerm],
    reverseFlags: Iterable[Boolean],
    floor: Boolean): Vector[IndexTerm] = {

    if (reverseFlags.sizeIs <= values.size) {
      values
    } else {
      val padding = reverseFlags.iterator.drop(values.size) map { reverse =>
        val term = if (floor ^ reverse) IndexTerm.MinValue else IndexTerm.MaxValue
        term.copy(reverse = reverse)
      }
      values ++ padding
    }
  }

  def defaultExtractor(path: List[String], reverse: Boolean): TermExtractor = {
    (version, _) =>
      val terms = version.fields.fields.selectValues(path).iterator.distinct map {
        IndexTerm(_, reverse)
      }
      Query.value(terms.toVector)
  }

  def dropPartitionKey(terms: Vector[IRValue]): Vector[IRValue] = {
    require(
      terms.nonEmpty && terms.last.isInstanceOf[LongV],
      s"invalid partition key ${terms.lastOption.orNull}"
    )
    terms.dropRight(1)
  }

  object Empty extends IndexConfig {
    val scopeID = ScopeID.MinValue
    val id = IndexID.MinValue
    val sources = IndexSources.empty
    val binders = { _: CollectionID => None }
    val defaultBinder = None
    val terms = Vector.empty
    val values = Vector.empty
    val constraint = Unconstrained
    val isSerial = false
  }

  final case class Native(
    scopeID: ScopeID,
    id: IndexID,
    sources: IndexSources,
    terms: Vector[(TermExtractor, Boolean)],
    constraint: Constraint,
    isSerial: Boolean,
    values: Vector[(TermExtractor, Boolean)] = Vector.empty,
    override val partitions: Long = 1)
      extends IndexConfig {

    val binders = {
      val Empty = SomeNil
      sources match {
        case IndexSources.All | IndexSources.Custom => { _: CollectionID => Empty }
        case IndexSources.Limit(ids) => { cls: CollectionID =>
          if (ids contains cls) Empty else None
        }
      }
    }
  }

  def DocumentsByCollection(scope: ScopeID) = {
    val extractor: TermExtractor = { (vers, _) =>
      Query.value(Vector(IndexTerm(vers.collID.toDocID)))
    }
    val terms = Vector((extractor, false))

    Native(
      scope,
      NativeIndexID.DocumentsByCollection,
      IndexSources.All,
      terms,
      constraint = Unconstrained,
      isSerial = false,
      partitions = 8)
  }

  // NB: This index is not backfilled! It was initially added in January of 2024,
  // where it only coverred user defined documents. Then, it was later updated in
  // August of 2024 to cover native collections as well.
  //
  // All updates written before these times are not filled in this index, as it has
  // not been backfilled!
  def ChangesByCollection(scope: ScopeID) = {
    val terms: TermExtractor = { (version, _) =>
      Query.value(Vector(IndexTerm(version.collID.toDocID)))
    }
    val values: TermExtractor = { (version, _) =>
      Query.value(Vector(IndexTerm(version.validTSV)))
    }

    Native(
      scope,
      NativeIndexID.ChangesByCollection,
      IndexSources.All,
      terms = Vector((terms, false)),
      values = Vector((values, false)),
      constraint = Unconstrained,
      isSerial = false,
      partitions = 8
    )
  }
}

/** The entire record in the index store.
  */
case class IndexRow(key: IndexKey, value: IndexValue, indexer: BasicIndexer) {
  def constraint: Constraint = indexer.constraint

  override def toString = s"$key $value"
}
