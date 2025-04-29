package fauna.repo.store

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query._
import fauna.storage._
import fauna.storage.api.version.StorageVersion
import fauna.storage.cassandra.comparators._
import fauna.storage.ops._
import fauna.trace._
import io.netty.buffer.Unpooled
import scala.collection.mutable.{ Map => MMap }

/**
  * Revisions are rooted, continuous timelines, in source
  * (i.e. on-disk) and target (i.e. desired) forms. They are used as a
  * common medium of exchange between VersionStore and IndexStore.
  */
private[repo] case class Revision(root: Version, source: Seq[Conflict[Version]], target: Seq[Version])

private[repo] object Revision {
  def apply(root: Version, source: Iterable[Version], target: Seq[Version]): Revision =
    apply(root, Version.resolveConflicts(source), target)
}

/**
 * A VersionStore is a persisted map from a DocID to a history of the
 * Document's contents.
 *
 * Each "version" in the history is a snapshot of the document's data
 * and the delta from the previous version in (valid) time order of
 * application. A Document is thus versioned over time, wherein each
 * Version's snapshot is the union of all deltas from the Document's
 * creation to a point in time (a snapshot time).
 *
 * The history of a Document may be altered by inserting or removing a
 * version at a point in time. This point may be arbitrarily far in
 * the past or future.
 *
 * <scope> and <id> are CBOR serialized IDs. <valid_ts>, <action> and
 * <txn_ts> are a valid timestamp, create/delete flag and transaction
 * timestamp - they collectively compose a version.  <data> is a CBOR
 * serialized blob of the value of the Version *as of* this valid
 * time. <diff> is a CBOR serialized blob of the diff from this
 * Version to the previous Version.
 */
object VersionStore {
  type Key = Tables.Versions.Key

  implicit val keyCodec = implicitly[CassandraCodec[Key]]

  def sparseScan(
    bounds: ScanBounds): PagedQuery[Iterable[((ScopeID, DocID), StorageVersion)]] =
    sparseScan(bounds, None, Selector.All)

  /** A sparse scan returns the latest Version of each document within
    * the scan bounds, in pages of the given size.
    */
  def sparseScan(bounds: ScanBounds, from: Option[Version], selector: Selector)
      : PagedQuery[Iterable[((ScopeID, DocID), StorageVersion)]] = {

    val col = from flatMap { v =>
      val row = Tables.Versions.rowKey(v.parentScopeID, v.docID)
      val key = (row, v.ts.validTS, v.action, v.ts.transactionTS)
      val pred = Predicate(key)(keyCodec)
      Tables.Versions.Schema.encodePrefix(pred, Predicate.GTE)
    }

    Store.docScanRaw(
      bounds,
      col.getOrElse(Unpooled.EMPTY_BUFFER),
      selector,
      BulkPageSize) mapValuesT { v =>
      ((v.scopeID, v.docID), v)
    }
  }

  // writes

  // Skips index revisions, under the assumption that will happen in a separate background task.
  def clear(scopeID: ScopeID, id: DocID): Query[Unit] =
    Query.write(DocRemove(scopeID, id))

  // version revision/repair

  /**
    * Given two contiguous versions, produces a (possibly empty) set of
    * writes which will:
    *
    * 1. remove a's conflicts from storage
    * 2. rewrite a.diff such that a.patch(a.diff) equals b.data
    * 3. remove a, if both a and b are deletes
    *
    * Returns a Revision representing this section of this document's
    * history, if it has changed.
    */
  def repair(a: Conflict[Version], b: Conflict[Version]): Query[Option[Revision]] = {
    val aVers = a.canonical
    val bVers = b.canonical

    require(aVers.parentScopeID == bVers.parentScopeID &&
      aVers.id == bVers.id,
      s"versions must be from the same document $a != $b")

    require(aVers.ts.validTS > bVers.ts.validTS,
      s"versions must be in descending time order $a <= $b")

    val remove = Seq.newBuilder[Version]
    var kill = false
    var rewrite: Option[Version] = None

    Query.repo flatMap { repo =>
      remove ++= a.conflicts
      repo.stats.count("Schema.Documents.Events.Conflicts", a.conflicts.size)

      // NOTE: the following is intended to prevent hydration of data
      // and diff objects unless absolutely necessary

      // if both a and b are deletes and b is not the origin, remove a;
      // it is meaningless
      if (aVers.isDeleted && bVers.isDeleted &&
        bVers.ts.validTS > Timestamp.Epoch) {

        repo.stats.incr("Schema.Documents.Events.DoubleDelete")
        kill = true
        remove += aVers

        // b is a delete and used to be a create, but a still has
        // a non-empty diff
      } else if (bVers.isDeleted && aVers.diff.isDefined) {
        repo.stats.incr("Schema.Documents.Events.ObsoleteDiff")

        rewrite = Some(aVers.withUnresolvedTS.withDiff(None))
        remove += aVers

        // b is a create and used to be a delete, but a still has an empty
        // diff
      } else if (!bVers.isDeleted && aVers.diff.isEmpty) {
        repo.stats.incr("Schema.Documents.Events.MissingDiff")

        val diff = Some(aVers.fields.diffTo(bVers.fields))
        rewrite = Some(aVers.withUnresolvedTS.withDiff(diff))
        remove += aVers

        // b is a create and used to have different data, but a still has
        // an incorrect diff
      } else if (!bVers.isDeleted &&
        !aVers.prevFields().contains(bVers.fields)) {
        repo.stats.incr("Schema.Documents.Events.StaleDiff")

        val diff = Some(aVers.fields.diffTo(bVers.fields))
        rewrite = Some(aVers.withUnresolvedTS.withDiff(diff))
        remove += aVers
      }

      val removes = remove.result()
      val count = rewrite.size + removes.size

      if (count > 0) {
        val adds = rewrite map { addVersion(_) } toSeq
        val rems = removes map { removeVersion(_) }

        (adds ++ rems).join flatMap { _ =>
          repo.stats.count("Schema.Document.Events.Repaired", count)

          val target = if (kill) {
            Nil
          } else {
            Seq(rewrite getOrElse aVers)
          }

          Query.some(Revision(bVers, Seq(a), target))
        }
      } else {
        Query.none
      }
    }
  }

  private[repo] def revise(revision: Revision): Query[Revision] = {
    val newVersions = MMap.empty[VersionID, Version]
    val obsoleteVersions = Seq.newBuilder[Version]

    (revision.target foldLeft revision.root) { (prev, version) =>
      val diff = if (prev.action.isCreate) {
        Some(version.fields.diffTo(prev.fields))
      } else {
        None
      }

      newVersions(version.versionID) = version.withDiff(diff)
      version
    }

    revision.source foreach { col =>
      obsoleteVersions ++= col.conflicts

      newVersions.get(col.canonical.versionID) match {
        case Some(v) if col.canonical == v && col.canonical.diff == v.diff =>
          newVersions -= col.canonical.versionID
        case _ => obsoleteVersions += col.canonical
      }
    }

    val target = (revision.root +: revision.target) map { vers =>
      newVersions.get(vers.versionID) match {
        case Some(v) => v
        case None    => vers
      }
    }

    val revised = newVersions.values
    val obsolete = obsoleteVersions.result()
    val remove = obsolete map removeVersion join
    val add = revised map addVersion join

    val rev = Revision(target.head, revision.source, target.tail)

    Seq(remove, add).join map { _ => rev }
  }

  private def addVersion(v: Version): Query[Unit] = {
    val action =
      if (v.action.isCreate) {
        v.diff.fold(Create: DocAction)(_ => Update)
      } else {
        v.action
      }

    val value =
      VersionAdd(
        v.parentScopeID,
        v.docID,
        v.ts,
        action,
        v.schemaVersion,
        v.data,
        v.diff
      )

    Query.write(value) map { _ =>
      if (isTracingEnabled) {
        traceMsg(s"  INSERT Versions -> $value")
      }
    }
  }

  private def removeVersion(v: Version): Query[Unit] = {
    val value = VersionRemove(v.parentScopeID,
      v.docID,
      v.ts,
      v.action)

    Query.write(value) map { _ =>
      if (isTracingEnabled) {
        traceMsg(s"  REMOVE Versions -> $value")
      }
    }
  }
}
