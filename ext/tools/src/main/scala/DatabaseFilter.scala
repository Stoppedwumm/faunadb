package fauna.tools

import com.carrotsearch.hppc.LongHashSet
import fauna.atoms._
import fauna.model.schema.NativeIndex
import fauna.repo.store.DatabaseTree

final class DatabaseFilter(
  val tree: DatabaseTree,
  val scopeIDs: LongHashSet,
  val globalIDs: LongHashSet) {

  val parentScopeID = tree.parentScopeID
  val dbID = tree.dbID.toDocID

  def filterVersions(scopeID: ScopeID, docID: DocID): Boolean = {
    isExportingDatabase(scopeID, docID) ||
    isParentKey(scopeID, docID) ||
    filterScope(scopeID)
  }

  @inline def filterScope(scopeID: ScopeID): Boolean =
    scopeIDs.contains(scopeID.toLong)

  @inline def isExportingDatabase(scopeID: ScopeID, docID: DocID) =
    scopeID == parentScopeID && dbID == docID

  // NB: Keys in the parentScope need to be decoded to
  // see if its "database" field points to the database
  @inline def isParentKey(scopeID: ScopeID, docID: DocID) =
    scopeID == parentScopeID && docID.collID == KeyID.collID

  def filterIndexes(scopeID: ScopeID, indexID: IndexID): Boolean = {
    if (scopeID == parentScopeID) {
      NativeIndex(scopeID, indexID).isDefined
    } else {
      scopeIDs contains scopeID.toLong
    }
  }

  def filterLookupRow(globalID: GlobalID, schemaMetadata: SchemaMetadata): Boolean =
    globalID match {
      case s: ScopeID          => scopeIDs contains s.toLong
      case g: GlobalDatabaseID => globalIDs contains g.toLong
      case k: GlobalKeyID =>
        Option(schemaMetadata.keys.get(KeyID(k.toLong))) exists {
          scopeIDs contains _.toLong
        }
    }

  /** This method leverage the fact filterLookupRow was called
    * and already did a pre-filtering
    */
  def filterLookupCell(
    globalID: GlobalID,
    parentScopeID: ScopeID,
    docID: DocID): Boolean = {
    if (scopeIDs.contains(parentScopeID.toLong)) {
      return true
    }

    globalID match {
      case ScopeID(_) | GlobalDatabaseID(_) =>
        isExportingDatabase(parentScopeID, docID)

      case _: GlobalKeyID =>
        isParentKey(parentScopeID, docID)
    }
  }
}

object DatabaseFilter {
  def apply(tree: DatabaseTree): DatabaseFilter = {
    val scopeIDs = collectAll(tree) {
      _.scopeID.toLong
    }

    val globalIDs = collectAll(tree) {
      _.globalID.toLong
    }

    new DatabaseFilter(tree, scopeIDs, globalIDs)
  }

  private def collectAll(tree: DatabaseTree)(
    fn: DatabaseTree => Long): LongHashSet = {
    val scopes = new LongHashSet

    def collect0(node: DatabaseTree): Unit = {
      scopes.add(fn(node))
      node.children foreach { collect0 }
    }

    collect0(tree)
    scopes
  }
}
