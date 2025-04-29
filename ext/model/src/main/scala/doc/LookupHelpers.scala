package fauna.model

import fauna.atoms._
import fauna.repo.doc.Version
import fauna.storage.doc.Data
import fauna.storage.lookup._
import fauna.storage.{ Add, Remove }

// helpers for generating Lookup entries for databases and keys
object LookupHelpers {

  def globalIDsFromData(id: DocID, data: Data): Seq[GlobalID] =
    id match {
      case DatabaseID(_) =>
        val scopeID = Database.ScopeField.read(data.fields).toOption

        // Prior to ENG-XXX, a database's global id was derived from its scope,
        // so fall back to `scope` if `global_id` is not present.
        //
        // TODO: remove this fallback as all live dbs should have a
        // `global_id` field now.
        val globalID = Database.GlobalIDField
          .read(data.fields)
          .toOption
          .orElse(scopeID.map { s => GlobalDatabaseID(s.toLong) })

        List(scopeID, globalID).flatten

      case KeyID(id) =>
        List(GlobalKeyID(id.toLong))

      case _ => Nil
    }

  def lookups(vers: Version): Seq[LookupEntry] = {
    val (prevIDs, currIDs) = if (vers.isDeleted) {
      // always generate a list if deleted, so that we generate a remove for keys
      val p = globalIDsFromData(vers.id, vers.prevData().getOrElse(Data.empty))
      (p, Seq.empty)
    } else {
      // if there is no prev data, this is a create, so definitely start with Nil
      val p = vers.prevData().map(globalIDsFromData(vers.id, _)).getOrElse(Nil)
      val c = globalIDsFromData(vers.id, vers.data)
      (p, c)
    }

    // we want to emit lookup adds for all globals which have entered the set
    val toAdd = currIDs.toSet -- prevIDs
    // we want to emit lookup removes for all globals which have left the set
    val toRemove = prevIDs.toSet -- currIDs

    val b = Seq.newBuilder[LookupEntry]

    toAdd foreach { g =>
      b += LiveLookup(g, vers.parentScopeID, vers.id, vers.ts, Add)
    }

    toRemove foreach { g =>
      b += LiveLookup(g, vers.parentScopeID, vers.id, vers.ts, Remove)
    }

    b.result()
  }
}
