package fauna.repair

import fauna.atoms.{
  DatabaseID,
  DocID,
  GlobalDatabaseID,
  GlobalID,
  GlobalKeyID,
  KeyID,
  ScopeID
}
import fauna.codex.json.{ JSArray, JSValue }
import fauna.codex.json2.{ JSON, JSONParser }
import fauna.lang.Timestamp
import fauna.util.ZBase32
import java.nio.file.{ Files, Paths }

/** snapshotTS in this context is the timestamp at which the snapshot was
  * taken that generated the lookup entries to repair.
  */
case class LookupsInput(snapshotTS: Timestamp, lookups: Seq[LookupData])

case class LookupData(globalID: GlobalID, scope: ScopeID, docID: DocID)

object LookupsParser {
  def parseLookupsFromFile(filePath: String): LookupsInput = {
    val fbytes = Files.readAllBytes(Paths.get(filePath))
    val json = JSON.decode[JSValue](JSONParser(fbytes))
    val readTS = Timestamp.parse((json / "snapshot_time").as[String])
    LookupsInput(
      snapshotTS = readTS,
      lookups = (json / "lookups").as[JSArray].value.map(jsonEntryToLookup)
    )
  }

  private def jsonEntryToLookup(lookupJson: JSValue): LookupData = {
    val tt = (lookupJson / "type").as[String]
    val globalID: GlobalID = tt match {
      case "global_id" =>
        if (!(lookupJson / "db_id").isEmpty) {
          GlobalDatabaseID(ZBase32.decodeLong((lookupJson / "id").as[String]))
          // todo is key_id right here?
        } else if (!(lookupJson / "key_id").isEmpty) {
          GlobalKeyID(ZBase32.decodeLong((lookupJson / "id").as[String]))
        } else {
          throw new IllegalStateException(
            s"entry included global_id but did not have an accompanying db_id or key_id, $lookupJson"
          )
        }
      case "scope_id" =>
        ScopeID((lookupJson / "id").as[String].toLong)
      case v =>
        throw new IllegalStateException(s"unexpected lookup type found: $v")
    }

    val scopeID = ScopeID((lookupJson / "parent_id").as[String].toLong)
    val docID = globalID match {
      case _: GlobalDatabaseID =>
        DatabaseID((lookupJson / "db_id").as[String].toLong).toDocID
      // todo: is this always a database id?
      case _: ScopeID => DatabaseID((lookupJson / "db_id").as[String].toLong).toDocID
      case _: GlobalKeyID =>
        KeyID((lookupJson / "key_id").as[String].toLong).toDocID
    }

    LookupData(globalID, scopeID, docID)
  }
}
