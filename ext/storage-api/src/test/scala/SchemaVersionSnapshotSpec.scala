package fauna.storage.api.test

import fauna.prop.Prop
import fauna.storage.api.SchemaVersionSnapshot
import fauna.storage.ops.RemoveAllWrite
import fauna.storage.Tables

class SchemaVersionSnapshotSpec extends Spec("schemaversion") {
  storageProp("works") { engine =>
    for {
      (scopeID0, _) <- idsP
      (scopeID1, _) <- idsP
      ts0 <- Prop.timestamp()
      ts1 <- Prop.timestampAfter(ts0)
      ts2 <- Prop.timestampAfter(ts1)
    } yield {
      val sv = Tables.SchemaVersions
      applyWrites(engine, ts0, RemoveAllWrite(sv.CFName, sv.rowKey(scopeID0)))
      applyWrites(engine, ts1, RemoveAllWrite(sv.CFName, sv.rowKey(scopeID1)))

      // Does it work?
      runSchemaVersion(engine, SchemaVersionSnapshot(scopeID0, ts2)) should be (ts0)
      runSchemaVersion(engine, SchemaVersionSnapshot(scopeID1, ts2)) should be (ts1)

      // Does it work after an update?
      applyWrites(engine, ts2, RemoveAllWrite(sv.CFName, sv.rowKey(scopeID0)))
      runSchemaVersion(engine, SchemaVersionSnapshot(scopeID0, ts2)) should be (ts2)
    }
  }
}