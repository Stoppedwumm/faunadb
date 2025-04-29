package fauna.storage.api.version.test

import fauna.atoms.SchemaVersion
import fauna.lang.Timestamp
import fauna.prop.Prop
import fauna.storage.{ AtValid, Create, Delete, DocAction, Update, VersionID }
import fauna.storage.api.test._
import fauna.storage.api.version._
import fauna.storage.doc.Data
import fauna.storage.ops.DocRemove
import fauna.storage.ops.VersionAdd
import fauna.storage.ops.Write
import fauna.storage.BiTimestamp
import fauna.storage.Unresolved

class DocHistoryForUpdateSpec extends Spec("dochistoryforupdate") {
  import DocHistoryForUpdate._

  storageProp("returns an Append on missing doc") { engine =>
    for {
      (scopeID, docID) <- idsP
      minValidTS       <- Prop.timestamp()
      validTS          <- Prop.timestampAfter(minValidTS)
      snapshotTS       <- Prop.timestampAfter(validTS)
      action           <- Prop.choose(Seq(Create, Update, Delete))
    } yield {
      val res =
        runRead(
          engine,
          DocHistoryForUpdate(
            scopeID,
            docID,
            VersionID(validTS, action),
            snapshotTS,
            minValidTS
          ))

      inside(res) { case r: Result.Append =>
        r.latest shouldBe empty
      }
    }
  }

  storageProp("returns versions around history change") { engine =>
    for {
      (scopeID, docID) <- idsP
      minValidTS       <- Prop.timestamp()
      createTS         <- Prop.timestampAfter(minValidTS)
      updateTS         <- Prop.timestampAfter(createTS)
      deleteTS         <- Prop.timestampAfter(updateTS)
      recreateTS       <- Prop.timestampAfter(deleteTS)
      snapshotTS       <- Prop.timestampAfter(recreateTS)
      action           <- Prop.choose(Seq(Create, Update, Delete))

      history =
        createBasicHistory(
          engine,
          scopeID,
          docID,
          createTS,
          updateTS,
          deleteTS,
          recreateTS
        )
    } yield {
      def read0(validTS: Timestamp, action: DocAction) =
        runRead(
          engine,
          DocHistoryForUpdate(
            scopeID,
            docID,
            VersionID(validTS, action),
            snapshotTS,
            minValidTS
          ))

      // Before create: rewrite history
      inside(read0(createTS.prevNano, action)) { case r: Result.Rewrite =>
        r.before shouldBe empty
        r.current shouldBe empty
        (r.after.value: TestVersion) shouldBe history.create
      }

      // At create: rewrite history
      inside(read0(createTS, Create)) { case r: Result.Rewrite =>
        r.before shouldBe empty
        (r.current.value: TestVersion) shouldBe history.create
        (r.after.value: TestVersion) shouldBe history.update
      }

      // After create: rewrite history
      inside(read0(createTS.nextMicro, action)) { case r: Result.Rewrite =>
        (r.before.value: TestVersion) shouldBe history.create
        r.current shouldBe empty
        (r.after.value: TestVersion) shouldBe history.update
      }

      // At update: rewrite history
      inside(read0(updateTS, Update)) { case r: Result.Rewrite =>
        (r.before.value: TestVersion) shouldBe history.create
        (r.current.value: TestVersion) shouldBe history.update
        (r.after.value: TestVersion) shouldBe history.delete
      }

      // After update: rewrite history
      inside(read0(updateTS.nextMicro, action)) { case r: Result.Rewrite =>
        (r.before.value: TestVersion) shouldBe history.update
        r.current shouldBe empty
        (r.after.value: TestVersion) shouldBe history.delete
      }

      // At delete: rewrite history
      inside(read0(deleteTS, Delete)) { case r: Result.Rewrite =>
        (r.before.value: TestVersion) shouldBe history.update
        (r.current.value: TestVersion) shouldBe history.delete
        (r.after.value: TestVersion) shouldBe history.recreate
      }

      // After delete: rewrite history
      inside(read0(deleteTS.nextMicro, action)) { case r: Result.Rewrite =>
        (r.before.value: TestVersion) shouldBe history.delete
        r.current shouldBe empty
        (r.after.value: TestVersion) shouldBe history.recreate
      }

      // At recreate: rewrite history
      inside(read0(recreateTS, Create)) { case r: Result.Rewrite =>
        (r.before.value: TestVersion) shouldBe history.delete
        (r.current.value: TestVersion) shouldBe history.recreate
        r.after shouldBe empty
      }

      // After recreate: append to history
      inside(read0(recreateTS.nextMicro, action)) { case r: Result.Append =>
        (r.latest.value: TestVersion) shouldBe history.recreate
      }
    }
  }

  storageProp("merge pending writes") { engine =>
    for {
      (scopeID, docID) <- idsP
      minValidTS       <- Prop.timestamp()
      createTS         <- Prop.timestampAfter(minValidTS)
      updateTS         <- Prop.timestampAfter(createTS)
      deleteTS         <- Prop.timestampAfter(updateTS)
      recreateTS       <- Prop.timestampAfter(deleteTS)
      snapshotTS       <- Prop.timestampAfter(recreateTS)
      action           <- Prop.choose(Seq(Create, Update, Delete))

      history =
        createBasicHistory(
          engine,
          scopeID,
          docID,
          createTS,
          updateTS,
          deleteTS,
          recreateTS
        )
    } yield {
      def write(ts: Timestamp) =
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(ts),
          schemaVersion = SchemaVersion.Min,
          action = action,
          data = Data.empty,
          diff = None
        )

      // Pending writes override create, update, and delete versions.
      val pendingWrites =
        Seq(
          write(createTS),
          write(updateTS),
          write(deleteTS)
        )

      // Read touches the rewritten history
      val result =
        runRead(
          engine,
          DocHistoryForUpdate(
            scopeID,
            docID,
            VersionID(updateTS, action),
            snapshotTS,
            minValidTS
          ),
          pendingWrites
        )

      inside(result) { case r: Result.Rewrite =>
        r.before.value.ts shouldBe AtValid(createTS)
        r.current.value.ts shouldBe AtValid(updateTS)
        r.after.value.ts shouldBe AtValid(deleteTS)
      }
    }
  }

  prop("skips IO") {
    for {
      (scopeID, docID) <- idsP
      minValidTS       <- Prop.timestamp()
      arbitryTS        <- Prop.timestampAfter(minValidTS)
      snapshotTS       <- Prop.timestampAfter(arbitryTS)
      action           <- Prop.choose(Seq(Create, Update, Delete))
    } yield {
      def add(writeTS: BiTimestamp) =
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS,
          schemaVersion = SchemaVersion.Min,
          action = action,
          data = Data.empty,
          diff = None
        )

      val remove = DocRemove(scopeID, docID)

      def skipIO(validTS: Timestamp, pendingWrites: Write*) = {
        val vsID = VersionID(validTS, action)
        val op = DocHistoryForUpdate(scopeID, docID, vsID, snapshotTS, minValidTS)
        op.skipIO(pendingWrites)
      }

      // Do not skip...
      skipIO(arbitryTS) shouldBe empty
      skipIO(arbitryTS, add(Unresolved)) shouldBe empty
      skipIO(Timestamp.MaxMicros, add(AtValid(arbitryTS))) shouldBe empty
      skipIO(Timestamp.MaxMicros, add(Unresolved), remove) shouldBe empty

      // Skip
      skipIO(Timestamp.MaxMicros, add(Unresolved)).value.current.ts shouldBe
        Unresolved
    }
  }
}
