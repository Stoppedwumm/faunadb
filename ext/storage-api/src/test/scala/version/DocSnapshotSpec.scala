package fauna.storage.api.version.test

import fauna.atoms.SchemaVersion
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.prop._
import fauna.storage._
import fauna.storage.api._
import fauna.storage.api.test._
import fauna.storage.api.version.{ DocSnapshot, StorageVersion }
import fauna.storage.doc._
import fauna.storage.ir.{ MapV, StringV }
import fauna.storage.ops._
import scala.util.Random

class DocSnapshotSpec extends Spec("docsnapshot") {

  storageProp("read snapshots anywhere in a document's history") { engine =>
    for {
      (scopeID, docID) <- idsP
      precreateTS      <- Prop.timestamp()
      createTS         <- Prop.timestampAfter(precreateTS)
      updateTS         <- Prop.timestampAfter(createTS)
      deleteTS         <- Prop.timestampAfter(updateTS)
      recreateTS       <- Prop.timestampAfter(deleteTS)
    } yield {
      createBasicHistory(
        engine,
        scopeID,
        docID,
        createTS,
        updateTS,
        deleteTS,
        recreateTS)

      val mksnap = docSnapshotFactory(scopeID, docID)(_, _)

      // Check the snapshot version is correct in each region of history.
      // No version before the document's creation.
      runDocSnapshot(engine, mksnap(precreateTS, createTS)) shouldBe empty

      // v1 Version after creation and before update.
      runDocSnapshot(engine, mksnap(createTS, updateTS)).value shouldBe
        TestVersion.Live(
          scopeID,
          docID,
          Some(createTS),
          None,
          createData,
          None
        )

      // v2 version after update and before delete.
      val updateVersion = TestVersion.Live(
        scopeID,
        docID,
        Some(updateTS),
        None,
        updateData,
        Some(Diff(createData.fields))
      )
      runDocSnapshot(engine, mksnap(updateTS, deleteTS)).value shouldBe updateVersion

      // No version after delete and before recreate.
      runDocSnapshot(engine, mksnap(deleteTS, recreateTS)) shouldBe empty

      // Specifying a Create action at the delete timestamp should miss the delete
      // because Create < Delete, and so the snapshot should be the version as of
      // updateTS.
      runDocSnapshot(
        engine,
        DocSnapshot(
          scopeID,
          docID,
          deleteTS,
          Timestamp.Epoch,
          VersionID(deleteTS, Create))).value shouldBe updateVersion

      // v3 version after recreation.
      runDocSnapshot(engine, mksnap(recreateTS, Clock.time)).value shouldBe
        TestVersion.Live(
          scopeID,
          docID,
          Some(recreateTS),
          None,
          recreateData,
          None
        )
    }
  }

  storageProp("snapshot is filtered by version TTL") { engine =>
    for {
      (scopeID, docID) <- idsP
      earlyTS          <- Prop.timestamp()
      txnTS            <- Prop.timestampAfter(earlyTS)
      ttlTS            <- Prop.timestampAfter(txnTS)
      snapshotTS       <- Prop.timestampAfter(ttlTS)
    } yield {
      val data = Data(Field[Option[Timestamp]]("ttl") -> Some(ttlTS))

      val earlyWrite =
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = Resolved(earlyTS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = Data.empty,
          diff = None
        )

      val write =
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = Resolved(txnTS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = data,
          diff = None
        )

      applyWrites(engine, earlyTS, earlyWrite)
      applyWrites(engine, txnTS, write)

      val version =
        runDocSnapshot(
          engine,
          DocSnapshot(
            scopeID,
            docID,
            snapshotTS
          ))

      version shouldBe empty
    }
  }

  storageProp("snapshot resolves version conflicts") { engine =>
    for {
      (scopeID, docID) <- idsP
      // For txn-time-ordered conflict resolution, use timestamps after the change
      // epoch. See Version.scala.
      changeEpoch <- Prop.const(Timestamp.parse("2019-06-01T00:00:00Z"))
      firstTS     <- Prop.timestampAfter(changeEpoch)
      secondTS    <- Prop.timestampAfter(firstTS)
      thirdTS     <- Prop.timestampAfter(secondTS)
    } yield {
      val write = writeFactory(engine, scopeID, docID)

      val data0 = Data(Field[String]("version") -> "v0")
      val data1 = Data(Field[String]("version") -> "v1")

      def mksnap(ts: Timestamp) = docSnapshotFactory(scopeID, docID)(ts, Clock.time)

      // No conflict, no problem.
      write(firstTS, firstTS, Create, data0, None)
      runDocSnapshot(engine, mksnap(firstTS)).value shouldBe
        TestVersion.Live(
          scopeID,
          docID,
          Some(firstTS),
          None,
          data0,
          None
        )

      // Two versions in conflict with a delete winning.
      write(firstTS, secondTS, Delete, Data.empty, None)
      runDocSnapshot(engine, mksnap(secondTS)) shouldBe None

      // Three versions in conflict with a create winning.
      write(firstTS, thirdTS, Create, data1, None)
      runDocSnapshot(engine, mksnap(thirdTS)).value shouldBe
        TestVersion.Live(
          scopeID,
          docID,
          Some(firstTS),
          None,
          data1,
          None
        )
    }
  }

  storageProp("merge additive write intents upon reading") { engine =>
    for {
      (scopeID, docID) <- idsP
      beforeWriteTS    <- Prop.timestamp()
      writeTS          <- Prop.timestampAfter(beforeWriteTS)
      afterWriteTS     <- Prop.timestampAfter(writeTS)
      snapshotTS       <- Prop.timestampAfter(afterWriteTS)

      read = DocSnapshot(scopeID, docID, snapshotTS)
      data0 = Data(Field[String]("version") -> "v0")
      data1 = Data(Field[String]("version") -> "v1")

      _ = applyWrites(
        engine,
        writeTS,
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(writeTS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = data0,
          diff = None
        ))
    } yield {
      // writing before the latest version have no affect on the snapshot
      runDocSnapshot(
        engine,
        read,
        Seq(
          VersionAdd(
            scopeID,
            docID,
            AtValid(beforeWriteTS),
            Create,
            schemaVersion = SchemaVersion.Min,
            data1,
            None
          ))
      ).value shouldBe TestVersion.Live(
        scopeID,
        docID,
        Some(writeTS),
        None,
        data0,
        None)

      // writing after the latest version affects the snapshot
      runDocSnapshot(
        engine,
        read,
        Seq(
          VersionAdd(
            scopeID,
            docID,
            AtValid(afterWriteTS),
            Create,
            schemaVersion = SchemaVersion.Min,
            data1,
            None
          ))
      ).value shouldBe TestVersion.Live(
        scopeID,
        docID,
        Some(afterWriteTS),
        None,
        data1,
        None)

      // deleting a document should affect the snapshot
      runDocSnapshot(
        engine,
        read,
        Seq(
          VersionAdd(
            scopeID,
            docID,
            AtValid(afterWriteTS),
            Delete,
            schemaVersion = SchemaVersion.Min,
            Data.empty,
            None
          ))
      ) shouldBe empty
    }
  }

  storageProp("merge destructive write intents upon reading") { engine =>
    for {
      (scopeID, docID) <- idsP
      vs0TS            <- Prop.timestamp()
      vs1TS            <- Prop.timestampAfter(vs0TS)
      vs2TS            <- Prop.timestampAfter(vs1TS)
      writeTS          <- Prop.timestampAfter(vs2TS)
      snapshotTS       <- Prop.timestampAfter(writeTS)

      read = DocSnapshot(scopeID, docID, snapshotTS)
      data0 = Data(Field[String]("version") -> "v0")
      data1 = Data(Field[String]("version") -> "v1")
      data2 = Data(Field[String]("version") -> "v2")

      _ = applyWrites(
        engine,
        writeTS,
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(vs0TS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = data0,
          diff = None
        ),
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(vs1TS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = data1,
          diff = None
        ),
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(vs2TS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = data2,
          diff = None
        )
      )
    } yield {
      // remove v1, returns v2
      runDocSnapshot(
        engine,
        read,
        Seq(VersionRemove(scopeID, docID, Resolved(vs1TS, writeTS), Create))
      ).value shouldBe TestVersion.Live(
        scopeID,
        docID,
        Some(vs2TS),
        None,
        data2,
        None)

      // remove v2, returns v1
      runDocSnapshot(
        engine,
        read,
        Seq(VersionRemove(scopeID, docID, Resolved(vs2TS, writeTS), Create))
      ).value shouldBe TestVersion.Live(
        scopeID,
        docID,
        Some(vs1TS),
        None,
        data1,
        None)

      // remove v2 and v1, returns v0
      runDocSnapshot(
        engine,
        read,
        Random.shuffle(
          Seq(
            VersionRemove(scopeID, docID, Resolved(vs2TS, writeTS), Create),
            VersionRemove(scopeID, docID, Resolved(vs1TS, writeTS), Create)
          ))
      ).value shouldBe TestVersion.Live(
        scopeID,
        docID,
        Some(vs0TS),
        None,
        data0,
        None)

      // remove all versions, returns none
      runDocSnapshot(
        engine,
        read,
        Random.shuffle(
          Seq(
            VersionRemove(scopeID, docID, Resolved(vs2TS, writeTS), Create),
            VersionRemove(scopeID, docID, Resolved(vs1TS, writeTS), Create),
            VersionRemove(scopeID, docID, Resolved(vs0TS, writeTS), Create)
          ))
      ) shouldBe empty
    }
  }

  storageProp("merge clear row write intent upon reading") { engine =>
    for {
      (scopeID, docID) <- idsP
      writeTS          <- Prop.timestamp()
      snapshotTS       <- Prop.timestampAfter(writeTS)

      read = DocSnapshot(scopeID, docID, snapshotTS)
      _ = applyWrites(
        engine,
        writeTS,
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(writeTS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = Data.empty,
          diff = None
        ))
    } yield {
      // existing value before clear row
      runDocSnapshot(engine, read).value shouldBe
        TestVersion.Live(scopeID, docID, Some(writeTS), None, Data.empty, None)

      // clear the entire row
      runDocSnapshot(
        engine,
        read,
        Seq(
          RemoveAllWrite(
            Tables.Versions.CFName,
            read.rowKey
          ))
      ) shouldBe empty
    }
  }

  storageProp("merge remove doc write intent upon reading") { engine =>
    for {
      (scopeID, docID) <- idsP
      writeTS          <- Prop.timestamp()
      snapshotTS       <- Prop.timestampAfter(writeTS)

      read = DocSnapshot(scopeID, docID, snapshotTS)
      _ = applyWrites(
        engine,
        writeTS,
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(writeTS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = Data.empty,
          diff = None
        ))
    } yield {
      // existing value before clear row
      runDocSnapshot(engine, read).value shouldBe
        TestVersion.Live(scopeID, docID, Some(writeTS), None, Data.empty, None)

      // remove the entire row
      runDocSnapshot(engine, read, Seq(DocRemove(scopeID, docID))) shouldBe empty
    }
  }

  storageProp("apply GC rules") { engine =>
    for {
      (scopeID, docID) <- idsP
      createTS         <- Prop.timestamp()
      updateTS         <- Prop.timestampAfter(createTS)
      deleteTS         <- Prop.timestampAfter(updateTS)
      recreateTS       <- Prop.timestampAfter(deleteTS)
      snapshotTS       <- Prop.timestampAfter(recreateTS)

      history = createBasicHistory(
        engine,
        scopeID,
        docID,
        createTS,
        updateTS,
        deleteTS,
        recreateTS
      )
    } yield {
      def sliceAt(validTS: Timestamp, minValidTS: Timestamp) =
        runDocSnapshot(
          engine,
          DocSnapshot(
            scopeID,
            docID,
            snapshotTS,
            minValidTS,
            Some(validTS)
          ))

      // now <--------------------------------------------- epoch
      //                                     +-----MVT----+
      //                                     |            |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                                     |
      //                                     v
      //                                GC edge & root
      //                                     |
      //                                     v
      //                                valid time
      all(
        Seq(
          sliceAt(createTS, Timestamp.Epoch).value,
          sliceAt(createTS, createTS).value
        )) shouldBe history.create

      // now <--------------------------------------------- epoch
      //                                   +-----MVT------+
      //                                   |              |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                            |        |
      //                            |        v
      //                            |   GC edge & root
      //                            V
      //                       valid time
      all(
        Seq(
          sliceAt(updateTS, Timestamp.Epoch).value,
          sliceAt(updateTS, createTS).value,
          sliceAt(updateTS, createTS.nextMicro).value
        )) shouldBe history.update

      // now <--------------------------------------------- epoch
      //                           MVT     +- DEAD -+
      //                            ^      |        |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                            |
      //                            v
      //                     GC edge & root
      //                            |
      //                            v
      //                       valid time
      sliceAt(updateTS, updateTS).value shouldBe history.update.copy(diff = None)

      // now <--------------------------------------------- epoch
      //                   +----------- MVT --------------+
      //                   +------- DEAD ---------+       |
      //                   |                      |       |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                   |
      //                   v
      //              valid time
      //
      // NB. GC edge and root are irrelevant in this case. Looking at the DELETE's
      // valid time, the resulting snapshot should always be empty.
      all(
        Seq(
          sliceAt(deleteTS, Timestamp.Epoch),
          sliceAt(deleteTS, createTS),
          sliceAt(deleteTS, createTS.nextMicro),
          sliceAt(deleteTS, updateTS),
          sliceAt(deleteTS, updateTS.nextMicro),
          sliceAt(deleteTS, deleteTS)
        )
      ) shouldBe empty

      // now <--------------------------------------------- epoch
      //        +------------------ MVT ------------------+
      //        +------------------ DEAD -----------+     |
      //        |                                   |     |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //        |
      //        v
      //   valid time
      all(
        Seq(
          sliceAt(recreateTS, Timestamp.Epoch).value,
          sliceAt(recreateTS, createTS).value,
          sliceAt(recreateTS, createTS.nextMicro).value,
          sliceAt(recreateTS, updateTS).value,
          sliceAt(recreateTS, updateTS.nextMicro).value,
          sliceAt(recreateTS, deleteTS).value,
          sliceAt(recreateTS, deleteTS.nextMicro).value,
          sliceAt(recreateTS, recreateTS).value
        )) shouldBe history.recreate
    }
  }

  once("disallows reading document snapshot below MVT") {
    for {
      (scopeID, docID) <- idsP
      validTS          <- Prop.timestamp()
      minValidTS       <- Prop.timestampAfter(validTS)
      snapshotTS       <- Prop.timestampAfter(minValidTS)
    } yield {
      val err =
        the[ReadValidTimeBelowMVT] thrownBy {
          DocSnapshot(
            scopeID,
            docID,
            snapshotTS,
            minValidTS,
            Some(validTS)
          )
        }
      err.validTS shouldBe validTS
      err.mvt shouldBe minValidTS
      err.collID shouldBe docID.collID
    }
  }
  once("skipIO returns None if any non version add write is encountered") {
    for {
      (scopeID, docID) <- idsP
      snapshotTS       <- Prop.timestamp()
    } yield {
      DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            diff = None
          ),
          DocRemove(
            scopeID,
            docID
          )
        )
      ) shouldBe None
      DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            diff = None
          ),
          VersionRemove(
            scopeID,
            docID,
            Unresolved,
            DocAction.Delete
          )
        )
      ) shouldBe None
    }
  }
  test("skipIO returns None if there are multiple matching VersionAdd writes") {
    for {
      (scopeID, docID) <- idsP
      snapshotTS       <- Prop.timestamp()
    } yield {
      DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            diff = None
          ),
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("baz"))),
            diff = None
          )
        )
      ) shouldBe None
    }
  }
  once("skipIO returns None if the only matching VersionAdd has a valid time") {
    for {
      (scopeID, docID) <- idsP
      writeTS          <- Prop.timestamp()
      snapshotTS       <- Prop.timestampAfter(writeTS)
    } yield {
      DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            AtValid(writeTS),
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            diff = Some(Diff(MapV("bar" -> StringV("baz"))))
          )
        )
      ) shouldBe None
    }
  }
  test("skipIO returns None if the DocSnapshot has a valid time") {
    for {
      (scopeID, docID) <- idsP
      writeTS          <- Prop.timestamp()
      snapshotTS       <- Prop.timestampAfter(writeTS)
    } yield {
      DocSnapshot(
        scopeID,
        docID,
        snapshotTS,
        validTS = Some(writeTS)
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            diff = Some(Diff(MapV("bar" -> StringV("baz"))))
          )
        )
      ) shouldBe None
    }
  }
  once("skipIO returns the version corresponding to the found VersionAdd event") {
    for {
      (scopeID, docID) <- idsP
      snapshotTS       <- Prop.timestamp()
    } yield {
      val res = DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            diff = Some(Diff(MapV("bar" -> StringV("baz"))))
          )
        )
      )
      res shouldBe Some(
        DocSnapshot.Result(
          Some(
            StorageVersion.fromDecoded(
              scopeID,
              docID,
              Unresolved,
              DocAction.Create,
              SchemaVersion.Min,
              None,
              Data(MapV("foo" -> StringV("bar"))),
              Some(Diff(MapV("bar" -> StringV("baz"))))
            ))
        )
      )
    }
  }
  once("skipIO fails for VersionAdd event with different scope/doc") {
    for {
      (scopeID, docID)           <- idsP
      (otherScopeID, otherDocID) <- idsP
      snapshotTS                 <- Prop.timestamp()
    } yield {
      the[IllegalArgumentException] thrownBy DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            otherScopeID,
            docID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            Some(Diff(MapV("bar" -> StringV("baz"))))
          )
        )
      )
      the[IllegalArgumentException] thrownBy DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            otherDocID,
            Unresolved,
            DocAction.Create,
            schemaVersion = SchemaVersion.Min,
            Data(MapV("foo" -> StringV("bar"))),
            Some(Diff(MapV("bar" -> StringV("baz"))))
          )
        )
      )
    }
  }
  once(
    "skipIO returns Some(None) for a Version.Add event that has a delete doc action") {
    for {
      (scopeID, docID) <- idsP
      snapshotTS       <- Prop.timestamp()
    } yield {
      val res = DocSnapshot(
        scopeID,
        docID,
        snapshotTS
      ).skipIO(
        Vector(
          VersionAdd(
            scopeID,
            docID,
            Unresolved,
            DocAction.Delete,
            schemaVersion = SchemaVersion.Min,
            Data.empty,
            Some(Diff(MapV("bar" -> StringV("baz"))))
          )
        )
      )

      res shouldBe Some(DocSnapshot.Result(None))
    }
  }
}
