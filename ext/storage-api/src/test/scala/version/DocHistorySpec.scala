package fauna.storage.api.version.test

import fauna.atoms._
import fauna.lang._
import fauna.prop._
import fauna.storage._
import fauna.storage.api.test._
import fauna.storage.api.version.DocHistory
import fauna.storage.doc._
import fauna.storage.ops._
import scala.util.Random

class DocHistorySpec extends Spec("dochistory") {

  // A "quick" version for when the scope ID and docID are constant.
  sealed trait QV

  object QV {
    case class Live(validTS: Timestamp, data: Data, diff: Option[Diff] = None)
        extends QV

    case class Deleted(validTS: Timestamp, diff: Option[Diff] = None) extends QV

    def toVersions(s: ScopeID, d: DocID, qvs: Vector[QV]): Vector[TestVersion] =
      qvs map { qv =>
        qv match {
          case Live(vts, data, diff) =>
            TestVersion.Live(s, d, Some(vts), None, data, diff)
          case Deleted(vts, diff) => TestVersion.Deleted(s, d, Some(vts), diff)
        }
      }
  }

  // Accumulates all the results of running `op` and its follow-up operations.
  // Asserts on the size of each page.
  def unfold(engine: StorageEngine, op: DocHistory): Vector[TestVersion] = {
    var curr = Option(op)
    val b = Vector.newBuilder[TestVersion]
    while (curr.nonEmpty) {
      val (versions, next) = runDocHistory(engine, curr.get)
      versions.size should be <= op.maxResults
      b ++= versions
      curr = next
    }
    b.result()
  }

  Seq(Order.Ascending, Order.Descending).foreach { order =>
    storageProp(s"read a document's history (order=$order)") { engine =>
      for {
        (scopeID, docID) <- idsP
        ts0              <- Prop.timestamp()
        ts1              <- Prop.timestampAfter(ts0)
        ts2              <- Prop.timestampAfter(ts1)
        ts3              <- Prop.timestampAfter(ts2)
        ts4              <- Prop.timestampAfter(ts3)
      } yield {
        createBasicHistory(engine, scopeID, docID, ts1, ts2, ts3, ts4)

        // These are parallel.
        val ts = Vector(ts0, ts1, ts2, ts3, ts4)
        val vs = Vector(
          None,
          Some(QV.Live(ts1, createData, None)),
          Some(QV.Live(ts2, updateData, Some(Diff(createData.fields)))),
          Some(QV.Deleted(ts3, Some(Diff(updateData.fields)))),
          Some(QV.Live(ts4, recreateData, None))
        )

        val mkhistory = docHistoryFactory(scopeID, docID, order = order)(_, _)

        (0 until ts.size) foreach { left =>
          (left until ts.size) foreach { right =>
            val x = unfold(engine, mkhistory(ts(right), ts(left)))
            val y = reverseIfDescending(
              order,
              QV.toVersions(scopeID, docID, vs.slice(left, right + 1).flatten))
            withClue(s"left=$left, right=$right") {
              x shouldBe y
            }
          }
        }
      }
    }

    storageProp(s"resolves conflicts correctly (within a page) (order=$order)") {
      engine =>
        for {
          (scopeID, docID) <- idsP
          // For txn-time-ordered conflict resolution, use timestamps after the
          // change epoch. See Version.scala.
          changeEpoch <- Prop.const(Timestamp.parse("2019-06-01T00:00:00Z"))
          firstTS     <- Prop.timestampAfter(changeEpoch)
          secondTS    <- Prop.timestampAfter(firstTS)
        } yield {
          val write = writeFactory(engine, scopeID, docID)

          // Two versions in conflict with a the second one winning.
          write(firstTS, firstTS, Create, createData, None)
          write(firstTS, secondTS, Create, updateData, None)
          write(secondTS, secondTS, Create, updateData, None)

          val (versions, next) =
            runDocHistory(
              engine,
              DocHistory(
                scopeID,
                docID,
                secondTS,
                firstTS,
                secondTS,
                order = order
              ))

          versions shouldBe reverseIfDescending(
            order,
            QV.toVersions(
              scopeID,
              docID,
              Vector(
                QV.Live(firstTS, updateData),
                QV.Live(secondTS, updateData)
              ))
          )
          next shouldBe None
        }
    }

    // 11 is small and prime, so only page sizes of 1 and 11 will fit evenly into it.
    // 207 is larger and divisible by 3, so a page size of 3 will fit evenly while
    // not being a page size of 1 or a single-page read.
    Seq(11, 207) foreach { n =>
      // 1-result pages to test that corner case and for maximum pagination.
      // 3-result pages to test a page size larger than 1 that doesn't fit evenly
      // into the total result size.
      // n-result pages to test the corner case of the result set being exactly one
      // page.
      // (n + 2)-result pages to test the case that the whole result set fits
      // strictly inside one page.
      Seq(1, 3, n, n + 2) foreach { p =>
        storageProp(s"paginates $n results with $p-result pages (order=$order)") {
          engine =>
            for {
              (scopeID, docID) <- idsP
              baseTS           <- Prop.timestamp()
              bagOfTS          <- Prop.timestampAfter(baseTS) * n
            } yield {
              val timestamps = bagOfTS.sorted
              val latestTS = timestamps.last

              val write = writeFactory(engine, scopeID, docID)

              def mkData(i: Int): Data = Data(Field[String]("version") -> s"v$i")

              timestamps.zipWithIndex foreach { case (ts, i) =>
                write(ts, ts, Create, mkData(i + 1), None)
              }

              unfold(
                engine,
                DocHistory(
                  scopeID,
                  docID,
                  latestTS,
                  baseTS,
                  latestTS,
                  p,
                  order)) shouldBe reverseIfDescending(
                order,
                QV.toVersions(
                  scopeID,
                  docID,
                  (1 to n).map { i =>
                    QV.Live(timestamps(i - 1), mkData(i))
                  }.toVector))
            }
        }
      }
    }

    storageProp(s"paginates with conflicts correctly (order=$order)") { engine =>
      for {
        (scopeID, docID) <- idsP
        // For txn-time-ordered conflict resolution, use timestamps after the change
        // epoch. See Version.scala.
        changeEpoch <- Prop.const(Timestamp.parse("2019-06-01T00:00:00Z"))
        firstTS     <- Prop.timestampAfter(changeEpoch)
        secondTS    <- Prop.timestampAfter(firstTS)
        thirdTS     <- Prop.timestampAfter(secondTS)
        fourthTS    <- Prop.timestampAfter(thirdTS)
      } yield {
        val write = writeFactory(engine, scopeID, docID)

        def mkData(i: Int): Data = Data(Field[String]("version") -> s"v$i")

        // Version outside the range of the read.
        write(changeEpoch, changeEpoch, Create, mkData(0), None)

        // Two conflicts at the first time. The last one, at the latest
        // transaction timestamp, is the canonical version.
        write(firstTS, firstTS, Create, mkData(1), None)
        write(firstTS, secondTS, Create, mkData(2), None)

        // Two conflicts at the second timestamp. The delete is the canonical
        // version.
        write(secondTS, secondTS, Create, mkData(11), None)
        write(secondTS, thirdTS, Create, mkData(12), None)

        // Three conflicts at the third.
        write(thirdTS, thirdTS, Create, mkData(111), None)
        write(thirdTS, fourthTS, Delete, mkData(112), None)
        write(thirdTS, fourthTS, Create, mkData(113), None)

        val results = QV.toVersions(
          scopeID,
          docID,
          Vector(
            QV.Live(firstTS, mkData(2)),
            QV.Live(secondTS, mkData(12)),
            QV.Deleted(thirdTS)
          ))

        // Read all the versions in one page (default page size is 64).
        unfold(
          engine,
          DocHistory(
            scopeID,
            docID,
            fourthTS,
            firstTS,
            fourthTS,
            order = order)) shouldBe reverseIfDescending(order, results)

        // Read all the versions over two pages.
        unfold(
          engine,
          DocHistory(
            scopeID,
            docID,
            fourthTS,
            firstTS,
            fourthTS,
            order = order,
            maxResults = 2)) shouldBe reverseIfDescending(order, results)
      }
    }
  }

  storageProp("merge additive write intents upon reading (incl. reversed)") {
    engine =>
      for {
        (scopeID, docID) <- idsP
        beforeWriteTS    <- Prop.timestamp()
        writeTS          <- Prop.timestampAfter(beforeWriteTS)
        afterWriteTS     <- Prop.timestampAfter(writeTS)
        snapshotTS       <- Prop.timestampAfter(afterWriteTS)

        data0 = Data(Field[String]("version") -> "v0")
        data1 = Data(Field[String]("version") -> "v1")

        _ = applyWrites(
          engine,
          writeTS,
          VersionAdd(
            scopeID,
            docID,
            AtValid(writeTS),
            Create,
            SchemaVersion.Min,
            data0,
            None
          ))
      } yield {
        val beforeWrite = VersionAdd(
          scopeID,
          docID,
          AtValid(beforeWriteTS),
          Create,
          SchemaVersion.Min,
          data1,
          None
        )

        // A pending write before the stored version shows up.
        {
          val read = DocHistory(scopeID, docID, writeTS, beforeWriteTS, snapshotTS)
          val (versions, next) = runDocHistory(
            engine,
            read,
            Seq(beforeWrite)
          )
          versions.size shouldBe 2
          versions shouldBe Seq(
            TestVersion.Live(scopeID, docID, Some(writeTS), None, data0, None),
            TestVersion.Live(scopeID, docID, Some(beforeWriteTS), None, data1, None)
          )
          next shouldBe None
        }

        // A pending write will cause pagination, and can be the only
        // result on its page.
        {
          val page1 =
            DocHistory(scopeID, docID, writeTS, beforeWriteTS, snapshotTS, 1)
          val (versions1, next1) = runDocHistory(
            engine,
            page1,
            Seq(beforeWrite)
          )
          versions1.size shouldBe 1
          versions1 shouldBe Seq(
            TestVersion.Live(scopeID, docID, Some(writeTS), None, data0, None)
          )
          next1.isEmpty shouldBe false

          val (versions2, next2) = runDocHistory(
            engine,
            next1.get,
            Seq(beforeWrite)
          )
          versions2.size shouldBe 1
          versions2 shouldBe Seq(
            TestVersion.Live(scopeID, docID, Some(beforeWriteTS), None, data1, None)
          )
          next2 shouldBe None
        }

        val afterWrite = VersionAdd(
          scopeID,
          docID,
          AtValid(afterWriteTS),
          Delete,
          SchemaVersion.Min,
          Data.empty,
          None
        )

        // A pending write after the stored versions shows up, in either order.
        Seq(Order.Descending, Order.Ascending) foreach { order =>
          val read =
            DocHistory(
              scopeID,
              docID,
              afterWriteTS,
              beforeWriteTS,
              snapshotTS,
              order = order)
          val (versions, next) = runDocHistory(
            engine,
            read,
            Seq(afterWrite)
          )
          versions.size shouldBe 2
          versions shouldBe reverseIfDescending(
            order,
            Vector(
              TestVersion.Live(scopeID, docID, Some(writeTS), None, data0, None),
              TestVersion.Deleted(scopeID, docID, Some(afterWriteTS), None)
            )
          )
          next shouldBe None
        }

        // A pending write with an unresolved timestamp shows up.
        {
          val read =
            DocHistory(
              scopeID,
              docID,
              Timestamp.MaxMicros,
              Timestamp.Epoch,
              snapshotTS)
          val (versions, next) = runDocHistory(
            engine,
            read,
            Seq(
              VersionAdd(
                scopeID,
                docID,
                Unresolved,
                Create,
                SchemaVersion.Min,
                Data.empty,
                None))
          )
          versions.size shouldBe 2
          versions shouldBe Seq(
            TestVersion.Live(scopeID, docID, None, None, Data.empty, None),
            TestVersion.Live(scopeID, docID, Some(writeTS), None, data0, None)
          )
          next shouldBe None
        }
      }
  }

  storageProp("merge destructive write intents upon reading (incl. reversed)") {
    engine =>
      for {
        (scopeID, docID) <- idsP
        vs0TS            <- Prop.timestamp()
        vs1TS            <- Prop.timestampAfter(vs0TS)
        vs2TS            <- Prop.timestampAfter(vs1TS)
        writeTS          <- Prop.timestampAfter(vs2TS)
        snapshotTS       <- Prop.timestampAfter(writeTS)

        read = DocHistory(scopeID, docID, writeTS, vs0TS, snapshotTS)
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
        // Remove v2, returns v0 and v1.
        {
          val (versions, next) = runDocHistory(
            engine,
            read,
            Seq(VersionRemove(scopeID, docID, Resolved(vs2TS, writeTS), Create))
          )
          versions shouldBe Seq(
            TestVersion.Live(scopeID, docID, Some(vs1TS), None, data1, None),
            TestVersion.Live(scopeID, docID, Some(vs0TS), None, data0, None)
          )
          next shouldBe None
        }

        // Remove v0, returns v1 and v2, in either order.
        Seq(Order.Descending, Order.Ascending) foreach { order =>
          val read =
            DocHistory(scopeID, docID, writeTS, vs0TS, snapshotTS, order = order)
          val (versions, next) = runDocHistory(
            engine,
            read,
            Seq(VersionRemove(scopeID, docID, Resolved(vs0TS, writeTS), Create))
          )
          versions shouldBe reverseIfDescending(
            order,
            Vector(
              TestVersion.Live(scopeID, docID, Some(vs1TS), None, data1, None),
              TestVersion.Live(scopeID, docID, Some(vs2TS), None, data2, None)
            )
          )
          next shouldBe None
        }

        // Remove v1, returns v0 and v2.
        {
          val (versions, next) = runDocHistory(
            engine,
            DocHistory(scopeID, docID, writeTS, vs0TS, snapshotTS),
            Seq(VersionRemove(scopeID, docID, Resolved(vs1TS, writeTS), Create))
          )
          versions shouldBe Seq(
            TestVersion.Live(scopeID, docID, Some(vs2TS), None, data2, None),
            TestVersion.Live(scopeID, docID, Some(vs0TS), None, data0, None)
          )
          next shouldBe None
        }

        // Remove all versions, returns none.
        {
          val (versions, next) = runDocHistory(
            engine,
            read,
            Random.shuffle(
              Seq(
                VersionRemove(scopeID, docID, Resolved(vs0TS, writeTS), Create),
                VersionRemove(scopeID, docID, Resolved(vs1TS, writeTS), Create),
                VersionRemove(scopeID, docID, Resolved(vs2TS, writeTS), Create)
              ))
          )
          versions.isEmpty shouldBe true
          next shouldBe None
        }
      }
  }

  storageProp("filter out expired versions") { engine =>
    for {
      (scopeID, docID) <- idsP
      createTS         <- Prop.timestamp()
      updateTS         <- Prop.timestampAfter(createTS)
      deleteTS         <- Prop.timestampAfter(updateTS)
      recreateTS       <- Prop.timestampAfter(deleteTS)
      snapshotTS       <- Prop.timestampAfter(recreateTS)
      order            <- Prop.choose(Order.Ascending, Order.Descending)

      history = createBasicHistory(
        engine,
        scopeID,
        docID,
        createTS,
        updateTS,
        deleteTS,
        recreateTS
      )
    } yield withClue(s"(order=$order)") {
      def sliceAt(minValidTS: Timestamp) =
        unfold(
          engine,
          DocHistory(
            scopeID,
            docID,
            order = order,
            maxTS = Timestamp.MaxMicros,
            minTS = Timestamp.Epoch,
            snapshotTS = snapshotTS,
            minValidTS = minValidTS
          ))

      // now <--------------------------------------------- epoch
      //                                   +------MVT-----+
      //                                   |              |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                                     |
      //                                     v
      //                                GC edge & root
      all(
        Seq(
          sliceAt(Timestamp.Epoch),
          sliceAt(createTS),
          sliceAt(createTS.nextMicro)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            history.create, // GC root as the first live version in the doc's history
            history.update,
            history.delete,
            history.recreate
          ))

      // now <--------------------------------------------- epoch
      //                        +MVT+      +- DEAD -+
      //                        |   |      |        |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                            |
      //                            v
      //                       GC edge & root
      all(
        Seq(
          sliceAt(updateTS),
          sliceAt(updateTS.nextMicro),
          sliceAt(deleteTS.prevMicro)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            history.update.copy(diff = None), // swaps GC root at the edge of MVT
            history.delete,
            history.recreate
          ))

      // now <--------------------------------------------- epoch
      //                  MVT
      //                   ^
      //                   +-------- DEAD ----------+
      //                   |                        |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //        |          |
      //        v          v
      //     GC root    GC edge
      sliceAt(deleteTS) should contain only
        history.recreate // swaps root; drop deletes at the edge

      // now <--------------------------------------------- epoch
      //        +-- MVT -+ +-------- DEAD ----------+
      //        |        | |                        |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //        |
      //        v
      //     GC edge & root
      all(
        Seq(
          sliceAt(deleteTS.nextMicro),
          sliceAt(recreateTS)
        )
      ) should contain only history.recreate // GC root remains the RECREATE
    }
  }

  storageProp("slicing history preserves GC root") { engine =>
    for {
      (scopeID, docID) <- idsP
      createTS         <- Prop.timestamp()
      afterCreate      <- Prop.timestampAfter(createTS)
      updateTS         <- Prop.timestampAfter(afterCreate)
      afterUpdate      <- Prop.timestampAfter(updateTS)
      deleteTS         <- Prop.timestampAfter(afterUpdate)
      recreateTS       <- Prop.timestampAfter(deleteTS)
      snapshotTS       <- Prop.timestampAfter(recreateTS)
      order            <- Prop.choose(Order.Ascending, Order.Descending)

      history = createBasicHistory(
        engine,
        scopeID,
        docID,
        createTS,
        updateTS,
        deleteTS,
        recreateTS
      )
    } yield withClue(s"(order=$order)") {
      def sliceAt(maxTS: Timestamp, minTS: Timestamp, mvt: Timestamp) =
        unfold(
          engine,
          DocHistory(
            scopeID,
            docID,
            order = order,
            maxTS = maxTS,
            minTS = minTS,
            minValidTS = mvt,
            snapshotTS = snapshotTS
          ))

      // now <--------------------------------------------- epoch
      //                           MVT     +---- read ----+
      //                            ^      |              |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                            v
      //                    GC edge & root
      sliceAt(
        maxTS = afterCreate,
        minTS = Timestamp.Epoch,
        mvt = updateTS
      ) shouldBe empty

      // now <--------------------------------------------- epoch
      //                          +--------- read --------+
      //                          |MVT                    |
      //                          | ^                     |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                            v
      //                    GC edge & root
      sliceAt(
        maxTS = afterUpdate,
        minTS = Timestamp.Epoch,
        mvt = updateTS
      ) should contain only history.update.copy(diff = None)

      // now <--------------------------------------------- epoch
      //                          +--------- read --------+
      //                  MVT     |                       |
      //                   ^      |                       |
      // ...  | RECREATE | DELETE | UPDATE | CREATE | ... |
      //                   v
      //           GC edge & root
      sliceAt(
        maxTS = afterUpdate,
        minTS = Timestamp.Epoch,
        mvt = deleteTS
      ) shouldBe empty
    }
  }

  storageProp("omit GC root if TTLed") { engine =>
    for {
      (scopeID, docID) <- idsP
      writeTS          <- Prop.timestamp()
      beforeTTL        <- Prop.timestampAfter(writeTS)
      ttl              <- Prop.timestampAfter(beforeTTL)
      afterTTL         <- Prop.timestampAfter(ttl)
      snapshotTS       <- Prop.timestampAfter(afterTTL)
      order            <- Prop.choose(Order.Ascending, Order.Descending)

      _ = applyWrites(
        engine,
        writeTS,
        VersionAdd(
          scope = scopeID,
          id = docID,
          writeTS = AtValid(writeTS),
          action = Create,
          schemaVersion = SchemaVersion.Min,
          data = Data(Field[Option[Timestamp]]("ttl") -> Some(ttl)),
          diff = None
        )
      )
    } yield {
      def historyAt(mvt: Timestamp) =
        unfold(
          engine,
          DocHistory(
            scopeID,
            docID,
            order = order,
            maxTS = Timestamp.MaxMicros,
            minTS = Timestamp.Epoch,
            minValidTS = mvt,
            snapshotTS = snapshotTS
          ))

      historyAt(beforeTTL) should not be empty
      historyAt(ttl) should not be empty
      historyAt(afterTTL) shouldBe empty
    }
  }

  storageProp("skips IO when looking for latest version") { engine =>
    for {
      (scopeID, docID) <- idsP
      writeTS          <- Prop.timestamp()
      snapshotTS       <- Prop.timestampAfter(writeTS)

      // Existing doc history.
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
        )
      )
    } yield {
      val pendingWrites =
        Vector(
          VersionAdd(
            scope = scopeID,
            id = docID,
            writeTS = Unresolved,
            action = Create,
            schemaVersion = SchemaVersion.Min,
            data = Data.empty,
            diff = None
          ))

      val latestVersionReadOp =
        DocHistory(
          scopeID,
          docID,
          order = Order.Descending,
          maxTS = Timestamp.MaxMicros,
          minTS = Timestamp.Epoch,
          minValidTS = Timestamp.Epoch,
          snapshotTS = snapshotTS,
          maxResults = 1
        )

      val result = latestVersionReadOp.skipIO(pendingWrites).value
      result.versions should have size 1
      result.versions.head.ts shouldBe Unresolved

      // Returned cursor forces a read if used
      val cursor = result.next.value
      cursor.skipIO(pendingWrites) shouldBe empty

      // Reading from cursor gets in-disk data
      val rest = unfold(engine, cursor)
      rest should have size 1
      rest.head.validTS.value shouldBe writeTS
    }
  }
}
