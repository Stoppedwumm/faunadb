package fauna.storage.api.set.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.prop._
import fauna.storage.{ Add, Order, Remove, Resolved, SetAction }
import fauna.storage.api.set.{ Element, SetHistory }
import fauna.storage.api.test._
import fauna.storage.index.IndexTerm
import fauna.storage.ir.IRValue
import fauna.storage.ops.SetAdd

class SetHistorySpec extends Spec("sethistory") {

  Seq(Order.Descending, Order.Ascending) map { order =>
    storageProp(s"reads the history of a set (+conflicts) (order=$order)") {
      engine =>
        for {
          (scopeID, docID) <- idsP
          indexID          <- indexP

          ts0 <- Prop.timestamp()
          ts1 <- Prop.timestampAfter(ts0)
          ts2 <- Prop.timestampAfter(ts1)
          ts3 <- Prop.timestampAfter(ts2)
        } yield {
          def write(ts: Timestamp, action: SetAction) =
            applyWrites(
              engine,
              ts,
              SetAdd(
                scopeID,
                indexID,
                doc = docID,
                terms = Vector("foo"),
                values = Vector.empty,
                writeTS = Resolved(ts),
                action = action
              )
            )

          write(ts0, Add)
          write(ts1, Remove); write(ts1, Add) // Remove wins in conflict resolution.
          write(ts2, Add)
          write(ts3, Remove)

          def read(maxTS: Timestamp, minTS: Timestamp) =
            runSetHistory(
              engine,
              SetHistory(
                scopeID,
                indexID,
                Vector("foo"),
                snapshotTS = maxTS,
                maxTS = maxTS,
                minTS = minTS,
                order = order
              ))

          {
            val (elems, next) = read(ts2, ts0)
            elems shouldBe reverseIfDescending(
              order,
              Vector(
                Element.Live(scopeID, docID, Vector(), Some(ts0)),
                Element.Deleted(scopeID, docID, Vector(), Some(ts1))
              )
            )
            next shouldBe empty
          }
        }
    }

    storageProp(s"merges pending writes (order=$order)") { engine =>
      for {
        (scopeID, docID) <- idsP
        indexID          <- indexP

        ts0 <- Prop.timestamp()
        ts1 <- Prop.timestampAfter(ts0)
        ts2 <- Prop.timestampAfter(ts1)
        ts3 <- Prop.timestampAfter(ts2)
      } yield {
        def mkwrite(ts: Timestamp, action: SetAction) =
          SetAdd(
            scopeID,
            indexID,
            doc = docID,
            terms = Vector("foo"),
            values = Vector.empty,
            writeTS = Resolved(ts),
            action = action
          )

        applyWrites(engine, ts1, mkwrite(ts1, Remove))

        def read(maxTS: Timestamp, minTS: Timestamp) =
          runSetHistory(
            engine,
            SetHistory(
              scopeID,
              indexID,
              Vector("foo"),
              snapshotTS = maxTS,
              maxTS = maxTS,
              minTS = minTS,
              order = order
            ),
            Seq(
              mkwrite(ts0, Add),
              mkwrite(ts2, Add)
            ))

        {
          val (elems, next) = read(ts3, ts0)
          elems shouldBe reverseIfDescending(
            order,
            Vector(
              Element.Live(scopeID, docID, Vector(), Some(ts0)),
              Element.Deleted(scopeID, docID, Vector(), Some(ts1)),
              Element.Live(scopeID, docID, Vector(), Some(ts2))
            )
          )
          next shouldBe empty
        }
      }
    }

    // See DocHistorySpec for an explanation of these numbers.
    Seq(11, 207) foreach { n =>
      Seq(1, 3, n, n + 2) foreach { p =>
        storageProp(s"paginates the history of a set (n=$n, p=$p) (order=$order)") {
          engine =>
            for {
              (scopeID, _) <- idsP
              indexID      <- indexP

              baseTS  <- Prop.timestamp()
              bagOfTS <- Prop.timestampAfter(baseTS) * n
            } yield {
              def doc(i: Int) = DocID(SubID(i), CollectionID(1024))

              def write(i: Int, ts: Timestamp) =
                applyWrites(
                  engine,
                  ts,
                  SetAdd(
                    scopeID,
                    indexID,
                    doc = doc(i),
                    terms = Vector("foo"),
                    values = Vector.empty,
                    writeTS = Resolved(ts),
                    action = Add
                  )
                )

              val timestamps = bagOfTS.sorted
              timestamps.zipWithIndex foreach { case (ts, i) => write(i, ts) }

              val latestTS = timestamps.last
              var remaining = n
              var op =
                SetHistory(
                  scopeID,
                  indexID,
                  Vector("foo"),
                  snapshotTS = latestTS,
                  order = order,
                  maxResults = p
                )

              while (remaining > 0) {
                val (versions, next) = runSetHistory(engine, op)
                versions.length should be >= 0
                versions.length should be <= p
                versions map { v =>
                  v shouldBe (order match {
                    case Order.Ascending =>
                      Element.Live(
                        scopeID,
                        doc(n - remaining),
                        Vector.empty,
                        Some(timestamps(n - remaining)))
                    case Order.Descending =>
                      Element.Live(
                        scopeID,
                        doc(remaining - 1),
                        Vector.empty,
                        Some(timestamps(remaining - 1)))
                  })
                  remaining -= 1
                  remaining should be >= 0
                }
                if (remaining > 0) {
                  next should not be empty
                  op = next.get
                } else {
                  // One more read may be required to verify that there are no more
                  // results between the last result returned and the minimum TS.
                  if (!next.isEmpty) {
                    val (noElems, noNext) = runSetHistory(engine, next.get)
                    noElems.isEmpty shouldBe true
                    noNext.isEmpty shouldBe true
                  }
                }
              }
            }
        }
      }
    }
  }

  storageProp("apply inline GC rules") { engine =>
    for {
      (scopeID, docID) <- idsP
      indexID          <- indexP
      addA             <- Prop.timestamp()
      addB             <- Prop.timestampAfter(addA)
      removeA          <- Prop.timestampAfter(addB)
      removeB          <- Prop.timestampAfter(removeA)
      snapshotTS       <- Prop.timestampAfter(removeB)
      order            <- Prop.choose(Seq(Order.Ascending, Order.Descending))
    } yield withClue(s"(order=$order)") {
      def write(validTS: Timestamp, action: SetAction, value: IRValue) =
        applyWrites(
          engine,
          validTS,
          SetAdd(
            scopeID,
            indexID,
            doc = docID,
            terms = Vector("foo"),
            values = Vector(IndexTerm(value)),
            writeTS = Resolved(validTS),
            action = action
          )
        )

      write(addA, Add, "a")
      write(addB, Add, "b")
      write(removeA, Remove, "a")
      write(removeB, Remove, "b")

      def readAt(minValidTS: Timestamp) =
        runSetHistory(
          engine,
          SetHistory(
            scopeID,
            indexID,
            Vector("foo"),
            snapshotTS,
            Map(docID.collID -> minValidTS),
            order = order
          )
        )._1

      def liveAt(value: IRValue, validTS: Timestamp) =
        Element.Live(scopeID, docID, Vector(IndexTerm(value)), Some(validTS))

      def deletedAt(value: IRValue, validTS: Timestamp) =
        Element.Deleted(scopeID, docID, Vector(IndexTerm(value)), Some(validTS))

      //  now <------------------------------------------------- epoch
      //                                          +-----MVT------+
      //                                          |              |
      //  ...  | REMOVE(b) | REMOVE(a) | ADD(b) | ADD(a) | ... |
      all(
        Seq(
          readAt(Timestamp.Epoch),
          readAt(addA)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            liveAt("a", addA),
            liveAt("b", addB),
            deletedAt("a", removeA),
            deletedAt("b", removeB)
          )
        )

      //  now <------------------------------------------------- epoch
      //                                 +--MVT-+
      //                                 |      |
      //  ...  | REMOVE(b) | REMOVE(a) | ADD(b) | ADD(a) | ... |
      all(
        Seq(
          readAt(addA.nextMicro),
          readAt(addB)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            liveAt("b", addB),
            deletedAt("a", removeA),
            deletedAt("b", removeB)
          )
        )

      //  now <------------------------------------------------- epoch
      //                              MVT
      //                               ^
      //  ...  | REMOVE(b) | REMOVE(a) | ADD(b) | ADD(a) | ... |
      readAt(addB.nextMicro) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            deletedAt("a", removeA),
            deletedAt("b", removeB)
          )
        )

      //  now <------------------------------------------------- epoch
      //                   MVT
      //                   ^ ^
      //  ...  | REMOVE(b) | REMOVE(a) | ADD(b) | ADD(a) | ... |
      all(
        Seq(
          readAt(removeA),
          readAt(removeA.nextMicro)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            deletedAt("b", removeB)
          )
        )

      //  now <------------------------------------------------- epoch
      //       MVT
      //       ^ ^
      //  ...  | REMOVE(b) | REMOVE(a) | ADD(b) | ADD(a) | ... |
      all(
        Seq(
          readAt(removeB),
          readAt(removeB.nextMicro)
        )) shouldBe empty
    }
  }
}
