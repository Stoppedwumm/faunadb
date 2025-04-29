package fauna.storage.api.set.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.prop._
import fauna.storage._
import fauna.storage.api._
import fauna.storage.api.set._
import fauna.storage.api.test._
import fauna.storage.index._
import fauna.storage.ir.IRValue
import fauna.storage.ops.{ SetAdd, Write }

class SetSortedSpec extends Spec("setsorted") {
  Seq(Order.Descending, Order.Ascending) map { order =>
    storageProp(s"reads an ordered set and a set snapshot (order=$order)") {
      engine =>
        for {
          (scopeID, docID) <- idsP
          indexID          <- indexP

          ts0 <- Prop.timestamp()
          ts1 <- Prop.timestampAfter(ts0)
          ts2 <- Prop.timestampAfter(ts1)
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
          write(ts1, Remove)
          write(ts2, Add)

          def read(ts: Timestamp) = {
            val snapRes = runSetSnapshot(
              engine,
              SetSnapshot(
                scopeID,
                indexID,
                terms = Vector("foo"),
                cursor = SetSnapshot.Cursor(order = order),
                validTS = None,
                snapshotTS = ts
              ))

            val sortedRes = runSetSortedValues(
              engine,
              SetSortedValues(
                scopeID,
                indexID,
                terms = Vector("foo"),
                cursor = SetSortedValues.Cursor(order = order),
                snapshotTS = ts
              ))

            (snapRes, sortedRes)
          }

          {
            val ((snap, nextSnap), (_, _)) = read(ts0)
            snap should have size (1)
            nextSnap shouldBe empty
          }
          {
            val ((snap, nextSnap), (_, _)) = read(ts1)
            snap shouldBe empty
            nextSnap shouldBe empty

          }
          {
            val ((snap, nextSnap), (elems, nextElems)) = read(ts2)
            snap should have size (1)
            elems should have size (3)
            nextSnap shouldBe empty
            nextElems shouldBe empty
          }
        }
    }

    storageProp(
      s"reads an ordered set and a set snapshot with padding (order=$order)") {
      engine =>
        for {
          (scopeID, docID) <- idsP
          indexID          <- indexP

          ts0 <- Prop.timestamp()
          ts1 <- Prop.timestampAfter(ts0)
          ts2 <- Prop.timestampAfter(ts1)
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
                values = Vector(IndexTerm("foo")),
                writeTS = Resolved(ts),
                action = action
              )
            )

          write(ts0, Add)
          write(ts1, Remove)
          write(ts2, Add)

          val tuple = IndexTuple(scopeID, docID)
          val max = IndexValue.MaxValue.withTuple(tuple)
          val min = IndexValue.MinValue

          val (max0, min0) = SetPadding.pad(max, min, Seq(false))
          val snapCursor = SetSnapshot.Cursor(max0.tuple, min0.tuple, order)
          val svCursor = SetSortedValues.Cursor(max0, min0, order)

          def read(ts: Timestamp) = {
            val snapRes = runSetSnapshot(
              engine,
              SetSnapshot(
                scopeID,
                indexID,
                terms = Vector("foo"),
                cursor = snapCursor,
                validTS = None,
                snapshotTS = ts
              ))

            val sortedRes = runSetSortedValues(
              engine,
              SetSortedValues(
                scopeID,
                indexID,
                terms = Vector("foo"),
                cursor = svCursor,
                snapshotTS = ts
              ))

            (snapRes, sortedRes)
          }

          {
            val ((snap, nextSnap), (_, _)) = read(ts0)
            snap should have size (1)
            nextSnap shouldBe empty
          }
          {
            val ((snap, nextSnap), (_, _)) = read(ts1)
            snap shouldBe empty
            nextSnap shouldBe empty
          }
          {
            val ((snap, nextSnap), (elems, nextElems)) = read(ts2)
            snap should have size (1)
            elems should have size (3)
            nextSnap shouldBe empty
            nextElems shouldBe empty
          }
        }
    }

    storageProp(s"resolves conflicts (order=$order)") { engine =>
      for {
        (scopeID, docID) <- idsP
        indexID          <- indexP

        ts0 <- Prop.timestamp()
        ts1 <- Prop.timestampAfter(ts0)
      } yield {
        def write(validTS: Timestamp, txnTS: Timestamp, action: SetAction) =
          applyWrites(
            engine,
            txnTS,
            SetAdd(
              scopeID,
              indexID,
              doc = docID,
              terms = Vector("foo"),
              values = Vector.empty,
              writeTS = Resolved(validTS, txnTS),
              action = action
            )
          )

        write(ts0, ts0, Remove)
        write(ts0, ts1, Add)

        def read(ts: Timestamp) = {
          val snapRes = runSetSnapshot(
            engine,
            SetSnapshot(
              scopeID,
              indexID,
              terms = Vector("foo"),
              cursor = SetSnapshot.Cursor(order = order),
              validTS = None,
              snapshotTS = ts
            ))

          val sortedRes = runSetSortedValues(
            engine,
            SetSortedValues(
              scopeID,
              indexID,
              terms = Vector("foo"),
              cursor = SetSortedValues.Cursor(order = order),
              snapshotTS = ts
            ))

          (snapRes, sortedRes)
        }

        val ((snap, snapNext), (elems, elemsNext)) = read(ts1)
        elems should have size (1)
        elemsNext shouldBe empty
        snap should have size (1)
        snapNext shouldBe empty
      }
    }

    storageProp(s"snapshot respects TTLs (order=$order)") { engine =>
      for {
        (scopeID, docID) <- idsP
        indexID          <- indexP

        ts0 <- Prop.timestamp()
        ts1 <- Prop.timestampAfter(ts0)
        ts2 <- Prop.timestampAfter(ts1)
      } yield {
        def write(ts: Timestamp, action: SetAction, ttl: Option[Timestamp]) =
          applyWrites(
            engine,
            ts,
            SetAdd(
              scopeID,
              indexID,
              terms = Vector("foo"),
              values = Vector.empty,
              doc = docID,
              writeTS = Resolved(ts),
              action = action,
              ttl
            )
          )

        write(ts0, Add, None)
        write(ts1, Remove, Some(ts1))
        write(ts2, Add, Some(ts2))

        def read(ts: Timestamp) =
          runSetSnapshot(
            engine,
            SetSnapshot(
              scopeID,
              indexID,
              terms = Vector("foo"),
              cursor = SetSnapshot.Cursor(order = order),
              validTS = None,
              snapshotTS = ts
            ))

        {
          val (snap, snapNext) = read(ts0)
          snap should have size (1)
          snapNext shouldBe empty
        }
        {
          val (snap, snapNext) = read(ts1)
          snap shouldBe empty
          snapNext shouldBe empty
        }
        {
          val (snap, snapNext) = read(ts2)
          snap shouldBe empty
          snapNext shouldBe empty
        }
      }
    }

    storageProp(s"merges pending writes (order=$order)") { engine =>
      for {
        (scopeID, docID) <- idsP
        indexID          <- indexP

        ts0 <- Prop.timestamp()
      } yield {
        def write(ts: Timestamp, action: SetAction) =
          applyWrites(
            engine,
            ts,
            SetAdd(
              scopeID,
              indexID,
              terms = Vector("foo"),
              values = Vector.empty,
              doc = docID,
              writeTS = Resolved(ts),
              action = action
            )
          )

        write(ts0, Add)

        def read(ts: Timestamp, writes: Seq[Write]) = {
          val snapRes = runSetSnapshot(
            engine,
            SetSnapshot(
              scopeID,
              indexID,
              terms = Vector("foo"),
              cursor = SetSnapshot.Cursor(order = order),
              validTS = None,
              snapshotTS = ts
            ),
            writes)

          val sortedRes = runSetSortedValues(
            engine,
            SetSortedValues(
              scopeID,
              indexID,
              terms = Vector("foo"),
              cursor = SetSortedValues.Cursor(order = order),
              snapshotTS = ts
            ),
            writes)

          (snapRes, sortedRes)
        }

        {
          val ((snap, snapNext), (elems, elemsNext)) = read(ts0, Seq.empty)
          elems should have size (1)
          elemsNext shouldBe empty
          snap should have size (1)
          snapNext shouldBe empty
        }
        {
          val ((snap, snapNext), (elems, elemsNext)) = read(
            ts0,
            Seq(
              SetAdd(
                scopeID,
                indexID,
                Vector("foo"),
                Vector.empty,
                doc = docID,
                writeTS = Unresolved,
                action = Remove)))
          elems should have size (2)
          elemsNext shouldBe empty
          snap shouldBe empty
          snapNext shouldBe empty
        }
      }
    }

    Seq(11, 207) foreach { n =>
      Seq(1, 3, n, n + 2) foreach { p =>
        storageProp(s"paginates $n results with $p-result pages (order=$order)") {
          engine =>
            for {
              (scopeID, _) <- idsP
              indexID      <- indexP
              baseTS       <- Prop.timestamp()
              bagOfTS      <- Prop.timestampAfter(baseTS).times(n)
            } yield {
              val timestamps = bagOfTS.sorted

              def mkDocID(i: Int) = DocID(SubID(i), CollectionID(0))

              def write(i: Int, ts: Timestamp) =
                applyWrites(
                  engine,
                  ts,
                  SetAdd(
                    scopeID,
                    indexID,
                    terms = Vector("foo"),
                    values = Vector(),
                    doc = mkDocID(i + 1),
                    writeTS = Resolved(ts),
                    action = Add,
                    None
                  )
                )

              timestamps.zipWithIndex foreach { case (ts, i) =>
                write(i, ts)
              }

              val latestTS = timestamps.last
              var remaining = n
              var op =
                SetSnapshot(
                  scopeID,
                  indexID,
                  Vector("foo"),
                  SetSnapshot.Cursor(order = order),
                  latestTS,
                  maxResults = p
                )
              while (remaining > 0) {
                val (elems, next) = runSetSnapshot(engine, op)
                elems.length shouldBe >(0)
                elems.length shouldBe <=(p)
                elems map { v =>
                  v shouldBe (order match {
                    case Order.Ascending =>
                      Element.Live(
                        scopeID,
                        mkDocID(n + 1 - remaining),
                        Vector(),
                        Some(timestamps(n - remaining)))
                    case Order.Descending =>
                      Element.Live(
                        scopeID,
                        mkDocID(remaining),
                        Vector(),
                        Some(timestamps(remaining - 1)))
                  })
                  remaining -= 1
                  remaining shouldBe >=(0)
                }
                if (remaining > 0) {
                  next.nonEmpty shouldBe true
                  op = next.get
                } else {
                  // One more read may be required to verify that there are no more
                  // results.
                  if (!next.isEmpty) {
                    val (noVersions, noNext) = runSetSnapshot(engine, next.get)
                    noVersions.isEmpty shouldBe true
                    noNext.isEmpty shouldBe true
                  }
                }
              }
            }
        }
      }
    }
  }

  storageProp(s"handles predicates correctly") { engine =>
    for {
      (scopeID, docID) <- idsP
      indexID          <- indexP

      ts0 <- Prop.timestamp()
      ts1 <- Prop.timestampAfter(ts0)
      ts2 <- Prop.timestampAfter(ts1)
    } yield {
      def write(ts: Timestamp, action: SetAction, vs: String*) = {
        def w(v: String) = SetAdd(
          scopeID,
          indexID,
          doc = docID,
          terms = Vector("letter"),
          values = Vector(IndexTerm(v)),
          writeTS = Resolved(ts),
          action = action
        )
        applyWrites(
          engine,
          ts,
          (vs map w): _*
        )
      }

      write(ts0, Add, "a", "b", "c", "z")
      write(ts1, Remove, "a", "z")
      write(ts2, Add, "a", "d", "e")

      def read(ts: Timestamp, max: IndexTuple, min: IndexTuple, order: Order) = {
        val (max0, min0) = SetPadding.pad(max, min, Seq(false))
        val cursor = SetSnapshot.Cursor(max0, min0, order)

        runSetSnapshot(
          engine,
          SetSnapshot(
            scopeID,
            indexID,
            terms = Vector("letter"),
            cursor = cursor,
            validTS = None,
            snapshotTS = ts
          )
        )
      }

      def actual(elems: Vector[Element.Live]) = Vector.concat(elems.map {
        _.values.head
      })
      def exp(vs: String*) = vs map { IndexTerm(_) }

      {
        val (elems, next) =
          read(
            ts2,
            IndexTuple.MaxValue,
            IndexTuple.MinValue.copy(values = Vector(IndexTerm("b"))),
            Order.Ascending
          )
        actual(elems) should equal(exp("b", "c", "d", "e"))
        next shouldBe empty
      }

      {
        val (elems, next) =
          read(
            ts2,
            IndexTuple.MaxValue,
            IndexTuple.MinValue.copy(values = Vector(IndexTerm("c"))),
            Order.Descending
          )
        actual(elems) should equal(exp("e", "d", "c"))
        next shouldBe empty
      }

      {
        val (elems, next) = read(
          ts2,
          IndexTuple.MaxValue.copy(values = Vector(IndexTerm("b"))),
          IndexTuple.MinValue.copy(values = Vector(IndexTerm("b"))),
          Order.Ascending
        )
        actual(elems) should equal(exp("b"))
        next shouldBe empty
      }

      {
        val (elems, next) = read(
          ts2,
          IndexTuple.MaxValue.copy(values = Vector(IndexTerm("d"))),
          IndexTuple.MinValue.copy(values = Vector(IndexTerm("c"))),
          Order.Descending
        )
        actual(elems) should equal(exp("d", "c"))
        next shouldBe empty
      }
    }
  }

  storageProp(s"sparse snapshots work") { engine =>
    for {
      (scopeID, _) <- idsP
      indexID      <- indexP

      ts0 <- Prop.timestamp()
      ts1 <- Prop.timestampAfter(ts0)
    } yield {
      def write(id: Long, ts: Timestamp, action: SetAction, vs: String*) = {
        def w(v: String) = SetAdd(
          scopeID,
          indexID,
          doc = DocID(SubID(id), CollectionID(1024)),
          terms = Vector("letter"),
          values = Vector(IndexTerm(v)),
          writeTS = Resolved(ts),
          action = action
        )
        applyWrites(
          engine,
          ts,
          (vs map w): _*
        )
      }

      write(0, ts0, Add, "a", "b", "c", "z")
      write(0, ts1, Remove, "b", "c")
      write(1, ts0, Add, "a", "b", "c", "z")
      write(1, ts1, Remove, "a", "z")

      def mkDocID(id: Long) = DocID(SubID(id), CollectionID(1024))

      def read(ts: Timestamp, order: Order, locs: Vector[IndexValue]) =
        runSparseSetSnapshot(
          engine,
          SparseSetSnapshot(
            scopeID,
            indexID,
            terms = Vector("letter"),
            locs map { ell => (ell, ell) },
            validTS = None,
            snapshotTS = ts,
            order = order
          )
        )

      def mkIV(id: Long, value: String) = IndexValue(
        IndexTuple(scopeID, mkDocID(id), Vector(IndexTerm(value))),
        Unresolved,
        Remove)

      def check(actual: Vector[Element.Live], exp: Vector[(Long, String)]) =
        actual map { e =>
          (e.docID, e.values)
        } should contain theSameElementsInOrderAs (exp map { case (id, t) =>
          (mkDocID(id), Vector(IndexTerm(t)))
        })

      {
        // ts0.
        check(read(ts0, Order.Ascending, Vector(mkIV(0, "a"))), Vector((0, "a")))
        check(
          read(ts0, Order.Ascending, Vector(mkIV(0, "a"), mkIV(0, "b"))),
          Vector((0, "a"), (0, "b")))
        check(
          read(ts0, Order.Descending, Vector(mkIV(0, "a"), mkIV(0, "z"))),
          Vector((0, "z"), (0, "a")))
        check(
          read(
            ts0,
            Order.Ascending,
            Vector(mkIV(0, "a"), mkIV(0, "z"), mkIV(0, "b"), mkIV(1, "c"))),
          Vector((0, "a"), (0, "b"), (1, "c"), (0, "z")))
        check(
          read(ts0, Order.Descending, Vector(mkIV(1, "d"), mkIV(0, "y"))),
          Vector())

        // ts1.
        check(read(ts1, Order.Ascending, Vector(mkIV(0, "a"))), Vector((0, "a")))
        check(
          read(ts1, Order.Ascending, Vector(mkIV(0, "a"), mkIV(0, "b"))),
          Vector((0, "a")))
        check(
          read(
            ts1,
            Order.Ascending,
            Vector(mkIV(0, "a"), mkIV(0, "z"), mkIV(0, "b"), mkIV(1, "c"))),
          Vector((0, "a"), (1, "c"), (0, "z")))
      }
    }
  }

  storageProp("apply inline GC rules to set snapshot") { engine =>
    for {
      (scopeID, docID) <- idsP
      indexID          <- indexP
      add              <- Prop.timestamp()
      afterAdd         <- Prop.timestampAfter(add)
      remove           <- Prop.timestampAfter(afterAdd)
      afterRemove      <- Prop.timestampAfter(remove)
      readd            <- Prop.timestampAfter(afterRemove)
      afterReadd       <- Prop.timestampAfter(readd)
      snapshotTS       <- Prop.timestampAfter(afterReadd)
      order            <- Prop.choose(Order.Ascending, Order.Descending)
    } yield withClue(s"(order=$order)") {
      def write(ts: Timestamp, action: SetAction, value: IRValue) =
        applyWrites(
          engine,
          ts,
          SetAdd(
            scopeID,
            indexID,
            doc = docID,
            terms = Vector("foo"),
            values = Vector(IndexTerm(value)),
            writeTS = Resolved(ts),
            action = action
          )
        )

      write(add, Add, "a")
      write(add, Add, "b")
      write(remove, Remove, "a")
      write(remove, Remove, "b")
      write(readd, Add, "a")
      write(readd, Add, "b")

      def readAt(validTS: Timestamp, minValidTS: Timestamp) = {
        val (max, min) = (IndexTuple.MaxValue, IndexTuple.MinValue)
        val (max0, min0) = SetPadding.pad(max, min, Seq(false))
        val cursor = SetSnapshot.Cursor(max0, min0, order)

        runSetSnapshot(
          engine,
          SetSnapshot(
            scopeID,
            indexID,
            Vector("foo"),
            cursor,
            snapshotTS,
            Map(docID.collID -> minValidTS),
            Some(validTS)
          )
        )._1
      }

      def liveAt(ts: Timestamp, value: IRValue) =
        Element.Live(scopeID, docID, Vector(IndexTerm(value)), Some(ts))

      //  now <--------------------------------------- epoch
      //                                         validTS & MVT
      //                                                 ^
      //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
      //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
      readAt(Timestamp.Epoch, Timestamp.Epoch) shouldBe empty

      all(
        Seq(
          //  now <--------------------------------------- epoch
          //                               validTS          MVT
          //                                  ^              ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(add, Timestamp.Epoch),
          //  now <--------------------------------------- epoch
          //                           validTS              MVT
          //                              ^                  ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterAdd, Timestamp.Epoch),
          //  now <--------------------------------------- epoch
          //                        validTS & MVT
          //                                ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(add, add),
          //  now <--------------------------------------- epoch
          //                        validTS MVT
          //                              ^ ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterAdd, add),
          //  now <--------------------------------------- epoch
          //                      validTS & MVT
          //                              ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterAdd, afterAdd)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            liveAt(add, "a"),
            liveAt(add, "b")
          ))

      all(
        Seq(
          //  now <--------------------------------------- epoch
          //                    validTS                     MVT
          //                       ^                         ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(remove, Timestamp.Epoch),
          //  now <--------------------------------------- epoch
          //               validTS                          MVT
          //                  ^                              ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterRemove, Timestamp.Epoch),
          //  now <--------------------------------------- epoch
          //               validTS & MVT
          //                       ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(remove, remove),
          //  now <--------------------------------------- epoch
          //            validTS MVT
          //                  ^ ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterRemove, remove),
          //  now <--------------------------------------- epoch
          //          validTS & MVT
          //                  ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterRemove, afterRemove)
        )
      ) shouldBe empty

      all(
        Seq(
          //  now <--------------------------------------- epoch
          //        validTS                                 MVT
          //           ^                                     ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(readd, Timestamp.Epoch),
          //  now <--------------------------------------- epoch
          //    validTS                                     MVT
          //       ^                                         ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterReadd, Timestamp.Epoch),
          //  now <--------------------------------------- epoch
          //   validTS & MVT
          //           ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(readd, readd),
          //  now <--------------------------------------- epoch
          // validTS MVT
          //       ^ ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterReadd, readd),
          //  now <--------------------------------------- epoch
          // validTS & MVT
          //       ^
          //  ...  | READD(a) | REMOVE(a) | ADD(a) | ... |
          //  ...  | READD(b) | REMOVE(b) | ADD(b) | ... |
          readAt(afterReadd, afterReadd)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            liveAt(readd, "a"),
            liveAt(readd, "b")
          ))
    }
  }

  once("disallow reading set snapshot below MVT") {
    for {
      (scopeID, docID) <- idsP
      indexID          <- indexP
      validTS          <- Prop.timestamp()
      minValidTS       <- Prop.timestampAfter(validTS)
      snapshotTS       <- Prop.timestampAfter(minValidTS)
    } yield {
      val err =
        the[ReadValidTimeBelowMVT] thrownBy {
          SetSnapshot(
            scopeID,
            indexID,
            Vector.empty,
            SetSnapshot.Cursor(),
            snapshotTS,
            Map(docID.collID -> minValidTS),
            Some(validTS)
          )
        }
      err.validTS shouldBe validTS
      err.mvt shouldBe minValidTS
      err.collID shouldBe docID.collID
    }
  }

  storageProp("apply inline GC rules to set sorted values") { engine =>
    for {
      (scopeID, docID) <- idsP
      indexID          <- indexP
      add              <- Prop.timestamp()
      afterAdd         <- Prop.timestampAfter(add)
      remove           <- Prop.timestampAfter(afterAdd)
      afterRemove      <- Prop.timestampAfter(remove)
      readd            <- Prop.timestampAfter(afterRemove)
      afterReadd       <- Prop.timestampAfter(readd)
      snapshotTS       <- Prop.timestampAfter(afterReadd)
      order            <- Prop.choose(Order.Ascending, Order.Descending)
    } yield withClue(s"(order=$order)") {
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

      write(add, Add)
      write(remove, Remove)
      write(readd, Add)

      def readAt(minValidTS: Timestamp, max: IndexValue = IndexValue.MaxValue) =
        runSetSortedValues(
          engine,
          SetSortedValues(
            scopeID,
            indexID,
            Vector("foo"),
            SetSortedValues.Cursor(max, order = order),
            snapshotTS,
            Map(docID.collID -> minValidTS)
          )
        )._1

      def liveAt(ts: Timestamp) =
        Element.Live(scopeID, docID, Vector.empty, Some(ts))

      def deletedAt(ts: Timestamp) =
        Element.Deleted(scopeID, docID, Vector.empty, Some(ts))

      //  now <-------------------------------- epoch
      //                        +--- MVT----+
      //                        |           |
      //  ...  | READD | REMOVE | ADD | ... |
      all(
        Seq(
          readAt(Timestamp.Epoch),
          readAt(add),
          readAt(afterAdd)
        )
      ) should contain theSameElementsInOrderAs
        reverseIfDescending(
          order,
          Seq(
            liveAt(add),
            deletedAt(remove),
            liveAt(readd)
          ))

      //  now <-------------------------------- epoch
      //       +-- MVT --+
      //       |         |
      //  ...  | READD | REMOVE | ADD | ... |
      all(
        Seq(
          readAt(remove),
          readAt(afterRemove),
          readAt(readd),
          readAt(afterReadd)
        )
      ) should contain only liveAt(readd)

      //  now <-------------------------------- epoch
      //                      MVT +- read --+
      //                        ^ |         |
      //  ...  | READD | REMOVE | ADD | ... |
      readAt(
        afterAdd,
        max = IndexValue(
          IndexTuple(scopeID, docID),
          AtValid(add),
          Add
        )
      ) should contain only liveAt(add)

      //  now <-------------------------------- epoch
      //                MVT     +-- read ---+
      //                 ^      |           |
      //  ...  | READD | REMOVE | ADD | ... |
      readAt(
        remove,
        max = IndexValue(
          IndexTuple(scopeID, docID),
          AtValid(add),
          Add
        )
      ) shouldBe empty
    }
  }

  storageProp("omit GC root if TTLed") { engine =>
    for {
      (scopeID, docID) <- idsP
      indexID          <- indexP
      writeTS          <- Prop.timestamp()
      beforeTTL        <- Prop.timestampAfter(writeTS)
      ttl              <- Prop.timestampAfter(beforeTTL)
      afterTTL         <- Prop.timestampAfter(ttl)
      snapshotTS       <- Prop.timestampAfter(afterTTL)
      order            <- Prop.choose(Order.Ascending, Order.Descending)

      _ = applyWrites(
        engine,
        writeTS,
        SetAdd(
          scopeID,
          indexID,
          doc = docID,
          terms = Vector.empty,
          values = Vector.empty,
          writeTS = AtValid(writeTS),
          action = Add,
          ttl = Some(ttl)
        )
      )
    } yield {
      def readAt(minValidTS: Timestamp) =
        runSetSortedValues(
          engine,
          SetSortedValues(
            scopeID,
            indexID,
            Vector.empty,
            SetSortedValues.Cursor(order = order),
            snapshotTS,
            Map(docID.collID -> minValidTS)
          )
        )._1

      readAt(beforeTTL) should not be empty
      readAt(ttl) should not be empty
      readAt(afterTTL) shouldBe empty
    }
  }
}
