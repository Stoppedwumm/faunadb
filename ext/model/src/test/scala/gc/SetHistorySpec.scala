package fauna.model.gc.test

import fauna.codex.json._
import fauna.model.test._
import fauna.model.Collection
import scala.concurrent.duration._

class SetHistorySpec extends InlineGCSpec {
  import SocialHelpers._

  "Index" - {
    "history is correct" - {
      "according to snapshot, valid time, and MVT" in withSimpleHistory { _ =>
        // now <------------------------------------------------------------- epoch
        //                                                           snapshotTS
        //                                                             validTS
        //                                                                |    MVT
        //                                                                v     v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(createTS - 1.day)(pairwiseHistory("Bob", JSArray.empty))

        // now <------------------------------------------------------------- epoch
        //                                                   snapshotTS
        //                                                        |    validTS
        //                                                        |      MVT
        //                                                        v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(createTS)(
          At(createTS - 1.day, pairwiseHistory("Bob", JSArray.empty)))

        // now <------------------------------------------------------------- epoch
        //                                                   snapshotTS
        //                                                     validTS
        //                                                        |      MVT
        //                                                        v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(createTS)(
          pairwiseHistory(
            "Bob",
            JSArray(
              JSArray("add", MkObject("name" -> "Bob"))
            )))

        // now <------------------------------------------------------------- epoch
        //                                           snapshotTS
        //                                                |    validTS
        //                                                |      MVT
        //                                                v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(update0TS)(
          At(
            createTS,
            pairwiseHistory(
              "Bob",
              JSArray(
                JSArray("add", MkObject("name" -> "Bob"))
              ))
          ))

        // now <------------------------------------------------------------- epoch
        //                                           snapshotTS
        //                                             validTS
        //                                                |      MVT
        //                                                v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        //
        // The first update didn't change the indexed term `name`, thus the set
        // history contains only a single ADD event from which the doc snapshot at
        // the same valid time is still the CREATE with name "Bob".
        qassertAt(update0TS)(
          pairwiseHistory(
            "Bob",
            JSArray(
              JSArray("add", MkObject("name" -> "Bob"))
            )))

        // now <------------------------------------------------------------- epoch
        //                                 snapshotTS
        //                                      |      validTS
        //                                      |        MVT
        //                                      v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        //
        // Note that set history is truncated at MVT, therefore with both valid time
        // and MVT at the first update, the system returns an empty event set since
        // the first ADD was truncated away and the ADD|REMOVE pair introduced by the
        // second update is ahead of the query's valid time.
        qassertAt(update1TS)(At(update0TS, pairwiseHistory("Bob", JSArray.empty)))

        // now <------------------------------------------------------------- epoch
        //                                 snapshotTS
        //                                   validTS
        //                                      |        MVT
        //                                      v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        //
        // With the first ADD truncated already, looking for the term "Bob" at the
        // same valid time as the second update returns a REMOVE event which signals
        // that this update has changed the indexed term `name`. The document
        // snapshot at the same valid time as the REMOVE shows a version of the
        // document without the matched term "Bob". In other words, it no longer has
        // the information regarding the inclusion of "Bob" in the set but, it still
        // contains the information about its removal.
        qassertAt(update1TS)(
          pairwiseHistory(
            "Bob",
            JSArray(
              JSArray("remove", MkObject("name" -> "Bob L", "age" -> 42))
            )))
        qassertAt(update1TS)(
          pairwiseHistory(
            "Bob L",
            JSArray(
              JSArray("add", MkObject("name" -> "Bob L", "age" -> 42))
            )))

        // now <------------------------------------------------------------- epoch
        //                       snapshotTS
        //                            |      validTS
        //                            |        MVT
        //                            v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(deleteTS)(
          At(
            update1TS,
            pairwiseHistory(
              "Bob L",
              JSArray(
                JSArray("add", MkObject("name" -> "Bob L", "age" -> 42))
              ))
          ))
        // Note REMOVEs are dropped at MVT, therefore, the REMOVE event representing
        // the term "Bob" leaving the set was dropped at this point.
        qassertAt(deleteTS + Collection.MVTOffset)(
          At(
            update1TS,
            pairwiseHistory("Bob", JSArray.empty)
          ))

        // now <------------------------------------------------------------- epoch
        //                       snapshotTS
        //                         validTS
        //                            |        MVT
        //                            v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        //
        // The term "Bob L" contains two events: an ADD from which the doc snapshot
        // at the same valid time contains the update that changed the doc's name
        // from "Bob" to "Bob L"; and a REMOVE where the doc snapshot at the same
        // valid time is empty.
        qassertAt(deleteTS + Collection.MVTOffset)(
          pairwiseHistory(
            "Bob L",
            JSArray(
              JSArray("add", MkObject("name" -> "Bob L", "age" -> 42)),
              JSArray("remove", JSNull)
            )))
        // No more events are retained for the term "Bob".
        qassertAt(deleteTS + Collection.MVTOffset)(
          pairwiseHistory("Bob", JSArray.empty))

        // now <------------------------------------------------------------- epoch
        //             snapshotTS
        //                  |      validTS
        //                  |        MVT
        //                  v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        //
        // Note that REMOVEs are tuncated at MVT, thus the event set for the term
        // "Bob L" is empty at this point.
        qassertAt(recreateTS + Collection.MVTOffset)(
          At(
            deleteTS,
            pairwiseHistory("Bob L", JSArray.empty)
          ))

        // now <------------------------------------------------------------- epoch
        //             snapshotTS
        //               validTS
        //                  |        MVT
        //                  v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS + Collection.MVTOffset)(
          pairwiseHistory(
            "Bob L",
            JSArray(
              JSArray("add", MkObject("name" -> "Bob L", "age" -> 42))
            )))

        // now <------------------------------------------------------------- epoch
        //     snapshotTS
        //         |     validTS
        //         |       MVT
        //         v        v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS + 1.day)(
          At(
            recreateTS,
            pairwiseHistory(
              "Bob L",
              JSArray(
                JSArray("add", MkObject("name" -> "Bob L", "age" -> 42))
              ))
          ))

        // now <------------------------------------------------------------- epoch
        //     snapshotTS
        //       validTS
        //         |       MVT
        //         v        v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS + 1.day)(
          pairwiseHistory(
            "Bob L",
            JSArray(
              JSArray("add", MkObject("name" -> "Bob L", "age" -> 42))
            )))

        // now <------------------------------------------------------------- epoch
        // snapshotTS
        //  validTS
        //   |    MVT
        //   v     v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        //
        // Note that set history is truncated at MVT. Hence, once MVT has moved
        // beyond the last ADD event, the result set becomes empty.
        qassertAt(recreateTS + 2.days)(pairwiseHistory("Bob L", JSArray.empty))
      }

      "in the presence of pending writes" - {
        "create/update" in withSimpleHistory { userRef =>
          // now <----------------------------------------------------------- epoch
          // snapshotTS
          //  |    MVT
          //  v     v
          // ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
          // ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
          //  ^
          // PENDING WRITE
          qassertAt(recreateTS + 2.days)(Do(
            Update(userRef, MkData("name" -> "pending")),
            And(
              pairwiseHistory(
                "Bob L",
                JSArray(
                  JSArray("remove", MkObject("name" -> "pending", "age" -> 42))
                )),
              pairwiseHistory(
                "pending",
                JSArray(
                  JSArray("add", MkObject("name" -> "pending", "age" -> 42))
                )),
              At(recreateTS, pairwiseHistory("Bob L", JSArray.empty)),
              At(recreateTS, pairwiseHistory("pendign", JSArray.empty))
            )
          ))
        }

        "insert" in withSimpleHistory { userRef =>
          // now <----------------------------------------------------------- epoch
          //                               snapshotTS
          //                                    |         MVT
          //                                    v          v
          // ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
          // ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
          //                                          ^
          //                                   PENDING WRITE
          qassertAt(update1TS)(Do(
            InsertVers(
              userRef,
              update1TS - 1.hour,
              "create",
              MkData(
                "name" -> "pending",
                "age" -> 42
              )),
            And(
              pairwiseHistory(
                "Bob L",
                JSArray(
                  JSArray("add", MkObject("name" -> "Bob L", "age" -> 42))
                )),
              pairwiseHistory(
                "pending",
                JSArray(
                  JSArray("add", MkObject("name" -> "pending", "age" -> 42)),
                  JSArray("remove", MkObject("name" -> "Bob L", "age" -> 42))
                )),
              At(
                update1TS - 1.hour,
                pairwiseHistory("Bob L", JSArray.empty)
              ),
              At(
                update1TS - 1.hour,
                pairwiseHistory(
                  "pending",
                  JSArray(
                    JSArray("add", MkObject("name" -> "pending", "age" -> 42))
                  ))
              )
            )
          ))
        }

        "remove" in withSimpleHistory { userRef =>
          // now <----------------------------------------------------------- epoch
          //                 snapshotTS
          //                      |       MVT
          //                      v        v
          // ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
          // ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
          //                          ^
          //                   PENDING WRITE
          qassertAt(deleteTS + 1.hour)(
            Do(
              RemoveVers(userRef, deleteTS, "delete"),
              And(
                pairwiseHistory("Bob L", JSArray.empty),
                At(deleteTS, pairwiseHistory("Bob L", JSArray.empty))
              )))
        }

        "in the presence of TTLed documents" in withSimpleTTLedHistory { _ =>
          // Because functions like IsEmpty and Exists check a snapshot, they
          // don't return a correct result when run on Events(set) if the set
          // has all TTL'd documents...
          val nonEmptyQ = Equals(Count(Events(Match("users_by_name", "Alice"))), 1)
          val emptyQ = Equals(Count(Events(Match("users_by_name", "Alice"))), 0)

          // The set is nonempty.
          // now <---------------------------- epoch
          //                 snapshotTS
          //                  validTS
          //                     |    MVT
          //                     v     v
          //  ... | ... | TTL | ADD | ... | ...
          qassertAt(createTS)(
            pairwiseHistory(
              "Alice",
              JSArray(
                JSArray("add", MkObject("name" -> "Alice"))
              )))

          // Still nonempty (but pairwiseHistory fails because the doc is TTL'd).
          // now <---------------------------- epoch
          //           snapshotTS
          //            validTS
          //               |    MVT
          //               v     v
          //  ... | ... | TTL | ADD | ... | ...
          qassertAt(update0TS)(nonEmptyQ)

          // No more history: truncated at MVT.
          // now <---------------------------- epoch
          //     snapshotTS
          //      validTS
          //         |    MVT
          //         v     v
          //  ... | ... | TTL | ADD | ... | ...
          qassertAt(update1TS)(emptyQ)

          // Nothing beside remains.
          // now <---------------------------- epoch
          // snapshotTS
          // validTS
          //   |    MVT
          //   v     v
          //  ... | ... | TTL | ADD | ... | ...
          qassertAt(deleteTS)(emptyQ)
        }

        "when performing complex set operations" in withComplexSetHistory {

          def countIntersectionEventsQ(name: String, job: String) =
            Count(
              Events(
                Intersection(
                  Match("users_by_name", name),
                  Match("users_by_job", job))))

          def countDifferenceEventsQ(name: String, job: String) =
            Count(Events(
              Difference(Match("users_by_name", name), Match("users_by_job", job))))

          // now <----------------------------------------------------------- epoch
          //                          snapshotTS
          //                              |      MVT
          //                              v       v
          // ... | ... | DELETE (Tom) | CREATE | ... | ...
          // ... | ... | REMOVE       |  ADD   | ... | ...
          qassertAt(createTS)(Equals(countIntersectionEventsQ("Sarah", "cook"), 1))
          qassertAt(createTS)(Equals(countDifferenceEventsQ("Tom", "chef"), 3))

          // now <----------------------------------------------------------- epoch
          //           snapshotTS
          //               |             MVT
          //               v              v
          // ... | ... | DELETE (Tom) | CREATE | ... | ...
          // ... | ... | REMOVE       |  ADD   | ... | ...
          qassertAt(deleteTS)(Equals(countIntersectionEventsQ("Sarah", "cook"), 1))
          qassertAt(deleteTS)(Equals(countDifferenceEventsQ("Tom", "chef"), 4))

          // now <----------------------------------------------------------- epoch
          //    snapshotTS
          //        |     MVT
          //        v      v
          // ... | ... | DELETE (Tom) | CREATE | ... | ...
          // ... | ... | REMOVE       |  ADD   | ... | ...
          qassertAt(recreateTS)(Equals(countIntersectionEventsQ("Sarah", "cook"), 1))
          qassertAt(recreateTS)(Equals(countDifferenceEventsQ("Tom", "chef"), 4))

          // now <----------------------------------------------------------- epoch
          // snapshotTS
          //  |    MVT
          //  v     v
          // ... | ... | DELETE (Tom) | CREATE | ... | ...
          // ... | ... | REMOVE       |  ADD   | ... | ...
          qassertAt(recreateTS + 1.day)(
            Equals(countIntersectionEventsQ("Sarah", "cook"), 1))
          qassertAt(recreateTS + 1.day)(
            Equals(countDifferenceEventsQ("Tom", "chef"), 2))
        }
      }
    }
  }
}
