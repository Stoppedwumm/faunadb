package fauna.model.gc.test

import fauna.codex.json._
import fauna.model.test._
import scala.concurrent.duration._

class SetSnapshotSpec extends InlineGCSpec {
  import SocialHelpers._

  "Index" - {
    "snapshot is correct" - {
      "according to snapshot, valid time, and MVT" in withSimpleHistory { _ =>
        // now <------------------------------------------------------------- epoch
        //                                                           snapshotTS
        //                                                             validTS
        //                                                                |    MVT
        //                                                                v     v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(createTS - 1.day)(usersByName("Bob", JSArray.empty))

        // now <------------------------------------------------------------- epoch
        //                                                   snapshotTS
        //                                                        |    validTS
        //                                                        |      MVT
        //                                                        v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(createTS)(At(createTS - 1.day, usersByName("Bob", JSArray.empty)))

        // now <------------------------------------------------------------- epoch
        //                                                   snapshotTS
        //                                                     validTS
        //                                                        |      MVT
        //                                                        v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(createTS)(usersByName("Bob", JSArray(MkObject("name" -> "Bob"))))

        // now <------------------------------------------------------------- epoch
        //                                           snapshotTS
        //                                                |    validTS
        //                                                |      MVT
        //                                                v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(update0TS)(
          At(createTS, usersByName("Bob", JSArray(MkObject("name" -> "Bob")))))

        // now <------------------------------------------------------------- epoch
        //                                           snapshotTS
        //                                             validTS
        //                                                |      MVT
        //                                                v       v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(update0TS)(
          usersByName("Bob", JSArray(MkObject("name" -> "Bob", "age" -> 42))))

        // now <------------------------------------------------------------- epoch
        //                                 snapshotTS
        //                                      |      validTS
        //                                      |        MVT
        //                                      v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(update1TS)(
          At(
            update0TS,
            usersByName("Bob", JSArray(MkObject("name" -> "Bob", "age" -> 42)))
          ))

        // now <------------------------------------------------------------- epoch
        //                                 snapshotTS
        //                                   validTS
        //                                      |        MVT
        //                                      v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(update1TS)(usersByName("Bob", JSArray.empty))
        qassertAt(update1TS)(
          usersByName("Bob L", JSArray(MkObject("name" -> "Bob L", "age" -> 42))))

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
            usersByName("Bob L", JSArray(MkObject("name" -> "Bob L", "age" -> 42)))
          ))

        // now <------------------------------------------------------------- epoch
        //                       snapshotTS
        //                         validTS
        //                            |        MVT
        //                            v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(deleteTS)(usersByName("Bob L", JSArray.empty))

        // now <------------------------------------------------------------- epoch
        //             snapshotTS
        //                  |      validTS
        //                  |        MVT
        //                  v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS)(At(deleteTS, usersByName("Bob L", JSArray.empty)))

        // now <------------------------------------------------------------- epoch
        //             snapshotTS
        //               validTS
        //                  |        MVT
        //                  v         v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS)(
          usersByName("Bob L", JSArray(MkObject("name" -> "Bob L", "age" -> 42))))

        // now <------------------------------------------------------------- epoch
        //    snapshotTS
        //         |     validTS
        //         |       MVT
        //         v        v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS + 1.day)(
          At(
            recreateTS,
            usersByName("Bob L", JSArray(MkObject("name" -> "Bob L", "age" -> 42)))
          ))

        // now <------------------------------------------------------------- epoch
        //    snapshotTS
        //      validTS
        //         |       MVT
        //         v        v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS + 1.day)(
          usersByName("Bob L", JSArray(MkObject("name" -> "Bob L", "age" -> 42))))

        // now <------------------------------------------------------------- epoch
        // snapshotTS
        //  validTS
        //   |    MVT
        //   v     v
        //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
        //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
        qassertAt(recreateTS + 2.days)(
          usersByName("Bob L", JSArray(MkObject("name" -> "Bob L", "age" -> 42))))
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
              usersByName("Bob L", JSArray.empty),
              usersByName(
                "pending",
                JSArray(
                  MkObject("name" -> "pending", "age" -> 42)
                ))
            )
          ))
        }

        "insert" in withSimpleHistory { userRef =>
          // now <----------------------------------------------------------- epoch
          //                                snapshotTS
          //                                     |        MVT
          //                                     v         v
          // ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
          // ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
          //                                           ^
          //                                    PENDING WRITE
          qassertAt(update1TS)(
            Do(
              InsertVers(
                userRef,
                update1TS - 1.hour,
                "create",
                MkData("name" -> "pending")
              ),
              And(
                usersByName(
                  "Bob L",
                  JSArray(
                    MkObject("name" -> "Bob L", "age" -> 42)
                  )),
                At(
                  update1TS - 1.second,
                  usersByName(
                    "pending",
                    JSArray(
                      MkObject("name" -> "pending")
                    ))
                )
              )
            )
          )
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
                usersByName(
                  "Bob L",
                  JSArray(
                    MkObject("name" -> "Bob L", "age" -> 42)
                  )),
                At(
                  deleteTS,
                  usersByName(
                    "Bob L",
                    JSArray(
                      MkObject("name" -> "Bob L", "age" -> 42)
                    ))
                )
              )
            )
          )
        }
      }

      "in the presence of TTLed documents" in withSimpleTTLedHistory { _ =>
        // The set is nonempty.
        // now <---------------------------- epoch
        //                  snapshotTS
        //                   validTS
        //                      |      MVT
        //                      v       v
        //  ... | ... | TTL | ADD | ... | ...
        qassertAt(createTS)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // TTL'd.
        // now <---------------------------- epoch
        //           snapshotTS
        //            validTS
        //               |    MVT
        //               v     v
        //  ... | ... | TTL | ADD | ... | ...
        qassertAt(update0TS)(usersByName("Alice", JSArray.empty))

        // Still TTL'd.
        // now <---------------------------- epoch
        //     snapshotTS
        //      validTS
        //         |    MVT
        //         v     v
        //  ... | ... | TTL | ADD | ... | ...
        qassertAt(update1TS)(usersByName("Alice", JSArray.empty))

        // RIP.
        // now <---------------------------- epoch
        // snapshotTS
        // validTS
        //   |    MVT
        //   v     v
        //  ... | ... | TTL | ADD | ... | ...
        qassertAt(deleteTS)(usersByName("Alice", JSArray.empty))
      }

      "when performing complex set operations" in withComplexSetHistory {

        def countIntersectionQ(name: String, job: String) =
          Count(
            Intersection(Match("users_by_name", name), Match("users_by_job", job)))

        def countDifferenceQ(name: String, job: String) =
          Count(Difference(Match("users_by_name", name), Match("users_by_job", job)))

        // now <----------------------------------------------------------- epoch
        //                          snapshotTS
        //                              |      MVT
        //                              v       v
        // ... | ... | DELETE (Tom) | CREATE | ... | ...
        // ... | ... | REMOVE       |  ADD   | ... | ...
        qassertAt(createTS)(Equals(countIntersectionQ("Sarah", "cook"), 1))
        qassertAt(createTS)(Equals(countDifferenceQ("Tom", "chef"), 1))

        // now <----------------------------------------------------------- epoch
        //           snapshotTS
        //               |             MVT
        //               v              v
        // ... | ... | DELETE (Tom) | CREATE | ... | ...
        // ... | ... | REMOVE       |  ADD   | ... | ...
        qassertAt(deleteTS)(Equals(countIntersectionQ("Sarah", "cook"), 1))
        qassertAt(deleteTS)(Equals(countDifferenceQ("Tom", "chef"), 0))

        // now <----------------------------------------------------------- epoch
        //    snapshotTS
        //        |     MVT
        //        v      v
        // ... | ... | DELETE (Tom) | CREATE | ... | ...
        // ... | ... | REMOVE       |  ADD   | ... | ...
        qassertAt(recreateTS)(Equals(countIntersectionQ("Sarah", "cook"), 1))
        qassertAt(recreateTS)(Equals(countDifferenceQ("Tom", "chef"), 0))

        // now <----------------------------------------------------------- epoch
        // snapshotTS
        //  |    MVT
        //  v     v
        // ... | ... | DELETE (Tom) | CREATE | ... | ...
        // ... | ... | REMOVE       |  ADD   | ... | ...
        qassertAt(recreateTS + 1.day)(Equals(countIntersectionQ("Sarah", "cook"), 1))
        qassertAt(recreateTS + 1.day)(Equals(countDifferenceQ("Tom", "chef"), 0))
      }

      "when updating TTLs" in withSimpleTTLedHistory { userRef =>
        writeAt(
          update0TS - 1.hour, // 1h before its orinal TTL
          Update(
            userRef,
            MkObject("ttl" -> TS(update1TS.toString))
          ))

        writeAt(
          update1TS - 1.hour, // 1h before its new TTL
          Update(
            userRef,
            MkObject("ttl" -> TS((update1TS + 1.day).toString))
          ))

        // now <-------------------------------------------------------------- epoch
        //                                                      snapshotTS
        //                                                           |    MVT
        //                                                           v     v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(createTS - 1.day)(usersByName("Alice", JSArray.empty))

        // now <-------------------------------------------------------------- epoch
        //                                                snapshotTS
        //                                                     |    MVT
        //                                                     v     v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(createTS)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // now <-------------------------------------------------------------- epoch
        //                                   snapshotTS
        //                                        |    MVT
        //                                        v     v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(update0TS - 1.hour)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // now <-------------------------------------------------------------- epoch
        //                           snapshotTS
        //                                |             MVT
        //                                v              v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(update0TS)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // now <-------------------------------------------------------------- epoch
        //                    snapshotTS
        //                         |        MVT
        //                         v         v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(update1TS - 1.hour)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // now <-------------------------------------------------------------- epoch
        //          snapshotTS
        //               |            MVT
        //               v             v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(update1TS)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // now <-------------------------------------------------------------- epoch
        //    snapshotTS
        //         |        MVT
        //         v         v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(update1TS + 1.day)(usersByName("Alice", JSArray.empty))

        // now <-------------------------------------------------------------- epoch
        // snapshotTS
        // |MVT
        // v v
        // ... | TTL2 | TTL1 |  ADD+RM | TT0 | ADD+RM | ... | ADD | ... | ...
        qassertAt(update1TS + 3.days)(usersByName("Alice", JSArray.empty))
      }

      "when updating + deleting TTLed docs" in withSimpleTTLedHistory { userRef =>
        writeAt(
          update0TS - 1.hour, // 1h before the original ttl
          Do(
            Update(userRef, MkObject("ttl" -> TS((update0TS + 2.days).toString))),
            DeleteF(userRef)
          ))

        // now <---------------------------------------------------- epoch
        //                                           snapshotTS
        //                                                |    MVT
        //                                                v     v
        // ...  | ... | TTL | ... | ADD+RM | ... | ADD | ... | ...
        qassertAt(createTS - 1.day)(usersByName("Alice", JSArray.empty))

        // now <---------------------------------------------------- epoch
        //                                     snapshotTS
        //                                          |    MVT
        //                                          v     v
        // ...  | ... | TTL | ... | ADD+RM | ... | ADD | ... | ...
        qassertAt(createTS)(
          usersByName("Alice", JSArray(MkObject("name" -> "Alice"))))

        // now <---------------------------------------------------- epoch
        //                        snapshotTS
        //                             |     MVT
        //                             v      v
        // ...  | ... | TTL | ... | ADD+RM | ... | ADD | ... | ...
        qassertAt(update0TS)(usersByName("Alice", JSArray.empty))

        // now <---------------------------------------------------- epoch
        //                 snapshotTS
        //                     |     MVT
        //                     v      v
        // ...  | ... | TTL | ... | ADD+RM | ... | ADD | ... | ...
        qassertAt(update0TS + 1.day)(usersByName("Alice", JSArray.empty))

        // now <---------------------------------------------------- epoch
        // snapshotTS
        //  |     MVT
        //  v      v
        // ...  | ... | TTL | ... | ADD+RM | ... | ADD | ... | ...
        qassertAt(update0TS + 3.days)(usersByName("Alice", JSArray.empty))
      }
    }
  }
}
