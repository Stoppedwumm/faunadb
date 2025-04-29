package fauna.model.gc.test

import fauna.codex.json._
import fauna.model.test._
import fauna.model.Collection
import scala.concurrent.duration._

class DocHistorySpec extends InlineGCSpec {
  import SocialHelpers._

  "Document" - {
    "history is correct" - {
      "according to snapshot, valid time, and MVT" in withSimpleHistory { userRef =>
        // now <--------------------------------------------------------------- epoch
        //                                                        snapshotTS
        //                                                          validTS
        //                                                              |    MVT
        //                                                              v     v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(createTS - 1.day)(userHistory(userRef, JSArray.empty))

        // now <--------------------------------------------------------------- epoch
        //                                               snapshotTS
        //                                                    |     validTS
        //                                                    |        MVT
        //                                                    v         v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(createTS)(
          At(createTS - 1.day, userHistory(userRef, JSArray.empty)))

        // now <--------------------------------------------------------------- epoch
        //                                                  snapshotTS
        //                                                    validTS
        //                                                       |     MVT
        //                                                       v      v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(createTS)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob"))
            )))

        // now <--------------------------------------------------------------- epoch
        //                                        snapshotTS
        //                                             |      validTS
        //                                             |        MVT
        //                                             v         v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update0TS)(
          At(
            createTS,
            userHistory(
              userRef,
              JSArray(
                JSArray("create", MkObject("name" -> "Bob"))
              ))
          ))

        // now <--------------------------------------------------------------- epoch
        //                                        snapshotTS
        //                                          validTS
        //                                             |        MVT
        //                                             v         v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update0TS)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob")),
              JSArray("update", MkObject("age" -> 42))
            )))

        // now <--------------------------------------------------------------- epoch
        //                                snapshotTS
        //                                     |    validTS
        //                                     |      MVT
        //                                     v       v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update1TS + Collection.MVTOffset)(
          At(
            update0TS,
            userHistory(
              userRef,
              JSArray(
                JSArray("create", MkObject("name" -> "Bob", "age" -> 42))
              ))
          ))

        // now <--------------------------------------------------------------- epoch
        //                                snapshotTS
        //                                  validTS
        //                                     |      MVT
        //                                     v       v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update1TS + Collection.MVTOffset)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob", "age" -> 42)),
              JSArray("create", MkObject("name" -> "Bob L"))
            )))

        // now <--------------------------------------------------------------- epoch
        //                       snapshotTS
        //                            |     validTS
        //                            |       MVT
        //                            v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(deleteTS + Collection.MVTOffset)(
          At(
            update1TS,
            userHistory(
              userRef,
              JSArray(
                JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
              ))
          ))

        // now <--------------------------------------------------------------- epoch
        //                       snapshotTS
        //                         validTS
        //                            |       MVT
        //                            v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(deleteTS + Collection.MVTOffset)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob L", "age" -> 42)),
              JSArray("delete", JSNull)
            )))

        // now <--------------------------------------------------------------- epoch
        //            snapshotTS
        //                 |       validTS
        //                 |         MVT
        //                 v          v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + Collection.MVTOffset)(
          At(deleteTS, userHistory(userRef, JSArray.empty)))

        // now <--------------------------------------------------------------- epoch
        //            snapshotTS
        //              validTS
        //                 |         MVT
        //                 v          v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + Collection.MVTOffset)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
            )))

        // now <--------------------------------------------------------------- epoch
        //    snapshotTS
        //         |    validTS
        //         |      MVT
        //         v       v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + 1.day)(
          At(
            recreateTS,
            userHistory(
              userRef,
              JSArray(
                JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
              ))
          ))

        // now <--------------------------------------------------------------- epoch
        //    snapshotTS
        //      validTS
        //         |      MVT
        //         v       v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + 1.day)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
            )))

        // now <--------------------------------------------------------------- epoch
        // snapshotTS
        //  validTS
        //   |    MVT
        //   v     v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + 2.days)(
          userHistory(
            userRef,
            JSArray(
              JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
            )))
      }

      "in the presence of pending writes" - {
        "create/update" in withSimpleHistory { userRef =>
          // now <------------------------------------------------------------- epoch
          // snapshotTS
          //   |    MVT
          //   v     v
          //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
          //   ^
          // PENDING WRITE
          qassertAt(recreateTS + 2.days)(Do(
            Update(userRef, MkData("name" -> "pending")),
            And(
              userHistory(
                userRef,
                JSArray(
                  JSArray("create", MkObject("name" -> "Bob L", "age" -> 42)),
                  JSArray("update", MkObject("name" -> "pending"))
                )),
              At(
                recreateTS + 1.day,
                userHistory(
                  userRef,
                  JSArray(
                    JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
                  )
                ))
            )
          ))
        }
        "insert" in withSimpleHistory { userRef =>
          // now <------------------------------------------------------------- epoch
          //                               snapshotTS
          //                                    |        MVT
          //                                    v         v
          //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
          //                                         ^
          //                                  PENDING WRITE
          qassertAt(update1TS + Collection.MVTOffset)(Do(
            InsertVers(
              userRef,
              update1TS - 1.hour,
              "create",
              MkData(
                "name" -> "pending",
                "age" -> 42
              )),
            And(
              userHistory(
                userRef,
                JSArray(
                  JSArray("create", MkObject("name" -> "Bob", "age" -> 42)),
                  JSArray("update", MkObject("name" -> "pending")),
                  JSArray("update", MkObject("name" -> "Bob L"))
                )
              ),
              At(
                update1TS - 1.hour,
                userHistory(
                  userRef,
                  JSArray(
                    JSArray("create", MkObject("name" -> "Bob", "age" -> 42)),
                    JSArray("update", MkObject("name" -> "pending"))
                  )
                ))
            )
          ))
        }
        "remove" in withSimpleHistory { userRef =>
          // now <------------------------------------------------------------- epoch
          //                  snapshotTS
          //                       |       MVT
          //                       v        v
          //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
          //                           ^
          //                    PENDING WRITE
          qassertAt(deleteTS + 1.hour)(Do(
            RemoveVers(userRef, deleteTS, "delete"),
            And(
              userHistory(
                userRef,
                JSArray(
                  JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
                )),
              At(
                deleteTS,
                userHistory(
                  userRef,
                  JSArray(
                    JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
                  )
                ))
            )
          ))
        }

        "in the presence of TTLed documents" in withSimpleTTLedHistory { userRef =>
          val expectedHistory =
            JSArray(JSArray("create", MkObject("name" -> "Alice")))

          // There's history.
          // now <---------------------------- epoch
          //                  snapshotTS
          //                   validTS
          //                      |      MVT
          //                      v       v
          //  ... | ... | TTL | CREATE | ... | ...
          qassertAt(createTS)(userHistory(userRef, expectedHistory))

          // I don't think it's going anywhere.
          // now <---------------------------- epoch
          //           snapshotTS
          //            validTS
          //               |     MVT
          //               v      v
          //  ... | ... | TTL | CREATE | ... | ...
          qassertAt(update0TS)(userHistory(userRef, expectedHistory))

          // Look at it, just sitting there.
          // now <---------------------------- epoch
          //     snapshotTS
          //      validTS
          //         |    MVT
          //         v     v
          //  ... | ... | TTL | CREATE | ... | ...
          qassertAt(update1TS)(userHistory(userRef, expectedHistory))
        }
      }
    }
  }
}
