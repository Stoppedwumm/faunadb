package fauna.model.gc.test

import fauna.model.test._
import scala.concurrent.duration._

class DocSnapshotSpec extends InlineGCSpec {
  import SocialHelpers._

  "Document" - {
    "snapshot is correct" - {
      "according to snapshot, valid time, and MVT" in withSimpleHistory { userRef =>
        // now <--------------------------------------------------------------- epoch
        //                                                        snapshotTS
        //                                                          validTS
        //                                                              |    MVT
        //                                                              v     v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(createTS - 1.day)(Not(Exists(userRef)))

        // now <--------------------------------------------------------------- epoch
        //                                               snapshotTS
        //                                                    |     validTS
        //                                                    |        MVT
        //                                                    v         v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(createTS)(At(createTS - 1.day, Not(Exists(userRef))))

        // now <--------------------------------------------------------------- epoch
        //                                                 snapshotTS
        //                                                   validTS
        //                                                      |      MVT
        //                                                      v       v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(createTS)(userEqual(userRef, MkObject("name" -> "Bob")))

        // now <--------------------------------------------------------------- epoch
        //                                        snapshotTS
        //                                             |     validTS
        //                                             |       MVT
        //                                             v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update0TS)(
          At(createTS, userEqual(userRef, MkObject("name" -> "Bob"))))

        // now <--------------------------------------------------------------- epoch
        //                                        snapshotTS
        //                                          validTS
        //                                             |       MVT
        //                                             v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update0TS)(
          userEqual(userRef, MkObject("name" -> "Bob", "age" -> 42)))
        // now <--------------------------------------------------------------- epoch
        //                               snapshotTS
        //                                    |     validTS
        //                                    |       MVT
        //                                    v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update1TS)(
          At(update0TS, userEqual(userRef, MkObject("name" -> "Bob", "age" -> 42))))

        // now <--------------------------------------------------------------- epoch
        //                                snapshotTS
        //                                 validTS
        //                                    |       MVT
        //                                    v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(update1TS)(
          userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)))

        // now <--------------------------------------------------------------- epoch
        //                      snapshotTS
        //                           |     validTS
        //                           |       MVT
        //                           v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(deleteTS)(
          At(
            update1TS,
            userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42))))

        // now <--------------------------------------------------------------- epoch
        //                       snapshotTS
        //                        validTS
        //                           |       MVT
        //                           v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(deleteTS)(Not(Exists(userRef)))

        // now <--------------------------------------------------------------- epoch
        //            snapshotTS
        //                 |      validTS
        //                 |        MVT
        //                 v         v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS)(At(deleteTS, Not(Exists(userRef))))

        // now <--------------------------------------------------------------- epoch
        //             snapshotTS
        //               validTS
        //                  |       MVT
        //                  v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS)(
          userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)))

        // now <--------------------------------------------------------------- epoch
        //    snapshotTS
        //         |     validTS
        //         |       MVT
        //         v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + 1.day)(
          At(
            recreateTS,
            userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42))))

        // now <--------------------------------------------------------------- epoch
        //    snapshotTS
        //      validTS
        //         |       MVT
        //         v        v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + 1.day)(
          userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)))

        // now <--------------------------------------------------------------- epoch
        // snapshotTS
        //  validTS
        //    |   MVT
        //    v    v
        //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
        qassertAt(recreateTS + 2.days)(
          userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)))
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
          qassertAt(recreateTS + 2.days)(
            Do(
              Update(userRef, MkData("name" -> "pending")),
              userEqual(userRef, MkObject("name" -> "pending", "age" -> 42))
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
          qassertAt(update1TS)(Do(
            InsertVers(
              userRef,
              update1TS - 1.hour,
              "create",
              MkData("name" -> "pending")
            ),
            And(
              userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)),
              At(
                update1TS - 1.second,
                userEqual(userRef, MkObject("name" -> "pending")))
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
              userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)),
              At(
                deleteTS,
                userEqual(userRef, MkObject("name" -> "Bob L", "age" -> 42)))
            )
          ))
        }
      }

      "in the presence of TTLed documents" in withSimpleTTLedHistory { userTTLRef =>
        // There's a document.
        // now <---------------------------- epoch
        //                  snapshotTS
        //                   validTS
        //                      |      MVT
        //                      v       v
        //  ... | ... | TTL | CREATE | ... | ...
        qassertAt(createTS)(Exists(userTTLRef))

        // TTL'd.
        // now <---------------------------- epoch
        //           snapshotTS
        //            validTS
        //               |     MVT
        //               v      v
        //  ... | ... | TTL | CREATE | ... | ...
        qassertAt(update0TS)(Not(Exists(userTTLRef)))

        // Still TTL'd.
        // now <---------------------------- epoch
        //     snapshotTS
        //      validTS
        //         |    MVT
        //         v     v
        //  ... | ... | TTL | CREATE | ... | ...
        qassertAt(update1TS)(Not(Exists(userTTLRef)))

        // RIP.
        // now <---------------------------- epoch
        // snapshotTS
        // validTS
        //   |    MVT
        //   v     v
        //  ... | ... | TTL | CREATE | ... | ...
        qassertAt(deleteTS)(Not(Exists(userTTLRef)))
      }
    }
  }
}
