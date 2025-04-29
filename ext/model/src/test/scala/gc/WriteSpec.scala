package fauna.model.gc.test

import fauna.codex.json._
import fauna.lang.clocks._
import fauna.model.test._
import fauna.model.Collection
import fauna.repo.query.Query
import scala.concurrent.duration._

class WriteSpec extends InlineGCSpec {
  import SocialHelpers._

  private def withCustomHistory[A](test: JSValue => A) = {
    val userID = ctx ! Query.nextID
    val userRef = RefV(userID, ClsRefV("users"))

    // Align timeline so that Clock.time sees the 2 versions:
    //  - Update 1: latest version
    //  - Update 0: the gc root
    createTS = Clock.time - 2.days - Collection.MVTOffset
    update0TS = createTS + 1.day
    update1TS = update0TS + 1.day

    writeAt(createTS, CreateF(userRef, MkData("name" -> "Bob")))
    writeAt(update0TS, Update(userRef, MkData("age" -> 42)))
    writeAt(update1TS, Update(userRef, MkData("name" -> "Bob L")))

    test(userRef)
  }

  "Insert" - {
    "above MVT" in withCustomHistory { userRef =>
      // now <------------------------------------------- epoch
      //     snapshotTS         MVT
      //         v               v
      //  ... | ... | UPDATE | UPDATE | CREATE | ... | ...
      //                     ^
      //                  insert
      val res =
        runQueryAt(Clock.time)(
          InsertVers(
            userRef,
            update1TS - 1.hour,
            "create",
            MkData(
              "name" -> "INSERT",
              "age" -> 24
            )
          ))

      qassertAt(res.transactionTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob", "age" -> 42)),
            JSArray("update", MkObject("name" -> "INSERT", "age" -> 24)),
            JSArray("update", MkObject("name" -> "Bob L", "age" -> 42))
          )
        ))
    }

    "at MVT" in withCustomHistory { userRef =>
      // now <------------------------------------------- epoch
      //     snapshotTS         MVT
      //         v               v
      //  ... | ... | UPDATE | UPDATE | CREATE | ... | ...
      //                         ^
      //                      insert
      val res =
        runQueryAt(update1TS)(
          InsertVers(
            userRef,
            update0TS,
            "create",
            MkData(
              "name" -> "INSERT",
              "age" -> 24
            )
          ))

      qassertAt(res.transactionTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "INSERT", "age" -> 24)),
            JSArray("update", MkObject("name" -> "Bob L", "age" -> 42))
          )))
    }

    "below MVT" in withCustomHistory { userRef =>
      // now <------------------------------------------- epoch
      //     snapshotTS         MVT
      //         v               v
      //  ... | ... | UPDATE | UPDATE | CREATE | ... | ...
      //                                  ^
      //                               insert
      val res =
        runQueryAt(Clock.time)(
          InsertVers(
            userRef,
            createTS,
            "create",
            MkData(
              "name" -> "INSERT",
              "age" -> 24
            )
          ))

      qassertAt(res.transactionTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob", "age" -> 42)),
            JSArray("update", MkObject("name" -> "Bob L"))
          )))
    }
  }

  "Remove" - {
    "above MVT" in withCustomHistory { userRef =>
      // now <------------------------------------------- epoch
      //     snapshotTS         MVT
      //         v               v
      //  ... | ... | UPDATE | UPDATE | CREATE | ... | ...
      //                ^
      //              remove
      val res = runQueryAt(Clock.time)(RemoveVers(userRef, update1TS, "create"))

      qassertAt(res.transactionTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob", "age" -> 42))
          )))
    }

    "at MVT" in withCustomHistory { userRef =>
      // now <------------------------------------------- epoch
      //     snapshotTS         MVT
      //         v               v
      //  ... | ... | UPDATE | UPDATE | CREATE | ... | ...
      //                         ^
      //                       remove
      val res = runQueryAt(Clock.time)(RemoveVers(userRef, update0TS, "create"))

      qassertAt(res.transactionTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob L", "age" -> 42))
          )))
    }

    "below MVT" in withCustomHistory { userRef =>
      // now <------------------------------------------- epoch
      //     snapshotTS         MVT
      //         v               v
      //  ... | ... | UPDATE | UPDATE | CREATE | ... | ...
      //                                  ^
      //                                remove
      val res = runQueryAt(Clock.time)(RemoveVers(userRef, createTS, "create"))

      qassertAt(res.transactionTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob", "age" -> 42)),
            JSArray("update", MkObject("name" -> "Bob L"))
          )))
    }
  }
}
