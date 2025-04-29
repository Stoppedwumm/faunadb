package fauna.model.gc.test

import fauna.codex.json._
import fauna.model.test._
import fauna.model.Collection
import scala.concurrent.duration._

// Because of the MVT offset, there isn't a qualitative distinction between a
// collection with zero history days and other collections. This test verifies
// that zero history collections act as if they had a history period equal to
// the MVT offset.
class ZeroHistorySpec extends InlineGCSpec(historyDays = 0) {
  import SocialHelpers._

  "Document" - {

    // There's nothing to check for snapshots.

    "history is correct" in withSimpleHistory { userRef =>
      // now <--------------------------------------------------------------- epoch
      //                                        snapshotTS
      //                                             v
      //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
      qassertAt(update0TS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob")),
            JSArray("update", MkObject("age" -> 42))
          )))

      // Compare with the previous test. The MVT is at the update, so
      // prior history is merged into a single create.
      // now <--------------------------------------------------------------- epoch
      //                            snapshotTS
      //                                v
      //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
      qassertAt(update0TS + Collection.MVTOffset)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob", "age" -> 42))
          )))

      // now <--------------------------------------------------------------- epoch
      //                       snapshotTS
      //                            v
      //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
      qassertAt(deleteTS)(
        userHistory(
          userRef,
          JSArray(
            JSArray("create", MkObject("name" -> "Bob L", "age" -> 42)),
            JSArray("delete", JSNull)
          )))

      // There's no history because MVT is at the delete, so the document ceases to
      // exist.
      // now <--------------------------------------------------------------- epoch
      //                   snapshotTS
      //                       v
      //  ... | ... | RECREATE | DELETE | UPDATE | UPDATE | CREATE | ... | ...
      qassertAt(deleteTS + Collection.MVTOffset)(userHistory(userRef, JSArray.empty))
    }
  }

  "Index" - {

    // Likewise, there isn't anything different to check for snapshots.

    "history is correct" in withSimpleHistory { _ =>
      // now <------------------------------------------------------------- epoch
      //                                                    snapshotTS
      //                                                        v
      //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
      //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
      qassertAt(createTS)(
        pairwiseHistory(
          "Bob",
          JSArray(
            JSArray("add", MkObject("name" -> "Bob"))
          )))

      // Set history is truncated before the MVT.
      // now <------------------------------------------------------------- epoch
      //                                                snapshotTS
      //                                                    v
      //  ... | ... | RECREATE | DELETE |  UPDATE  | UPDATE | CREATE | ... | ...
      //  ... | ... |    ADD   | REMOVE |ADD|REMOVE|        |  ADD   | ... | ...
      qassertAt(createTS + Collection.MVTOffset + 1.second)(
        pairwiseHistory("Bob", JSArray.empty))
    }
  }
}
