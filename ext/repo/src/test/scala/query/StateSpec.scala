package fauna.repo.test

import fauna.atoms._
import fauna.lang.{ TimeBound, Timestamp }
import fauna.repo.query.{ CollectionWrites, ReadsWrites, State }
import fauna.repo.test.Spec
import fauna.stats.StatsRecorder
import fauna.storage.{ AtValid, DocAction, Unresolved }
import fauna.storage.ir.MapV
import fauna.storage.ops.{ DocRemove, VersionAdd, Write }

class StateSpec extends Spec {

  def mkState() =
    State(
      parent = None,
      enabledConcurrencyChecks = false,
      readsWrites = ReadsWrites.empty,
      collectionWrites = CollectionWrites.empty,
      cacheMisses = 0,
      stats = StatsRecorder.Null,
      deadline = TimeBound.Max,
      txnSizeLimitBytes = 10000000,
      txnSizeBytes = 100000,
      readOnlyTxn = false,
      flushLocalKeyspace = Set.empty,
      serialReads = 5,
      unlimited = false
    )

  val create1 =
    VersionAdd(
      scope = ScopeID(1L),
      id = DocID(SubID(1L), CollectionID(1)),
      writeTS = Unresolved,
      action = DocAction.Create,
      schemaVersion = SchemaVersion.Min,
      data = MapV("foo" -> "Bar").toData,
      diff = None
    )

  val update1 =
    VersionAdd(
      scope = ScopeID(1L),
      id = DocID(SubID(1L), CollectionID(1)),
      writeTS = Unresolved,
      action = DocAction.Update,
      schemaVersion = SchemaVersion.Min,
      data = MapV("foo" -> "Baz").toData,
      diff = Some(MapV("foo" -> "Bar").toDiff)
    )

  val remove1 =
    DocRemove(
      ScopeID(1L),
      DocID(SubID(1L), CollectionID(1))
    )

  val create2 =
    VersionAdd(
      scope = ScopeID(1L),
      id = DocID(SubID(2L), CollectionID(1)),
      writeTS = Unresolved,
      action = DocAction.Create,
      schemaVersion = SchemaVersion.Min,
      data = MapV("foo" -> "Bar").toData,
      diff = None
    )

  def withWrites(writes: Write*)(test: State => Any) = {
    "in the same state" in {
      test(writes.foldLeft(mkState()) { _.addWrite(_) })
    }
    "in a state chain" in {
      test(writes.foldLeft(mkState()) { _.checkpoint.addWrite(_) })
    }
  }

  "State" - {

    "allPending" - {
      "return merged writes" - {
        withWrites(create1, update1, remove1, create2) {
          _.allPending().toSeq should
            contain.inOrderOnly(
              Write.merge(create1, update1),
              create2,
              remove1
            )
        }
      }
    }

    "writesForRowKey" - {
      "only returns writes for the row key requested" - {
        withWrites(create1, update1, create2) {
          _.writesForRowKey(create2.rowKey).toSeq should contain only create2
        }
      }

      "returns merged writes" - {
        withWrites(create1, update1) {
          _.writesForRowKey(create1.rowKey).toSeq should
            contain only Write.merge(create1, update1)
        }
      }

      "non-mergeable writes are returned last" - {
        withWrites(remove1, create1, update1) {
          _.writesForRowKey(create1.rowKey).toSeq should
            contain.inOrderOnly(
              Write.merge(create1, update1),
              remove1
            )
        }
      }

      "historical writes do not merge" - {
        val create = create1.copy(writeTS = AtValid(Timestamp.ofMicros(1)))
        val update = update1.copy(writeTS = AtValid(Timestamp.ofMicros(2)))

        withWrites(create, update) {
          _.writesForRowKey(update.rowKey).toSeq should
            contain.inOrderOnly(create, update)
        }
      }
    }
  }
}
