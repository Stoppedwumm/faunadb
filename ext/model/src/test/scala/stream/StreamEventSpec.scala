package fauna.model.test

import fauna.atoms._
import fauna.lang._
import fauna.model.stream._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ops._
import scala.concurrent.duration._

class StreamEventSpec extends Spec {

  val txnTS = Timestamp.ofMicros(12345)
  val docID = DocID(SubID.MinValue, CollectionID(1234))
  val idxID = IndexID(1025)

  "StreamEvent" - {
    "should derive new version added event from write" in {
      val write =
        VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data.empty,
          diff = None
        )
      val event = StreamEvent(txnTS, write, isPartitioned = false)
      event shouldBe NewVersionAdded(write)
    }

    "should derive history rewrite event from write" in {
      val write =
        VersionAdd(
          ScopeID.RootID,
          docID,
          AtValid(txnTS - 10.days),
          Create,
          SchemaVersion.Min,
          Data.empty,
          diff = None
        )
      val event = StreamEvent(txnTS, write, isPartitioned = false)
      event shouldBe HistoryRewrite(write)
    }

    "should derive version removed event from write" in {
      val write = VersionRemove(ScopeID.RootID, docID, Unresolved, Delete)
      val event = StreamEvent(txnTS, write, isPartitioned = false)
      event shouldBe VersionRemoved(write)
    }

    "should derive document removed event from write" in {
      val write = DocRemove(ScopeID.RootID, docID)
      val event = StreamEvent(txnTS, write, isPartitioned = false)
      event shouldBe DocumentRemoved(write)
    }

    "should derive set add event from write" in {
      val write =
        SetAdd(
          ScopeID.RootID,
          idxID,
          Vector.empty,
          Vector.empty,
          docID,
          Unresolved,
          Add,
          None
        )
      val event = StreamEvent(txnTS, write, isPartitioned = true)
      event shouldBe SetAdded(write, isPartitioned = true)
    }

    "should derive set remove event from write" in {
      val write =
        SetRemove(
          ScopeID.RootID,
          idxID,
          Vector.empty,
          Vector.empty,
          docID,
          Unresolved,
          Add,
          None
        )
      val event = StreamEvent(txnTS, write, isPartitioned = true)
      event shouldBe SetRemoved(write, isPartitioned = true)
    }
  }
}
