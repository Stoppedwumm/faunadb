package fauna.model.test

import fauna.atoms._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.model.stream._
import fauna.repo.service.stream._
import fauna.repo.test._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ops._

class StreamSubscriptionSpec extends Spec {
  import FaunaExecutionContext.Implicits.global

  val ctx = CassandraHelper.context("model")

  "StreamSubscription" - {

    "convert writes to events" in {
      val startTS = Timestamp.ofMicros(1234)
      val writeTS = Timestamp.ofMicros(1235)

      val write =
        VersionAdd(
          ScopeID.RootID,
          DocID.MinValue,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data.empty,
          diff = None
        )
      val dispatchTS = Clock.time
      val sub = new StreamSubscription(
        Observable.from(
          Seq(
            TxnResult(startTS, dispatchTS, Vector.empty),
            TxnResult(writeTS, dispatchTS, Vector(write))
          )),
        isPartitioned = false,
      )

      await(sub.events.sequenceF) should contain.inOrderOnly(
        TxnEvents(startTS, dispatchTS, Vector(StreamStart(startTS))),
        TxnEvents(writeTS, dispatchTS, Vector(NewVersionAdded(write)))
      )
    }
  }
}
