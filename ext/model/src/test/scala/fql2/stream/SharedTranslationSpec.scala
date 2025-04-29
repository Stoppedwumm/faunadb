package fauna.model.test

import fauna.atoms.{ CollectionID, DocID, SubID }
import fauna.exec.{ ImmediateExecutionContext, Observable }
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.SingletonSet
import fauna.model.runtime.stream.SharedTranslation
import fauna.repo.service.stream.TxnResult
import fauna.repo.values.Value
import scala.concurrent.Future

class SharedTranslationSpec extends FQL2Spec with StreamContextHelpers {

  implicit val ec = ImmediateExecutionContext

  "SharedTranslation" - {

    "can share stream translation" in {
      val sharedTranslations = new SharedTranslation[Long]
      val set = SingletonSet(Value.Doc(DocID(SubID(1), CollectionID(1024L))))
      val key = SharedTranslation.Key(newScope, set, Vector.empty)
      val (pub, obs) = Observable.gathering[Seq[TxnResult]]()

      @volatile var translations = 0

      def translate(txns: Seq[TxnResult]) =
        synchronized {
          translations += 1
          Future.successful(
            Seq(
              txns.last.txnTS.micros
            ))
        }

      val translated0 = sharedTranslations.share(key, obs, translate).sequenceF
      pub.publish(Seq(TxnResult(Timestamp.ofMicros(1), Clock.time, Vector.empty)))
      pub.publish(Seq(TxnResult(Timestamp.ofMicros(2), Clock.time, Vector.empty)))

      val translated1 = sharedTranslations.share(key, obs, translate).sequenceF
      pub.publish(Seq(TxnResult(Timestamp.ofMicros(3), Clock.time, Vector.empty)))
      pub.publish(Seq(TxnResult(Timestamp.ofMicros(4), Clock.time, Vector.empty)))
      pub.close()

      await(translated0) should contain.inOrderOnly(1, 2, 3, 4) // see all txns
      await(translated1) should contain.inOrderOnly(2, 3, 4) // 2 is synthetic
      sharedTranslations.size shouldBe 0 // clean up the map after streams are done
      translations shouldBe 5 // 4 txns + 1 synthetic start event
    }
  }
}
