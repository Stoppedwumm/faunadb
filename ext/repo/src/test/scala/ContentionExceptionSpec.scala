package fauna.repo.test

import fauna.atoms._
import fauna.lang._
import fauna.repo._
import scala.concurrent.duration._

class ContentionExceptionSpec extends Spec {
  "aggregate" - {
    "returns a single error if possible" in {
      val err =
        DocContentionException(
          ScopeID.RootID,
          DocID.MinValue,
          Timestamp.Epoch
        )

      ContentionException.aggregate(err :: err :: err :: Nil) shouldBe err
    }

    "aggregates by source" in {
      val doc0 =
        DocContentionException(
          ScopeID.RootID,
          DocID.MinValue,
          Timestamp.Epoch
        )
      val doc1 = doc0.copy(newTS = Timestamp.Epoch + 1.hour)
      val schema = SchemaContentionException(ScopeID.RootID, SchemaVersion.Min)
      val agg = ContentionException.aggregate(doc0 :: doc1 :: schema :: Nil)
      agg shouldBe a[AggregateContentionException]

      val byErrCount = agg.asInstanceOf[AggregateContentionException].exns
      byErrCount should have size 2
      byErrCount should contain(doc1 -> 2) // preserve highest ts
      byErrCount should contain(schema -> 1)
    }
  }
}
