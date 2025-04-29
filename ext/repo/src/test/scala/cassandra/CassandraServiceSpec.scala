package fauna.repo.test

import fauna.repo.cassandra.CassandraService
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import scala.concurrent.Await
import scala.concurrent.duration._

class CassandraServiceSpec extends Spec with Eventually with OptionValues {

  "logLeaders in a single node cluster" - {

    "should eventually return itself as the SegmentLog leader" in {
      CassandraHelper.startServer("repo")

      val service = CassandraService.instanceOpt.value

      //should eventually return itself as the leader.
      eventually(timeout(30.seconds), interval(1.second)) {
        val leaders = Await.result(service.logLeaders, 10.seconds)
        leaders should contain only service.txnPipeline.hostID
      }
    }
  }
}
