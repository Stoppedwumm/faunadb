package fauna.repo.test

import fauna.storage.{ Cassandra, SetReplicaNameResult }
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.util.{ Success, Try }

/**
  * This test should really be under [[fauna.storage]] package where
  * [[Cassandra]] implementation lives but [[fauna.storage]] does not
  * have access to [[CassandraHelper]] so this test case is temporarily
  * in this package.
  */
class CassandraSpec extends Spec with BeforeAndAfterAll {

  override def beforeAll(): Unit =
    Try(CassandraHelper.startServer("repo")) shouldBe Success(()) //server is mandatory. Assert there are no exception starting.

  "Cassandra" - {
    "should return None is no replica_name is set" in {
      Cassandra.getReplicaName shouldBe empty
    }

    "should set & get replica_name" in {
      val replicaName = "NoDC_replica_name"
      Cassandra.setReplicaNameIfEmpty(replicaName) shouldBe SetReplicaNameResult.Success
      Cassandra.getReplicaName should contain(replicaName)
    }

    "should fail if replica_name is already set" in {
      val replicaName = "NoDC_replica_name"
      val setReplicaNameInvokeDynamic = PrivateMethod[SetReplicaNameResult](Symbol("setReplicaName"))
      Cassandra.invokePrivate(setReplicaNameInvokeDynamic(replicaName)) shouldBe SetReplicaNameResult.Success

      //try updating the replica name - which is not allowed!
      Cassandra.setReplicaNameIfEmpty("new replica name") shouldBe SetReplicaNameResult.ReplicaNameAlreadySet(replicaName)

      //old replica name is not modified
      Cassandra.getReplicaName should contain(replicaName)
    }

    "should return valid replica_name when concurrent modified" in {
      val repeatTestRange = (1 to 10)
      val replicaName = "concurrent_replica_name_test"

      val setReplicaNameInvokeDynamic = PrivateMethod[SetReplicaNameResult](Symbol("setReplicaName"))

      def setReplicaName() =
        Cassandra.invokePrivate(setReplicaNameInvokeDynamic(replicaName)) shouldBe SetReplicaNameResult.Success

      //set an initial replica_name so reads return valid result.
      setReplicaName()

      //concurrently modify replica_name
      val f = Future.sequence {
        repeatTestRange.map  {
          _ => Future { setReplicaName() }
        }
      }
      //all reads return valid result.
      val g = Future.sequence {
        repeatTestRange.map  {
          _ => Future{ Cassandra.getReplicaName should contain(replicaName) }
        }
      }

      // make sure the next test doesn't fail because we're still mutating the replicaName
      Await.result(f, Duration.Inf)
      Await.result(g, Duration.Inf)
    }

    "insert valid replicaNames - names borrowed from ReplicaNameValidatorSpec" in {
      val setReplicaNameInvokeDynamic = PrivateMethod[SetReplicaNameResult](Symbol("setReplicaName"))

      Seq(
        "ABCabc123-_",
        "ABCDEFGHIJKLMNOPQRSTUVWYXZ",
        "abcdefghijklmnopqrstuvwyxz",
        "1abcdef",
        "a1234567890",
        "مركزمعلوماتالرياض",
        "מרכזבחיפה",
        "北京數據中心",
        "a-b_c",
      ) foreach {
        replicaName =>
          Cassandra.invokePrivate(setReplicaNameInvokeDynamic(replicaName)) shouldBe SetReplicaNameResult.Success
          Cassandra.getReplicaName should contain(replicaName)
      }

    }
  }
}
