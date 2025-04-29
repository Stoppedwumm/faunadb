package fauna.tx.test

import fauna.atoms.HostID
import fauna.tx.consensus.{ Quorum, QuorumCheck, Ring }
import java.util.concurrent.TimeoutException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class QuorumSpec extends Spec {

  val id1 = HostID.randomID
  val id2 = HostID.randomID
  val id3 = HostID.randomID

  implicit class AwaitImplicit[F](f: Future[F]) {
    def await: F = Await.result(f, Duration.Inf)
  }

  "Quorum check" - {

    "on single hostID" - {

      val hostIDQuorum: QuorumCheck[Option[HostID], HostID] =
        (responses: Seq[Option[HostID]]) =>
          Ring(Set(id1)).quorumValue(responses.flatten)

      "should return None if response was None" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(Future.successful(None)),
          check = hostIDQuorum
        ).await shouldBe empty
      }

      "should successfully quorum and return the hostID" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(Future.successful(Some(id1))),
          check = hostIDQuorum
        ).await should contain(id1)
      }

      "should timeout for late response" in {
        assertThrows[TimeoutException] {
          Quorum.check[Option[HostID], HostID](
            requests = List(
              Future {
                Thread.sleep((Quorum.QuorumCheckTimeout + 2.seconds).toMillis)
                Some(id1)
              }
            ),
            check = hostIDQuorum
          ).await
        }
      }
    }

    "on multiple hostIDs" - {

      implicit val quorumCheck: QuorumCheck[Option[HostID], HostID] =
        (responses: Seq[Option[HostID]]) =>
          Ring(Set(id1, id2, id3)).quorumValue(responses.flatten)

      "should return none if majority do not agree" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(
            Future.successful(Some(id1)),
            Future.successful(Some(id2)),
            Future.successful(Some(id3))
          ),
          check = quorumCheck
        ).await shouldBe empty
      }

      "should return some if majority agree" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(
            Future.successful(Some(id2)),
            Future.successful(Some(id2)),
            Future.successful(Some(id3))
          ),
          check = quorumCheck
        ).await should contain(id2)
      }

      "should return some if all agree" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(
            Future.successful(Some(id3)),
            Future.successful(Some(id3)),
            Future.successful(Some(id3))
          ),
          check = quorumCheck
        ).await should contain(id3)
      }

      "should return some if majority agree and one fails" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(
            Future.successful(Some(id2)),
            Future.successful(Some(id2)),
            Future.failed(new Exception("Oh no!"))
          ),
          check = quorumCheck
        ).await should contain(id2)
      }

      "should return none if majority disagree and one has a delay response" in {
        Quorum.check[Option[HostID], HostID](
          requests = List(
            Future.successful(Some(id2)),
            Future.successful(Some(id1)),
            Future {
              Thread.sleep((Quorum.QuorumCheckTimeout - 2.second).toMillis)
              Some(id3)
            }
          ),
          check = quorumCheck
        ).await shouldBe empty
      }

      "should fail if majority disagree and one times out" in {
        assertThrows[TimeoutException] {
          Quorum.check[Option[HostID], HostID](
            requests = List(
              Future.successful(Some(id2)),
              Future.successful(Some(id1)),
              Future {
                Thread.sleep((Quorum.QuorumCheckTimeout + 2.second).toMillis)
                Some(id2)
              }
            ),
            check = quorumCheck
          ).await
        }
      }
    }
  }
}
