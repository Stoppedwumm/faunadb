package fauna.cluster.test

import fauna.cluster.workerid.{ AssignedWorkerID, UnavailableWorkerID }
import fauna.exec.ImmediateExecutionContext
import org.scalatest.time.SpanSugar
import scala.concurrent.Future

class WorkerIDSpec extends Spec with SpanSugar {
  var nodes: IndexedSeq[Node] = _
  var node0: Node = _

  override def beforeAll() = {
    nodes = (Node.minWorkerID to Node.maxWorkerID) map { _ => Node.create() }
    node0 = nodes.head
    await(node0.membership.init())

    def join(nodes: Seq[Node]): Future[Unit] =
      if (nodes.isEmpty) {
        Future.unit
      } else {
        implicit val ec = ImmediateExecutionContext
        nodes.head.membership.join(node0.bus.hostAddress) flatMap { _ =>
          join(nodes.tail)
        }
      }

    await(join(nodes.tail))
  }

  private def nodeShouldBeActive(n: Node) =
    n.workerIDs.hostWorkerID(n.id).isInstanceOf[AssignedWorkerID]

  "Worker IDs" - {

    "should be assigned to every node" in {
      eventually(timeout(20 seconds), interval(200 milliseconds)) {
        nodes exists { _.workerIDs.workerID.idOpt.isEmpty } should equal (false)
      }
    }

    "should be unique" in {
      assertUniqueIDs()
    }

    "once assigned to every node should let the nodes become live" in {
      eventually(timeout(20 seconds), interval(200 milliseconds)) {
        nodes foreach nodeShouldBeActive
      }
    }

    "can't be assigned once cluster capacity is reached" in {
      val stepchild = Node.create()
      await(stepchild.membership.join(node0.bus.hostAddress))
      // It never got a worker ID
      eventually(timeout(20 seconds), interval(200 milliseconds)) {
        stepchild.workerIDs.workerID should equal (UnavailableWorkerID)
      }
      leaveGracefully(stepchild)
    }

    "can be assigned again once cluster shrinks below capacity" in {
      val leaving = nodes(1)
      leaving.workerIDs.workerID.idOpt.nonEmpty should equal (true)
      leaveGracefully(leaving)
      eventually(timeout(20 seconds), interval(200 milliseconds)) {
        nodes(0).workerIDs.hostWorkerID(leaving.id).idOpt.isEmpty should equal (true)
      }

      val newcomer = Node.create()
      await(newcomer.membership.join(node0.bus.hostAddress))
      eventually(timeout(40 seconds), interval(200 milliseconds)) {
        newcomer.workerIDs.workerID.idOpt.nonEmpty should equal (true)
      }
      eventually(timeout(60 seconds), interval(200 milliseconds)) {
        nodeShouldBeActive(newcomer)
      }

      assertUniqueIDs()

      leaveGracefully(newcomer)
    }
  }

  private def assertUniqueIDs() =
    (nodes flatMap { _.workerIDs.workerID.idOpt }).foldLeft(Set[Int]()) { (ids, id) =>
      ids.contains(id) should equal (false)
      ids + id
    }

  private def leaveGracefully(n: Node) = {
    removeGracefully(n, nodes(0))
    nodes = nodes.filter { _ != n }
  }

  override def afterAll() = {
    nodes foreach { _.stop() }
  }
}
