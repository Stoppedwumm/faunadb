package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.codex.json._
import fauna.prop._
import fauna.prop.api._
import fauna.prop.test.PropSpecConfig
import scala.concurrent.Await
import scala.concurrent.duration._

class TopologyChangesSpec
    extends Spec
    with APIGenerators
    with DefaultQueryHelpers {
  private[this] var _aDatabase: Prop[Database] = _

  val apiVers = "2.1"
  val apis = (1 to 4) map { CoreLauncher.client(_, apiVers).admin }
  val admin = apis(0)
  val api = CoreLauncher.client(1, apiVers).api
  def aDatabase = _aDatabase
  override val rootKey = FaunaDB.rootKey

  override def client = CoreLauncher.client(1, apiVers)

  val classCount = 2
  val fieldCount = 3
  val instanceCount = 100
  var initSuccessful = false
  var balanceSuccessful = false
  var shrinkSuccessful = false

  implicit val propConfig = PropSpecConfig()

  var ids: Seq[JSValue] = _
  var database: DbData = _

  def id(i: Int) = ids(i - 1)

  override protected def afterAll() =
    CoreLauncher.terminateAll()

  "Cluster should be able to" - {

    "initialize and get populated with data consistently" in {
      CoreLauncher.launch(1, apiVers)
      init(admin, "r1")

      Await.result(waitUntilLive(admin, getIdentity(admin)), 10.seconds)

      CoreLauncher.launchMultiple(Seq(2, 3, 4), 2)
      apis.drop(1).zip(Seq("r1", "r1", "r2")) foreach {
        case (client, replica) =>
          join(client, replica)
      }

      setReplication(admin, "r1" -> ReplicaTypeNames.Log, "r2" -> ReplicaTypeNames.Log) should respond(NoContent)

      ids = apis map getIdentity

      _aDatabase = aDatabase(apiVers)
      database = DbData(_aDatabase, classCount, instanceCount, fieldCount, this).sample

      verifyDataIntegrity(false)
      initSuccessful = true
    }

    // FIXME: these tests require clean storage, which
    // currently takes 5+ mins. to run.
    "balance a replica" in pendingUntilFixed {
      initSuccessful should equal(true)

      Await.result(noTopologyChangeInProgress(admin), 120.seconds)

      // No replica name specified
      val res1 = admin.post("/admin/balance_replica", JSObject(), rootKey)
      res1 should respond (BadRequest)

      //  Invalid replica name specified
      val res2 = admin.post("/admin/balance_replica", JSObject("replicaName" -> "invalid"), rootKey)
      res2 should respond (BadRequest)

      def r1Ownership() =
        getStatuses(admin) collect {
          case n if n / "replica" == JSString("r1") => ((n / "ownership_goal").as[Double]*100000).round
        }

      // It could be a miracle that the topology was perfectly balanced immediately
      // but let's presume this can't happen
      r1Ownership() should not equal(Seq(33333, 33333, 33333))

      // Should work now
      val res3 = admin.post("/admin/balance_replica", JSObject("replicaName" -> "r1"), rootKey)
      res3 should respond (NoContent)

      // Should be balanced now
      r1Ownership() should equal(Seq(33333, 33333, 33333))

      balanceSuccessful = true
    }

    "not lose any data in replica after shutdown of one node in it and the whole other replica" in pendingUntilFixed {
      balanceSuccessful should equal(true)

      Await.result(noTopologyChangeInProgress(admin), 120.seconds)

      eventually(timeout(40.seconds), interval(1.second)) {
        getStatus(admin, id(3)) map { _ / "log_segment" } should equal(Some(JSString("none")))
      }

      CoreLauncher.terminate(3)
      removeNode(admin, id(3), force = true)

      setReplication(admin, "r1" -> ReplicaTypeNames.Log, "r2" -> ReplicaTypeNames.Compute) should respond(NoContent)

      CoreLauncher.terminate(4)
      removeNode(admin, id(4))

      // At this point, nodes 1 and 2 are the only ones online and they must together hold all of the data.
      verifyDataIntegrity(false) // change to true for debug tracing

      shrinkSuccessful = true
    }

    "not lose any data in replica after shutdown of one node in the only replica" in pendingUntilFixed {
      shrinkSuccessful should equal(true)

      Await.result(noTopologyChangeInProgress(admin), 120.seconds)

      // Let's launch node 3 as a new node as we can't shrink the cluster down to 1 node
      CoreLauncher.launch(3, mcAPIVers)
      join(apis(2), "r1")

      // Can immediately remove node 2 after node 3 joined
      removeNode(admin, id(2))

      // Let node 3 stream data from node 2, which needs to be still online
      Await.result(noTopologyChangeInProgress(admin), 120.seconds)

      CoreLauncher.terminate(2)

      // At this point, node 1 and 3 are the only ones online and they must hold all of the data.
      verifyDataIntegrity(false) // change to true for debug tracing
    }
  }

  def verifyDataIntegrity(trace: Boolean): Unit =
    database.verifyDataIntegrity(trace, api)
}
