package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.atoms.ScopeID
import fauna.codex.json._
import fauna.lang.syntax._
import fauna.prop.api.{ CoreLauncher, DefaultQueryHelpers }
import fauna.prop.api.CoreLauncher.Gracefully
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import scala.util.Random

class RepairSpec
    extends Spec
    with Eventually
    with DefaultQueryHelpers {

  val admin1 = CoreLauncher.adminClient(1, mcAPIVers)
  val admin2 = CoreLauncher.adminClient(2, mcAPIVers)
  val admin3 = CoreLauncher.adminClient(3, mcAPIVers)

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)
  val api2 = CoreLauncher.apiClient(2, mcAPIVers)
  val api3 = CoreLauncher.apiClient(3, mcAPIVers)

  override protected def afterAll() = CoreLauncher.terminateAll()

  "Repair" - {

    def addLargeComponent(): Unit = {
      val colQ = api1.query(
        CreateCollection(MkObject("name" -> "col")),
        rootKey
      )
      colQ should respond (Created)
      val col = colQ.resource

      val idxQ = api1.query(
        CreateIndex(MkObject(
          "name" -> "col_by_f1",
          "source" -> col.refObj,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "f1"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "f2")))
        )),
        rootKey
      )
      idxQ should respond (Created)
      val idx = idxQ.resource

      eventually(timeout(2.minutes), interval(1.second)) {
        val eventualResponse = api1.query(Get(idx.refObj), rootKey)
        eventualResponse.resource.get("active") should equal(JSTrue)
      }

      val big: JSValue = Bytes(new Array[Byte](Short.MaxValue * 10))
      val docQ = api1.query(
        CreateF(
          col.refObj,
          MkObject(
            "data" -> MkObject(
              "f1" -> big,
              "f2" -> big
            )
          )
        ),
        rootKey
      )
      docQ should respond (Created)
    }

    "restores replication" ignore {
      CoreLauncher.launchMultiple(
        Seq(1, 2, 3),
        syncOnShutdown = true)

      admin1.post("/admin/init", JSObject("replica_name" -> "dc1"), rootKey)
      join(admin2, "dc2")
      join(admin3, "dc3")

      setReplication(admin1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log, "dc3" -> ReplicaTypeNames.Log) should respond (NoContent)

      eventually(timeout(scaled(3.minutes))) {
        val res = admin1.get("/admin/status", rootKey)
        res should respond (OK)

        val nodes = (res.json / "nodes").as[Seq[JSObject]]
        nodes foreach { node =>
          val owns = (node / "ownership").as[Float]
          owns should equal (1.0)
        }
      }

      waitForPing(Seq(api1, api2, api3))

      addLargeComponent()

      def api = Random.shuffle(Seq(api1, api2, api3)).head
      val dbs = Seq.newBuilder[String]
      val keys = Seq.newBuilder[String]

      (0 until 100) foreach { i =>
        val db = api.query(CreateDatabase(MkObject("name" -> s"monkey_stuff$i")), rootKey)
        db should respond (Created)
        dbs += db.ref

        // it can take some time for all replicas to observe this
        // write. We'll need to wait before trying to create a key for
        // it.
        eventually(timeout(scaled(5.seconds))) {
          val one = api1.query(Get(Ref(db.ref)), rootKey)
          one should respond (OK)

          val two = api2.query(Get(Ref(db.ref)), rootKey)
          two should respond (OK)

          val three = api3.query(Get(Ref(db.ref)), rootKey)
          three should respond (OK)
        }

        val key = api.query(CreateKey(MkObject("database" -> Ref(db.ref), "role" -> "server")), rootKey)
        key should respond (Created)
        keys += key.ref
      }

      dbs.result() foreach { ref =>
        val one = api1.query(Get(Ref(ref)), rootKey)
        one should respond (OK)

        val two = api2.query(Get(Ref(ref)), rootKey)
        two should respond (OK)

        val three = api3.query(Get(Ref(ref)), rootKey)
        three should respond (OK)
      }

      keys.result() foreach { ref =>
        val one = api1.query(Get(Ref(ref)), rootKey)
        one should respond (OK)

        val two = api2.query(Get(Ref(ref)), rootKey)
        two should respond (OK)

        val three = api3.query(Get(Ref(ref)), rootKey)
        three should respond (OK)
      }

      CoreLauncher.terminate(2, Gracefully)

      val keyspace = CoreLauncher.instanceDir(2) / "data" / "FAUNA"
      keyspace.deleteRecursively()

      CoreLauncher.relaunch(2, mcAPIVers)

      waitForPing(Seq(api2))

      CoreLauncher.adminCLI(1, Seq("repair")) should equal(0)

      // maximum sleep time of the task executor is 5 mins.
      eventually(timeout(10.minutes)) {
        val res = admin1.get("/admin/repair_task/status", rootKey)
        res should respond(OK)
        (res.resource / "pending").as[Boolean] should equal(true)
      }

      // time enough to sync Versions and rebuild indexes...
      eventually(timeout(5.minutes)) {
        val res = admin1.get("/admin/repair_task/status", rootKey)
        res should respond(OK)
        (res.resource / "pending").as[Boolean] should equal(false)
      }

      // prove that the cluster is healthy
      dbs.result() foreach { ref =>
        val one = api1.query(Get(Ref(ref)), rootKey)
        one should respond (OK)

        val two = api2.query(Get(Ref(ref)), rootKey)
        two should respond (OK)

        val three = api3.query(Get(Ref(ref)), rootKey)
        three should respond (OK)
      }

      keys.result() foreach { ref =>
        val one = api1.query(Get(Ref(ref)), rootKey)
        one should respond (OK)

        val two = api2.query(Get(Ref(ref)), rootKey)
        two should respond (OK)

        val three = api3.query(Get(Ref(ref)), rootKey)
        three should respond (OK)
      }

      // re-test with replica 2 completely isolated.

      setReplication(
        admin2,
        "dc1" -> ReplicaTypeNames.Data,
        "dc2" -> ReplicaTypeNames.Log,
        "dc3" -> ReplicaTypeNames.Data) should respond(NoContent)

      eventually(timeout(scaled(5.seconds))) {
        val res = admin2.get("/admin/replication", rootKey)
        res should respond(OK)

        res.json should equal(
          JSObject("resource" -> JSObject("replicas" -> JSArray(
            JSObject("name" -> "dc1", "type" -> ReplicaTypeNames.Data),
            JSObject("name" -> "dc2", "type" -> ReplicaTypeNames.Log),
            JSObject("name" -> "dc3", "type" -> ReplicaTypeNames.Data)
          ))))
      }
      // wait a bit longer for the log topology revalidation to do its thing
      Thread.sleep(30_000)

      CoreLauncher.terminate(1, Gracefully)
      CoreLauncher.terminate(3, Gracefully)

      // replica 2 really does have the data.
      dbs.result() foreach { ref => api2.query(Get(Ref(ref)), rootKey) should respond (OK) }
      keys.result() foreach { ref => api2.query(Get(Ref(ref)), rootKey) should respond (OK) }
    }

    "filter scope" in {
      pending

      CoreLauncher.terminateAll()

      val apis = Seq(api1, api2, api3)

      def relaunch(replica: Int): Unit = {
        CoreLauncher.relaunch(replica, mcAPIVers)
        waitForPing(Seq(apis(replica - 1)))
      }

      def assertExist(ref: JSValue, secret: String): Unit = {
        val query = Get(ref)

        val one = api1.query(query, secret)
        one should respond (OK)

        val two = api2.query(query, secret)
        two should respond (OK)

        val three = api3.query(query, secret)
        three should respond (OK)
      }

      def waitReplication(ref: JSValue, key: String): Unit = {
        eventually(timeout(scaled(5.seconds))) {
          assertExist(ref, key)
        }
      }

      def repair(scope: Option[ScopeID] = None): Unit = {
        val filter = scope map { s => s"?filter_scope=${s.toLong}" } getOrElse ""
        admin1.post(s"/admin/repair_task$filter", JSObject.empty, rootKey)

        // maximum sleep time of the task executor is 5 mins.
        eventually(timeout(6.minutes), interval(5.seconds)) {
          val res = admin1.get("/admin/repair_task/status", rootKey)
          res should respond(OK)
          (res.resource / "pending").as[Boolean] should equal(true)
        }

        // time enough to sync Versions and rebuild indexes...
        eventually(timeout(5.minutes)) {
          val res = admin1.get("/admin/repair_task/status", rootKey)
          res should respond(OK)
          (res.resource / "pending").as[Boolean] should equal(false)
        }
      }

      CoreLauncher.launchMultiple(
        Seq(1, 2, 3),
        syncOnShutdown = true)

      admin1.post("/admin/init", JSObject("replica_name" -> "dc1"), rootKey)
      join(admin2, "dc2")
      join(admin3, "dc3")
      setReplication(admin1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log, "dc3" -> ReplicaTypeNames.Log) should respond (NoContent)

      eventually(timeout(scaled(3.minutes))) {
        val res = admin1.get("/admin/status", rootKey)
        res should respond (OK)

        val nodes = (res.json / "nodes").as[Seq[JSObject]]
        nodes foreach { node =>
          val owns = (node / "ownership").as[Float]
          owns should equal (1.0)
        }
      }

      waitForPing(Seq(api1, api2, api3))

      def api = Random.shuffle(apis).head
      val dbs = Seq.newBuilder[JSValue]
      val keys = Seq.newBuilder[JSValue]
      val docs = Seq.newBuilder[(JSValue, String)]

      val size = 5

      (0 until size) foreach { i =>
        val db = api.query(CreateDatabase(MkObject("name" -> s"monkey_stuff$i")), rootKey)
        db should respond (Created)
        dbs += db.refObj

        waitReplication(db.refObj, rootKey)

        val key = api.query(CreateKey(MkObject("database" -> db.refObj, "role" -> "admin")), rootKey)
        key should respond (Created)
        keys += key.refObj

        waitReplication(key.refObj, rootKey)

        val secret = (key.resource / "secret").as[String]

        val col = api.query(CreateCollection(MkObject("name" -> "col")), secret)
        col should respond (Created)

        waitReplication(col.refObj, secret)

        val idx = api.query(CreateIndex(MkObject("name" -> "idx", "source" -> col.refObj)), secret)
        idx should respond (Created)

        val doc = api.query(CreateF(col.refObj, MkObject()), secret)
        doc should respond (Created)

        docs += doc.refObj -> secret
      }

      dbs.result() foreach { ref =>
        assertExist(ref, rootKey)
      }

      keys.result() foreach { ref =>
        assertExist(ref, rootKey)
      }

      docs.result() foreach { case (ref, secret) =>
        assertExist(ClassRef("col"), secret)
        assertExist(IndexRef("idx"), secret)
        assertExist(ref, secret)
      }

      CoreLauncher.terminate(2, Gracefully)

      val keyspace = CoreLauncher.instanceDir(2) / "data" / "FAUNA"
      keyspace.deleteRecursively()

      relaunch(2)
      waitForPing(Seq(api2))

      // ensure replica 2 doesn't have any data
      eventually(timeout(scaled(1.minute))) {
        dbs.result() foreach { ref => api2.query(Get(ref), rootKey) should respond (BadRequest) }
      }

      eventually(timeout(scaled(1.minute))) {
        keys.result() foreach { ref => api2.query(Get(ref), rootKey) should respond (NotFound) }
      }

      eventually(timeout(scaled(1.minute))) {
        docs.result() foreach { case (ref, secret) =>
          api2.query(Get(ClassRef("col")), secret) should respond (Unauthorized)
          api2.query(Get(IndexRef("idx")), secret) should respond (Unauthorized)
          api2.query(Get(ref), secret) should respond (Unauthorized)
        }
      }

      repair(Some(ScopeID.RootID))

      setReplication(admin1, "dc1" -> ReplicaTypeNames.Data, "dc2" -> ReplicaTypeNames.Log, "dc3" -> ReplicaTypeNames.Data) should respond (NoContent)

      // re-test with replica 2 completely isolated.
      CoreLauncher.terminate(1, Gracefully)
      CoreLauncher.terminate(3, Gracefully)

      // prove that replica 2 only have data of root scope.
      eventually(timeout(scaled(1.minute))) {
        dbs.result() foreach { ref => api2.query(Get(ref), rootKey) should respond (OK) }
      }

      eventually(timeout(scaled(1.minute))) {
        keys.result() foreach { ref => api2.query(Get(ref), rootKey) should respond (OK) }
      }

      // sub-scopes was not repaired
      eventually(timeout(scaled(1.minute))) {
        docs.result() foreach { case (ref, secret) =>
          api2.query(Get(ClassRef("col")), secret) should respond (BadRequest)
          api2.query(Get(IndexRef("idx")), secret) should respond (BadRequest)
          api2.query(Get(ref), secret) should respond (BadRequest)
        }
      }

      // relaunch other instances and repair everything
      relaunch(1)
      relaunch(3)

      setReplication(admin1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log, "dc3" -> ReplicaTypeNames.Log) should respond (NoContent)

      repair()

      dbs.result() foreach { ref =>
        assertExist(ref, rootKey)
      }

      keys.result() foreach { ref =>
        assertExist(ref, rootKey)
      }

      docs.result() foreach { case (ref, secret) =>
        assertExist(ClassRef("col"), secret)
        assertExist(IndexRef("idx"), secret)
        assertExist(ref, secret)
      }
    }
  }
}
