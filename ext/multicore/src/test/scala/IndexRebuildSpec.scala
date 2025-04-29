package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.codex.json._
import fauna.lang.syntax._
import fauna.net.http.Http2Client
import fauna.prop.api.CoreLauncher.Gracefully
import fauna.prop.api.{CoreLauncher, DefaultQueryHelpers}
import fauna.storage.CassandraHelpers
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._

class IndexRebuildSpec extends Spec with Eventually with DefaultQueryHelpers {

  val admin1 = CoreLauncher.adminClient(1, mcAPIVers)
  val admin2 = CoreLauncher.adminClient(2, mcAPIVers)
  val admin3 = CoreLauncher.adminClient(3, mcAPIVers)

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)
  val api2 = CoreLauncher.apiClient(2, mcAPIVers)
  val api3 = CoreLauncher.apiClient(3, mcAPIVers)

  override protected def beforeAll(): Unit = {
    CoreLauncher.launchMultiple(
      Seq(1, 2, 3),
      syncOnShutdown = true,
      withDualWrite = true)

    init(admin1, "rep1")
    join(admin2, "rep2")
    join(admin3, "rep3")
    setReplication(
      admin1,
      "rep1" -> ReplicaTypeNames.Log,
      "rep2" -> ReplicaTypeNames.Log,
      "rep3" -> ReplicaTypeNames.Log) should respond(NoContent)

    eventually(timeout(scaled(3.minutes))) {
      val res = admin1.get("/admin/status", rootKey)
      res should respond(OK)

      val nodes = (res.json / "nodes").as[Seq[JSObject]]
      nodes foreach { node =>
        val owns = (node / "ownership").as[Float]
        owns should equal(1.0)
      }
    }

    waitForPing(Seq(api1, api2, api3))
  }

  def withNodesDown(apis: Seq[Http2Client])(f: Int => Unit): Unit = {
    val range = 1 to apis.size
    range foreach { CoreLauncher.terminate(_, Gracefully) }
    range foreach f
    range foreach { CoreLauncher.relaunch(_, mcAPIVers) }
    waitForPing(apis)
  }

  override protected def afterAll() = CoreLauncher.terminateAll()

  "Index rebuild" - {

    "restores replication" in {
      val apis = Seq(api1, api2, api3)

      // Create a database.
      val db = api1.query(CreateDatabase(MkObject("name" -> s"my_db")), rootKey)
      db should respond(Created)

      val key = api1.query(
        CreateKey(MkObject("database" -> Ref(db.ref), "role" -> "server")),
        rootKey)
      key should respond(Created)

      // Create a collection.
      val col = api1.query(CreateCollection(MkObject("name" -> s"my_collection")), rootKey)
      col should respond(Created)

      // Create indexes on the collection.
      val idx1 = api1.query(
        CreateIndex(MkObject(
          "name" -> "my_index1",
          "source" -> ClassRef("my_collection"),
          "terms"  -> JSArray(
            MkObject("field" -> JSArray("data", "foo"))),
          "values" -> JSArray(
            MkObject("field" -> JSArray("data", "bar"))))),
        rootKey)
      idx1 should respond(Created)

      val idx2 = api1.query(
        CreateIndex(MkObject(
          "name" -> "my_index2",
          "source" -> ClassRef("my_collection"),
          "terms"  -> JSArray(
            MkObject("field" -> JSArray("data", "bar"))))),
        rootKey)
      idx2 should respond(Created)

      // Create a document in the collection.
      val doc1 = api1.query(
        CreateF(ClassRef("my_collection"), MkObject("data" -> MkObject("foo" -> "x", "bar" -> "y"))),
        rootKey)
      doc1 should respond(Created)

      // Create a second identical document in the collection
      val doc2 = api1.query(
        CreateF(ClassRef("my_collection"), MkObject("data" -> MkObject("foo" -> "x", "bar" -> "y"))),
        rootKey)
      doc2 should respond(Created)

      // Add a UDF.
      val udf = api1.query(
        CreateFunction(MkObject("name" -> "fn", "body" -> QueryF(Lambda("_" -> "fubs")))),
        rootKey
      )
      udf should respond(Created)

      // Check that `ref` can be retrieved from each replica.
      def checkRef(apis: Seq[Http2Client], ref: String): Unit =
        apis foreach { api => api.query(Get(Ref(ref)), rootKey) should respond (OK) }

      def checkIdxs(apis: Seq[Http2Client], isDoc2Present: Boolean) = {
        apis foreach { api =>
          val docs = api.query(Paginate(Documents(ClassRef("my_collection"))), rootKey)
          docs should respond (OK)
          (docs.json / "resource" / "data").as[Seq[JSValue]].size shouldEqual {
            if (isDoc2Present) 2 else 1
          }

          val lookup1 = api.query(Paginate(Match(IndexRef("my_index1"), "x")), rootKey)
          val lookup2 = api.query(Paginate(Match(IndexRef("my_index2"), "y")), rootKey)

          lookup1 should respond (OK)
          lookup2 should respond (OK)

          val bar = (lookup1.json / "resource" / "data")
          val baz = (lookup2.json / "resource" / "data")

          try {
            bar.as[Seq[String]] shouldEqual {
              if (isDoc2Present) Seq("y", "y") else Seq("y")
            }
            (baz.as[Seq[JSValue]] contains doc1.refObj) shouldEqual true
            (baz.as[Seq[JSValue]] contains doc2.refObj) shouldEqual {
              if (isDoc2Present) true else false
            }
          } catch {
            // print raw json results on failure.
            case e: Throwable =>
              println(bar)
              println(baz)
              throw e
          }
        }
      }

      def checkUDF(apis: Seq[Http2Client]) = {
        apis foreach { api =>
          (api.query(Call(FunctionRef("fn")), rootKey)) should respond(OK)
        }
      }

      def checkAll(apis: Seq[Http2Client], isDoc2Present: Boolean): Unit =
        eventually(timeout(scaled(5.seconds))) {
          checkRef(apis, db.ref)
          checkRef(apis, key.ref)
          checkRef(apis, col.ref)
          checkRef(apis, doc1.ref)
          if (isDoc2Present) { checkRef(apis, doc2.ref) }
          checkRef(apis, idx1.ref)
          checkRef(apis, idx2.ref)
          checkRef(apis, udf.ref)
          checkIdxs(apis, isDoc2Present)
          checkUDF(apis)
        }

      // Do a sanity-check of everything we just wrote.
      println("sanity check")
      checkAll(apis, isDoc2Present = true)

      // Shut down replicas
      withNodesDown(apis) { node =>
        CoreLauncher.writeConfigFile(
          node,
          syncOnShutdown = true,
          withDualWrite = true)

        // Snapshot node for "offline" build and clear out secondary cfs.
        val dir = CoreLauncher.instanceDir(node)
        (dir / "data").copyRecursively(dir / "rebuild")

        val secondaryCFs = (dir / "data" / "FAUNA").entries.filter { _.toString contains "Index_2" }
        val nonVersions = (dir / "rebuild" / "FAUNA").entries.filterNot { _.toString contains "Versions" }
        val toDelete = secondaryCFs ++ nonVersions ++ Seq(
          dir / "rebuild" / "LOCALINDEXBUILD",
          dir / "rebuild" / "LOCALINDEXINDEXBUILD")

        toDelete foreach { _.deleteRecursively() }
      }

      // Add a post-snapshot write
      // remove doc2 to simulate live writes
      val remove = api1.query(
        RemoveVers(doc2.refObj, doc2.ts, "create"),
        rootKey)
      remove should respond (OK)
      checkAll(apis, isDoc2Present = false)

      println("Switching to rebuild snapshot")

      withNodesDown(apis) { node =>
        val dir = CoreLauncher.instanceDir(node)
        (dir / "data").move(dir / "orig")
        (dir / "rebuild").move(dir / "data")
      }

      val maxTaskID = {
        val res = api1.query(NextID(), rootKey)
        res should respond (OK)
        (res.json / "resource").as[String]
      }

      // Run rebuild phases, skipping versions replication repair
      Seq("lookups", "build-and-distribute-metadata-index", "build-local-index") foreach { phase =>
        println(s"Rebuild phase $phase")
        val cmd = Seq("index-rebuild", s"--max-task-id=$maxTaskID", s"--$phase")

        // FIXME: somehow, we are throwing ContentionExceptions for this.
        eventually(timeout(1.minute)) {
          CoreLauncher.adminCLI(1, cmd) shouldEqual 0
        }

        eventually(timeout(2.minutes)) {
          val res = admin1.get("/admin/tasks/list", rootKey)
          res should respond (OK)
          (res.json / "tasks").as[Seq[JSValue]].isEmpty shouldEqual true
        }

        // TODO: Wait between the build-and-distribute-metadata-index phase and the build-local-index phase.
        if (phase == "build-and-distribute-metadata-index") {
          println("Sleeping for distribute")
          Thread.sleep(10_000)
        }
      }

      println("Switching to host dataset")

      withNodesDown(apis) { node =>
        val dir = CoreLauncher.instanceDir(node)
        (dir / "data" / "LOCALINDEXBUILD").move(dir / "rebuild-out")
        (dir / "data").deleteRecursively()
        (dir / "orig").move(dir / "data")
      }

      println("Loading rebuild CFs")

      1 to 3 foreach { node =>
        val dir = CoreLauncher.instanceDir(node)
        val snapDir = (dir / "rebuild-out")
        CoreLauncher.adminCLI(1, Seq("load-snapshot", snapDir.toString))
      }

      // Wait for sstable loading to finish
      Thread.sleep(10_000)

      // Disable dual writes and flip index CFs.
      println("promoting rebuild CFs")
      withNodesDown(apis) { node =>
        CoreLauncher.writeConfigFile(
          node,
          syncOnShutdown = true,
          withDualWrite = false)
        CassandraHelpers.promoteIndex2CFs(CoreLauncher.instanceDir(node).toString)
      }

      // Check that indexes are there, as it was before.
      println("repair check")
      checkAll(apis, isDoc2Present = false)
    }

    "rowservice index2cf validation" in {

      pending

      /*
      commenting out pending above allows getting the following output in the log:

      2021-01-25T12:16:59,819 [service.RowService] WARN RowService index2CF validation, got different result for request (RegionID(0), (scope, index, terms)=(ScopeID(0),IndexID(2),Vector("name")), 2021-01-25T11:16:59.671Z, Vector(Slice(SortedIndex, Vector(([0x00 0x0D 0x81 0xCD 0x4A 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x00 0x00 0x08 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x01 0x01 0x00 0x00 0x08 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF],[0x00 0x0D 0x81 0xCD 0x4A 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x08 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01 0x01 0x00 0x00 0x08 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x01])), 4, false)))
      2021-01-25T12:16:59,990 [service.RowService] WARN RowService index2CF validation, got different result for request (RegionID(0), (scope, index, terms)=(ScopeID(0),IndexID(24),Vector(DocID(1024,C1), 6)), 2021-01-25T11:16:59.907Z, Vector(Slice(SortedIndex, Vector(([0x00 0x0D 0x81 0xCD 0x4A 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x08 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01 0x01 0x00 0x00 0x08 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x01],[0x00 0x0D 0x81 0xCD 0x4A 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x00 0x00 0x08 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x01 0x00 0x00 0x00 0x08 0x7F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF])), 65, true)))
       */

      CoreLauncher.launch(1, mcAPIVers, index2CFValidationRatio = 1.0)

      init(admin1, "rep1")
      waitForPing(Seq(api1))

      val col = api1.query(
        CreateClass(
          MkObject("name" -> "name")
        ),
        rootKey
      )
      col should respond(Created)
      println(s"col=${col.json}")

      val doc = api1.query(CreateF(col.refObj), rootKey)
      doc should respond(Created)
      println(s"doc=${doc.json}")

      // read from documents index
      val res = api1.query(Paginate(Documents(col.refObj)), rootKey)
      res should respond(OK)
      println(s"${res.json}")

      // check the logs for exceptions and/or proper output
    }
  }
}
