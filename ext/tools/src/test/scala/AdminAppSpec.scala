package fauna.tools.test

import fauna.api.ReplicaTypeNames
import fauna.atoms.APIVersion
import fauna.codex.json._
import fauna.config.CoreConfig
import fauna.net.http.HttpClient
import fauna.prop.api.CoreLauncher
import fauna.storage.Storage
import fauna.tools._
import java.io._
import java.nio.file.{ Files, Path, Paths }
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Random

class AdminAppSpec extends Spec {

  val mainClass = "fauna.tools.Admin"

  val apiVers = "2.0"
  var api1: HttpClient = _
  var admin1: HttpClient = _

  var api2: HttpClient = _
  var admin2: HttpClient = _

  var tmpdir: Path = null
  var cPath: Path = null

  val one = CoreLauncher.nextInstanceID()
  val two = CoreLauncher.nextInstanceID()

  val sampleConfig =
    s"""---
       |auth_root_key: "secret"
        """.stripMargin

  override def beforeAll() = {
    launchCluster()
    createTemps()
  }

  override def afterAll() = {
    CoreLauncher.terminate(one)
    CoreLauncher.terminate(two)
    removeTemps()
  }

  def launchCluster() = {
    CoreLauncher.launchMultiple(Seq(one, two))

    api1 = CoreLauncher.apiClient(one, apiVers)
    api2 = CoreLauncher.apiClient(two, apiVers)

    admin1 = CoreLauncher.adminClient(one, apiVers)
    admin2 = CoreLauncher.adminClient(two, apiVers)

    val init = admin1.post("/admin/init", JSObject("replica_name" -> "dc1"), rootKey)
    init should respond(OK)

    eventually(timeout(scaled(3.minutes))) {
      val res = api1.get("/ping?scope=node", rootKey)
      res should respond(OK)
    }

    val join = admin2.post(
      "/admin/join",
      JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc2"),
      rootKey)
    join should respond(OK)

    eventually(timeout(scaled(3.minutes))) {
      val res = api2.get("/ping?scope=node", rootKey)
      res should respond(OK)
    }

    // Adjusting replication too quickly after nodes start up
    // and join a cluster can trigger a race that gets a log segment stuck.
    // Sleeping for a while is a crude but effective workaround.
    Thread.sleep(30000)

    val replicas = admin1.put(
      "/admin/replication",
      JSObject(
        "replicas" -> JSArray(
          JSObject("name" -> "dc1", "type" -> ReplicaTypeNames.Log),
          JSObject("name" -> "dc2", "type" -> ReplicaTypeNames.Log))),
      rootKey
    )
    replicas should respond(NoContent)

    eventually(timeout(scaled(3.minutes))) {
      val res = admin1.get("/admin/status", rootKey)
      res should respond(OK)

      val nodes = (res.json / "nodes").as[Seq[JSObject]]
      nodes foreach { node =>
        val owns = (node / "ownership").as[Float]
        owns should equal(1.0)
      }
    }
  }

  def createTemps(): Unit = {
    // A YAML file must already be present.
    tmpdir = Files.createTempDirectory("fauna-test-adminspec")
    cPath = {
      val f = new File(tmpdir.toString + "/faunadb.yml")

      val os = new OutputStreamWriter(new FileOutputStream(f))
      try {
        os.write(sampleConfig)
      } finally {
        os.close()
      }

      f.toPath
    }
  }

  def removeTemps(): Unit = {
    Files.deleteIfExists(cPath)
    Files.deleteIfExists(tmpdir)
  }

  val to = 5 seconds

  "Admin application" - {
    "has -k flag in the help listing" in {
      val flags = Map("-help" -> "")
      val (exitCode, stdout) = Await.result(launchTool(mainClass, flags, Nil), to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("--key <arg>")) should equal(true)
    }

    "will print COMMANDS list in the help listing" in {
      val flags = Map("-help" -> "")
      val (exitCode, stdout) = Await.result(launchTool(mainClass, flags, Nil), to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("COMMANDS")) should equal(true)
    }

    "without a config file but a key executes a valid command" in {
      val flags = Map("-k" -> "secret")
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("dc1")) should equal(true)
    }

    "should let us specify a host" in {
      val flags = Map("-c" -> cPath.toString, "-host" -> "127.0.0.1")
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      stdout.anyMatch(s => s.contains("dc1")) should equal(true)
      exitCode should equal(0)
    }

    "should let us specify two per-app CLI values" in {
      // context for this patch: previously: we specified per-app flags as an
      // a.c.OptionGroup, which only allowed one in the group to be set.
      // Note that we do not need the config file path passed in, as we are passing
      //  the secret in directly.
      val flags = Map("-k" -> "secret", "-host" -> "127.0.0.1")
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("dc1")) should equal(true)
    }

    "should prioritise CLI arguments over config" in {
      // By overriding -k with the wrong value, we should see an unauthorized
      // response.
      val flags = Map("-c" -> cPath.toString, "-k" -> "a different secret")
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(1)
      stdout.anyMatch(s => s.contains("Unauthorized")) should equal(true)
    }

    "without a config file, nor a key, complains" in {
      val flags = Map.empty[String, String]
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(1)
      stdout.anyMatch(s => s.contains("Unauthorized")) should equal(true)
    }

    "with a config file, executes a valid command" in {
      val flags = Map("-c" -> cPath.toString)
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("dc1")) should equal(true)
    }

    "with an invalid command, complains" in {
      val flags = Map("-c" -> cPath.toString)
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("xyzzy")), to)
      exitCode should equal(1)
      stdout.anyMatch(s => s.contains("Invalid command")) should equal(true)
    }

    "with an invalid path, complains" in {
      val f = new File(tmpdir.toString + "/cannotexist.yml")

      val flags = Map("-c" -> f.toPath.toString)
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(1)
      stdout.anyMatch(s => s.contains("Error loading configuration")) should equal(
        true)
    }

    "with FAUNADB_CONFIG, executes a valid command" in {
      val flags = Map.empty[String, String]
      val (exitCode, stdout) = Await.result(
        launchTool(mainClass, flags, List("status"), config = Some(cPath.toString)),
        to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("dc1")) should equal(true)
    }

    "should prioritise CLI arguments over env variable" in {
      val f = new File(tmpdir.toString + "/cannotexist.yml")

      val flags = Map("-c" -> cPath.toString)
      val (exitCode, stdout) = Await.result(
        launchTool(
          mainClass,
          flags,
          List("status"),
          config = Some(f.toPath.toString)),
        to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("dc1")) should equal(true)
    }

    "does not emit log4j warnings " in {
      val flags = Map("-c" -> cPath.toString)
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List("status")), to)
      exitCode should equal(0)
      stdout.anyMatch(s => s.contains("log4j:")) should equal(false)
    }

    "should only emit key output on stdout" in {
      val cmd = "host-version" // Only prints a single line of output.
      val flags = Map("-c" -> cPath.toString)
      val (exitCode, stdout) =
        Await.result(launchTool(mainClass, flags, List(cmd), andStderr = false), to)
      exitCode should equal(0)
      stdout.count() should equal(1)
    }

    "takes and restores a snapshot" in {
      val backupKey = Random.alphanumeric take 8 mkString ""
      val databaseName = Random.alphanumeric take 8 mkString ""

      @annotation.tailrec
      def clearDBs(): Unit = {
        val res = api1.query(
          Foreach(
            Lambda("db" -> DeleteF(Var("db"))),
            Paginate(Ref("databases"), size = 1000)),
          rootKey)

        res should respond(OK)
        if (!(res.resource / "data").as[JSArray].isEmpty) {
          clearDBs()
        }
      }
      clearDBs()

      val dbRes =
        api1.query(
          CreateF(Ref("databases"), MkObject("name" -> databaseName)),
          rootKey)
      dbRes should respond(Created)
      (dbRes.resource / "name").as[String] should equal(databaseName)

      val dbKey = (api1
        .query(
          CreateF(
            Ref("keys"),
            MkObject("database" -> Ref(dbRes.ref), "role" -> "server")),
          rootKey)
        .resource / "secret").as[String]
      val userRes =
        api1.query(CreateF(Ref("classes"), MkObject("name" -> "users")), dbKey)
      userRes should respond(Created)

      val userEvents = api1.get(s"/${userRes.ref}/events", dbKey)
      userEvents should respond(OK)

      AdminCommands
        .CreateSnapshot(admin1, rootKey, backupKey, false)
        .apply() should be(true)
      AdminCommands.ShowSnapshots(admin1, rootKey, false).apply() should be(true)

      val config =
        CoreConfig.load(Some(Paths.get(CoreLauncher.configFile(1).toString))).config
      val path = Option(config.storage_snapshot_path)
        .getOrElse(s"${config.storagePath}/snapshots")

      val backupPath = s"$path/$backupKey"

      // Ensure the backup directory has data
      new File(backupPath).list.length > 0 should be(true)

      AdminCommands
        .LoadSnapshot(admin2, rootKey, backupPath, None, false)
        .apply() should be(true)

      val dbResDest = api2.get(s"/databases/$databaseName", rootKey)
      dbResDest should respond(OK)
    }

    "Validate duplicate snapshot name errors" in {
      val backupKey = Random.alphanumeric take 8 mkString ""
      val databaseName = Random.alphanumeric take 8 mkString ""

      @annotation.tailrec
      def clearDBs(): Unit = {
        val res = api1.query(
          Foreach(
            Lambda("db" -> DeleteF(Var("db"))),
            Paginate(Ref("databases"), size = 1000)),
          rootKey)

        res should respond(OK)
        if (!(res.resource / "data").as[JSArray].isEmpty) {
          clearDBs()
        }
      }
      clearDBs()

      val dbRes =
        api1.query(
          CreateF(Ref("databases"), MkObject("name" -> databaseName)),
          rootKey)
      dbRes should respond(Created)
      (dbRes.resource / "name").as[String] should equal(databaseName)

      val dbKey = (api1
        .query(
          CreateF(
            Ref("keys"),
            MkObject("database" -> Ref(dbRes.ref), "role" -> "server")),
          rootKey)
        .resource / "secret").as[String]
      val userRes =
        api1.query(CreateF(Ref("classes"), MkObject("name" -> "users")), dbKey)
      userRes should respond(Created)

      val userEvents = api1.get(s"/${userRes.ref}/events", dbKey)
      userEvents should respond(OK)

      AdminCommands
        .CreateSnapshot(admin1, rootKey, backupKey, false)
        .apply() should be(true)
      AdminCommands
        .CreateSnapshot(admin1, rootKey, backupKey, false)
        .apply() should be(false)
      AdminCommands.ShowSnapshots(admin1, rootKey, false).apply() should be(true)

      val config =
        CoreConfig.load(Some(Paths.get(CoreLauncher.configFile(1).toString))).config
      val path = Option(config.storage_snapshot_path)
        .getOrElse(s"${config.storagePath}/snapshots")

      AdminCommands
        .LoadSnapshot(admin2, rootKey, s"$path/$backupKey", None, false)
        .apply() should be(true)

      val dbResDest = api2.get(s"/databases/$databaseName", rootKey)
      dbResDest should respond(OK)
    }

    "Try an invalid backup name" in {
      val backupKey = "backup*123"
      AdminCommands
        .CreateSnapshot(admin1, rootKey, backupKey, false)
        .apply() should be(false)
    }

    "communicates over SSL" in {
      // Use a large number that won't conflict with other tests.
      val inst = 10
      CoreLauncher.launch(inst, APIVersion.Default.toString, withAdminSSL = true)

      val config = CoreConfig
        .load(Some(Paths.get(CoreLauncher.configFile(inst).toString)))
        .config
      val admin = CoreLauncher
        .client(inst, APIVersion.Default.toString, ssl = Some(config.adminSSL))
        .admin

      AdminCommands.Init(admin, rootKey, "secureDC", false).apply() should be(true)
    }

    "allows index2cf gc grace fiddling" in {
      CoreLauncher.adminCLI(1, Seq("get-index2cf-gc-grace-period")) should be(0)
      CoreLauncher.adminCLI(1, Seq("set-index2cf-gc-grace-period", "3")) should be(0)

      val res = admin1.get("/admin/index2cf-gc-grace", rootKey)
      res should respond(OK)
      res.json should equal(
        JSObject(
          "HistoricalIndex_2_gc_grace_days" -> 3,
          "SortedIndex_2_gc_grace_days" -> 3))
    }

    "sets gc grace period" in {
      def checkGCGrace(days: Long) = {
        val res = admin1.get("/admin/storage/get-gc-grace", rootKey)
        res should respond(OK)
        val v = res.json / "Versions"
        (v / "days").as[Long] shouldEqual days
        (v / "seconds").as[Long] shouldEqual days * 24 * 60 * 60
      }

      checkGCGrace(Storage.StandardGCGrace.toDays)

      admin1.post(
        "/admin/storage/set-extended-gc-grace",
        JSObject(),
        rootKey) should respond(NoContent)

      Thread.sleep(500)

      checkGCGrace(Storage.ExtendedGracePeriod.toDays)

      admin1.post(
        "/admin/storage/set-standard-gc-grace",
        JSObject(),
        rootKey) should respond(NoContent)

      Thread.sleep(500)

      checkGCGrace(Storage.StandardGCGrace.toDays)
    }
  }
}
