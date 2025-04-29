package fauna.api.test

import fauna.api.test._
import fauna.codex.json._
import fauna.flags.test._
import fauna.prop.api._
import fauna.prop.api.DefaultQueryHelpers._
import fauna.prop.Prop
import fauna.stats.QueryMetrics
import scala.concurrent.duration._

class FQL2RateLimitsSpec extends FQL2APISpec {
  val FlaggedAcctID = 123456
  val MaxReads = 5.0
  val MaxWrites = 5.0
  val MaxCompute = 10.0

  private[this] var id: Int = _
  private[this] var hostID: String = _
  private[this] var _client: FaunaDB.Client = _
  override def client = _client

  override def api = client.api
  override def admin = client.admin

  private[this] var tenantDB: Database = _
  private[this] var db: Database = _
  private[this] var coll: String = _

  private[this] var _flagVers: Int = 0
  private def flagVersion(): Int = {
    _flagVers += 1
    _flagVers
  }

  private def runCore(
    useFlags: Boolean = false,
    readLimit: Double = 100_000.0,
    writeLimit: Double = 100_000.0,
    computeLimit: Double = 100_000.0,
    burstSeconds: Double = 0.0
  ) = {
    val hostFlags = FlagProps(
      "host_id",
      hostID,
      Map(
        "rate_limits_enabled" -> true,
      ))

    var opsLimits: OpsLimits = OpsLimits(readLimit, writeLimit, computeLimit)
    var flags: FeatureFlags = FeatureFlags(flagVersion(), Vector(hostFlags))

    if (useFlags) {
      opsLimits = OpsLimits(100_000.0, 100_000.0, 100_000.0)
      flags = FeatureFlags(
        flagVersion(),
        Vector(
          hostFlags,
          FlagProps(
            "account_id",
            FlaggedAcctID,
            Map(
              "rate_limits_reads_per_second" -> readLimit,
              "rate_limits_writes_per_second" -> writeLimit,
              "rate_limits_compute_per_second" -> computeLimit)
          )
        )
      )
    }

    CoreLauncher.terminate(id)
    CoreLauncher.writeConfigFile(
      id,
      opsLimits = Some(opsLimits),
      burstSeconds = Some(burstSeconds),
      flags = Some(flags))
    CoreLauncher.launchConfigured(id, apiVers, withStdOutErr = enableCoreStdOut)
    waitForCore()
  }

  private def waitForCore() =
    eventually(timeout(scaled(10.minutes)), interval(1.seconds)) {
      client.api.get("/ping?scope=write", rootKey) should respond(OK)
    }

  override protected def beforeAll() = {
    val CoreLauncher.IDAndClient(i, cl) =
      CoreLauncher.launchOneNodeCluster(apiVers, withStdOutErr = enableCoreStdOut)
    id = i
    _client = cl

    waitForCore()

    hostID =
      (admin.get("/admin/status", rootKey).json / "nodes" / 0 / "host_id").as[String]

    tenantDB = aContainerDB(FlaggedAcctID).sample
    db = aDatabase(tenantDB).sample
    coll = aCollection(db).sample
  }

  override protected def afterAll() = CoreLauncher.delete(id)

  override protected def beforeEach() = {
    client.api.query(
      Update(
        tenantDB.refObj,
        MkObject(
          "account" -> MkObject(
            "id" -> FlaggedAcctID,
            "limits" -> JSNull
          ))),
      rootKey
    ) should respond(OK)
  }

  def aContainerDB(accountID: Long): Prop[Database] =
    aUniqueDBName map {
      FaunaDB.makeDB(
        _,
        client.api,
        apiVers,
        FaunaDB.rootKey,
        container = true,
        accountID = Some(accountID))
    }

  def aDatabase(parent: Database): Prop[Database] =
    aUniqueDBName map { FaunaDB.makeDB(_, client.api, "5", parent.adminKey) }

  "using defaults" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = Array.ofDim[Int](MaxReads.toInt * 2).mkString("[", ",", "]")
        val docsQuery = queryRaw(
          s"""|let arr = $docs
              |arr.map(_ => $coll.create({}))
              |""".stripMargin,
          db)
        docsQuery should respond(OK)
        val docIDs = (docsQuery.json / "data").as[Seq[JSObject]] map { _ / "id" }

        runCore(readLimit = MaxReads)

        val justEnoughReads = docIDs.take(MaxReads.toInt / 2).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnoughReads
              |arr.map(x => $coll.byId(x))
              |""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docIDs.mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooManyReads
              |arr.map(x => $coll.byId(x))
              |""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("read")
      }
    }

    once("writes") {
      Prop.const {
        runCore(writeLimit = MaxWrites)

        val justEnoughWrites = MaxWrites.toInt
        val justEnough = Array.ofDim[Int](justEnoughWrites).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnough
              |arr.map(_ => $coll.create({}))
              |null""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = Array.ofDim[Int](tooManyWrites).mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooMany
              |arr.map(_ => $coll.create({}))
              |null""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("write")
      }
    }

    once("compute") {
      Prop.const {
        runCore(computeLimit = MaxCompute)

        val justEnoughCompute =
          ((MaxCompute - 10) * QueryMetrics.BaselineCompute).toInt
        val justEnough = Array.ofDim[Int](justEnoughCompute).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnough
              |arr.map(_ => "")
              |null""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute + 10) * QueryMetrics.BaselineCompute).toInt
        val tooMany = Array.ofDim[Int](tooManyCompute).mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooMany
              |arr.map(x => x + 0)
              |null""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("compute")
      }
    }
  }

  "hard limits set by tenant DB" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = Array.ofDim[Int](MaxReads.toInt * 2).mkString("[", ",", "]")
        val docsQuery = queryRaw(
          s"""|let arr = $docs
              |arr.map(_ => $coll.create({}))
              |""".stripMargin,
          db)
        docsQuery should respond(OK)
        val docIDs = (docsQuery.json / "data").as[Seq[JSObject]] map { _ / "id" }

        client.api.query(
          Update(
            tenantDB.refObj,
            MkObject(
              "account" -> MkObject(
                "id" -> FlaggedAcctID,
                "limits" -> MkObject(
                  "read_ops" -> MkObject("hard" -> MaxReads)
                )
              ))
          ),
          rootKey
        ) should respond(OK)

        runCore() // restart to ensure the caches are clear

        val justEnoughReads = docIDs.take(MaxReads.toInt / 2).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnoughReads
              |arr.map(x => $coll.byId(x))
              |""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docIDs.mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooManyReads
              |arr.map(x => $coll.byId(x))
              |""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("read")
      }
    }

    once("writes") {
      Prop.const {
        client.api.query(
          Update(
            tenantDB.refObj,
            MkObject(
              "account" -> MkObject(
                "id" -> FlaggedAcctID,
                "limits" -> MkObject(
                  "write_ops" -> MkObject("hard" -> MaxWrites)
                )
              ))
          ),
          rootKey
        ) should respond(OK)

        runCore() // restart to ensure the caches are clear

        val justEnoughWrites = MaxWrites.toInt
        val justEnough = Array.ofDim[Int](justEnoughWrites).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnough
              |arr.map(_ => $coll.create({}))
              |null""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = Array.ofDim[Int](tooManyWrites).mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooMany
              |arr.map(_ => $coll.create({}))
              |null""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("write")
      }
    }

    once("compute") {
      Prop.const {
        client.api.query(
          Update(
            tenantDB.refObj,
            MkObject(
              "account" -> MkObject(
                "id" -> FlaggedAcctID,
                "limits" -> MkObject(
                  "compute_ops" -> MkObject("hard" -> MaxCompute)
                )
              ))
          ),
          rootKey
        ) should respond(OK)

        runCore() // restart to ensure the caches are clear

        val justEnoughCompute =
          ((MaxCompute - 10) * QueryMetrics.BaselineCompute).toInt
        val justEnough = Array.ofDim[Int](justEnoughCompute).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnough
              |arr.map(_ => "")
              |null""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute + 10) * QueryMetrics.BaselineCompute).toInt
        val tooMany = Array.ofDim[Int](tooManyCompute).mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooMany
              |arr.map(x => x + 0)
              |null""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("compute")
      }
    }
  }

  "set by account flags" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = Array.ofDim[Int](MaxReads.toInt * 2).mkString("[", ",", "]")
        val docsQuery = queryRaw(
          s"""|let arr = $docs
              |arr.map(_ => $coll.create({}))
              |""".stripMargin,
          db)
        docsQuery should respond(OK)
        val docIDs = (docsQuery.json / "data").as[Seq[JSObject]] map { _ / "id" }

        runCore(useFlags = true, readLimit = MaxReads)

        val justEnoughReads = docIDs.take(MaxReads.toInt / 2).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnoughReads
              |arr.map(x => $coll.byId(x))
              |""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docIDs.mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooManyReads
              |arr.map(x => $coll.byId(x))
              |""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("read")
      }
    }

    once("writes") {
      Prop.const {
        runCore(useFlags = true, writeLimit = MaxWrites)

        val justEnoughWrites = MaxWrites.toInt
        val justEnough = Array.ofDim[Int](justEnoughWrites).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnough
              |arr.map(_ => $coll.create({}))
              |null""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = Array.ofDim[Int](tooManyWrites).mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooMany
              |arr.map(_ => $coll.create({}))
              |null""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("write")
      }
    }

    once("compute") {
      Prop.const {
        runCore(useFlags = true, computeLimit = MaxCompute)

        val justEnoughCompute =
          ((MaxCompute - 10) * QueryMetrics.BaselineCompute).toInt
        val justEnough = Array.ofDim[Int](justEnoughCompute).mkString("[", ",", "]")
        val justEnoughQuery = queryRaw(
          s"""|let arr = $justEnough
              |arr.map(_ => "")
              |null""".stripMargin,
          db)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute + 10) * QueryMetrics.BaselineCompute).toInt
        val tooMany = Array.ofDim[Int](tooManyCompute).mkString("[", ",", "]")
        val tooManyQuery = queryRaw(
          s"""|let arr = $tooMany
              |arr.map(x => x + 0)
              |null""".stripMargin,
          db)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "error" / "code").as[String] should equal(
          "limit_exceeded")
        (tooManyQuery.json / "error" / "message").as[String] should equal(
          "Rate limit exceeded")
        (tooManyQuery.json / "stats" / "rate_limits_hit" / 0)
          .as[String] should equal("compute")
      }
    }
  }
}
