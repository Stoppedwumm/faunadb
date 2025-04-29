package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.flags.test._
import fauna.prop.api._
import fauna.prop.Prop
import fauna.stats._
import scala.concurrent.duration._

class RateLimitsSpec extends QueryAPI5Spec {
  val FlaggedAcctID = 123456
  val MaxReads = 5.0
  val MaxWrites = 5.0
  val MaxCompute = 1.0

  private[this] var id: Int = _
  private[this] var hostID: String = _
  private[this] var _client: FaunaDB.Client = _
  override def client = _client

  override def api = client.api
  override def admin = client.admin

  private[this] var tenantDB: Database = _
  private[this] var db: Database = _
  private[this] var cls: Collection = _

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

    tenantDB = aContainerDB(apiVers, FlaggedAcctID).sample
    db = aDatabase(apiVers, tenantDB).sample
    cls = aCollection(db).sample
  }

  override protected def afterAll() = CoreLauncher.delete(id)

  override protected def beforeEach() = {
    runRawQuery(
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

  "using defaults" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = (0 to (MaxReads.toInt * 2)).map(_ => JSLong(0)).toIndexedSeq
        val docsQuery = runRawQuery(
          Do(MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(docs: _*))),
          db.adminKey)
        docsQuery should respond(OK)
        val docRefs = (docsQuery.json / "resource").as[Seq[JSObject]] map {
          _ / "ref"
        }

        runCore(readLimit = MaxReads)

        val justEnoughReads = docRefs.take(MaxReads.toInt / 2).map(Get(_))
        val justEnoughQuery =
          runRawQuery(Do(JSArray(justEnoughReads: _*), JSNull), db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docRefs.map(Get(_))
        val tooManyQuery =
          runRawQuery(Do(JSArray(tooManyReads: _*), JSNull), db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for read exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "read"
      }
    }

    once("writes") {
      Prop.const {
        runCore(writeLimit = MaxWrites)

        val justEnoughWrites = (MaxWrites - 1).toInt
        val justEnough = (0 to justEnoughWrites).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(justEnough: _*)),
              JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = (0 to tooManyWrites).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(tooMany: _*)),
              JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for write exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "write"
      }
    }

    once("compute") {
      Prop.const {
        runCore(computeLimit = MaxCompute)

        val justEnoughCompute =
          ((MaxCompute - 10) * QueryMetrics.BaselineCompute).toInt
        val justEnough = (0 to justEnoughCompute).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(MapF(Lambda("_" -> ""), JSArray(justEnough: _*)), JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute + 1) * QueryMetrics.BaselineCompute).toInt
        val tooMany = (0 to tooManyCompute).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(MapF(Lambda("x" -> AddF(Var("x"), 1)), JSArray(tooMany: _*)), JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for compute exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "compute"
      }
    }
  }

  "hard limits set by tenant DB" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = (0 to (MaxReads.toInt * 2)).map(_ => JSLong(0)).toIndexedSeq
        val docsQuery = runRawQuery(
          Do(MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(docs: _*))),
          db.adminKey)
        docsQuery should respond(OK)
        val docRefs = (docsQuery.json / "resource").as[Seq[JSObject]] map {
          _ / "ref"
        }

        runRawQuery(
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

        val justEnoughReads = docRefs.take(MaxReads.toInt / 2).map(Get(_))
        val justEnoughQuery =
          runRawQuery(Do(JSArray(justEnoughReads: _*), JSNull), db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docRefs.map(Get(_))
        val tooManyQuery =
          runRawQuery(Do(JSArray(tooManyReads: _*), JSNull), db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for read exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "read"
      }
    }

    once("writes") {
      Prop.const {
        runRawQuery(
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

        val justEnoughWrites = (MaxWrites - 1).toInt
        val justEnough = (0 to justEnoughWrites).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(justEnough: _*)),
              JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = (0 to tooManyWrites).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(tooMany: _*)),
              JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for write exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "write"
      }
    }

    once("compute") {
      Prop.const {
        runRawQuery(
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
        val justEnough = (0 to justEnoughCompute).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(MapF(Lambda("_" -> ""), JSArray(justEnough: _*)), JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute + 1) * QueryMetrics.BaselineCompute).toInt
        val tooMany = (0 to tooManyCompute).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(MapF(Lambda("x" -> AddF(Var("x"), 1)), JSArray(tooMany: _*)), JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for compute exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "compute"
      }
    }
  }

  "soft limits set by tenant DB" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = (0 to (MaxReads.toInt * 2)).map(_ => JSLong(0)).toIndexedSeq
        val docsQuery = runRawQuery(
          Do(MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(docs: _*))),
          db.adminKey)
        docsQuery should respond(OK)
        val docRefs = (docsQuery.json / "resource").as[Seq[JSObject]] map {
          _ / "ref"
        }

        runRawQuery(
          Update(
            tenantDB.refObj,
            MkObject(
              "account" -> MkObject(
                "id" -> FlaggedAcctID,
                "limits" -> MkObject(
                  "read_ops" -> MkObject("soft" -> MaxReads)
                )
              ))
          ),
          rootKey
        ) should respond(OK)

        runCore() // restart to ensure the caches are clear

        val justEnoughReads = docRefs.take(MaxReads.toInt / 2).map(Get(_))
        val justEnoughQuery =
          runRawQuery(Do(JSArray(justEnoughReads: _*), JSNull), db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docRefs.map(Get(_))
        val tooManyQuery =
          runRawQuery(Do(JSArray(tooManyReads: _*), JSNull), db.adminKey)
        tooManyQuery should respond(OK)
        tooManyQuery.header("x-rate-limited-ops") shouldBe "read"
      }
    }

    once("writes") {
      Prop.const {
        runRawQuery(
          Update(
            tenantDB.refObj,
            MkObject(
              "account" -> MkObject(
                "id" -> FlaggedAcctID,
                "limits" -> MkObject(
                  "write_ops" -> MkObject("soft" -> MaxWrites / 2)
                )
              ))
          ),
          rootKey
        ) should respond(OK)

        runCore() // restart to ensure the caches are clear

        val justEnoughWrites = (MaxWrites - 1).toInt
        val justEnough = (0 to justEnoughWrites).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(justEnough: _*)),
              JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = (0 to tooManyWrites).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(tooMany: _*)),
              JSNull),
            db.adminKey)
        tooManyQuery should respond(OK)

        // Writes work differently than reads/compute: read and
        // compute ops will be incrementally taken throughout
        // execution of the query, while writes are accumulated and
        // permits acquired in a single batch. Therefore, the soft
        // limit applies on the next query.
        val softLimitWrites = (MaxWrites + 1).toInt
        val softMany = (0 to softLimitWrites).map(_ => JSLong(0)).toIndexedSeq
        val softManyQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(softMany: _*)),
              JSNull),
            db.adminKey)
        softManyQuery should respond(OK)
        softManyQuery.header("x-rate-limited-ops") shouldBe "write"
      }
    }

    once("compute") {
      Prop.const {
        runRawQuery(
          Update(
            tenantDB.refObj,
            MkObject(
              "account" -> MkObject(
                "id" -> FlaggedAcctID,
                "limits" -> MkObject(
                  "compute_ops" -> MkObject("soft" -> MaxCompute)
                )
              ))
          ),
          rootKey
        ) should respond(OK)

        runCore() // restart to ensure the caches are clear

        val justEnoughCompute = (MaxCompute * QueryMetrics.BaselineCompute / 2).toInt
        val justEnough = (0 to justEnoughCompute).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(MapF(Lambda("_" -> ""), JSArray(justEnough: _*)), JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute * QueryMetrics.BaselineCompute) - 1).toInt
        val tooMany = (0 to tooManyCompute).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(MapF(Lambda("x" -> AddF(Var("x"), 1)), JSArray(tooMany: _*)), JSNull),
            db.adminKey)
        tooManyQuery should respond(OK)
        tooManyQuery.header("x-rate-limited-ops") shouldBe "compute"
      }
    }
  }

  "set by account flags" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = (0 to (MaxReads.toInt * 2)).map(_ => JSLong(0)).toIndexedSeq
        val docsQuery = runRawQuery(
          Do(MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(docs: _*))),
          db.adminKey)
        docsQuery should respond(OK)
        val docRefs = (docsQuery.json / "resource").as[Seq[JSObject]] map {
          _ / "ref"
        }

        runCore(useFlags = true, readLimit = MaxReads)

        val justEnoughReads = docRefs.take(MaxReads.toInt / 2).map(Get(_))
        val justEnoughQuery =
          runRawQuery(Do(JSArray(justEnoughReads: _*), JSNull), db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyReads = docRefs.map(Get(_))
        val tooManyQuery =
          runRawQuery(Do(JSArray(tooManyReads: _*), JSNull), db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for read exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "read"
      }
    }

    once("writes") {
      Prop.const {
        runCore(useFlags = true, writeLimit = MaxWrites)

        val justEnoughWrites = (MaxWrites - 1).toInt
        val justEnough = (0 to justEnoughWrites).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(justEnough: _*)),
              JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyWrites = (MaxWrites + 1).toInt
        val tooMany = (0 to tooManyWrites).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(tooMany: _*)),
              JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for write exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "write"
      }
    }

    once("compute") {
      Prop.const {
        runCore(useFlags = true, computeLimit = MaxCompute)

        val justEnoughCompute =
          ((MaxCompute - 10) * QueryMetrics.BaselineCompute).toInt
        val justEnough = (0 to justEnoughCompute).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(MapF(Lambda("_" -> ""), JSArray(justEnough: _*)), JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val tooManyCompute = ((MaxCompute + 1) * QueryMetrics.BaselineCompute).toInt
        val tooMany = (0 to tooManyCompute).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(MapF(Lambda("x" -> AddF(Var("x"), 1)), JSArray(tooMany: _*)), JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for compute exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "compute"
      }
    }
  }

  "hard limit burst" / {
    once("reads") {
      Prop.const {
        runCore()

        val docs = (0 to (MaxReads.toInt * 2)).map(_ => JSLong(0)).toIndexedSeq
        val docsQuery = runRawQuery(
          Do(MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(docs: _*))),
          db.adminKey)
        docsQuery should respond(OK)
        val docRefs = (docsQuery.json / "resource").as[Seq[JSObject]] map {
          _ / "ref"
        }

        runCore(useFlags = true, readLimit = MaxReads, burstSeconds = 1.0)

        // Creates the limiters in cache.
        val justEnoughReads = docRefs.take(MaxReads.toInt / 2).map(Get(_))
        val justEnoughQuery =
          runRawQuery(Do(JSArray(justEnoughReads: _*), JSNull), db.adminKey)
        justEnoughQuery should respond(OK)

        // Fill up the burst.
        Thread.sleep(2000)

        // Burst reads are simple to test, permits are requested one at a time
        // within a request so we build it up to burst capacity.
        val burstReads = docRefs.map(Get(_))
        val burstQuery =
          runRawQuery(Do(JSArray(burstReads: _*), JSNull), db.adminKey)
        burstQuery should respond(OK)
        burstQuery.header("x-rate-limited-ops") shouldBe "read"

        // Now both limiters are spent, we should only get 429s.
        val tooManyReads = docRefs.map(Get(_))
        val tooManyQuery =
          runRawQuery(Do(JSArray(tooManyReads: _*), JSNull), db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for read exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "read"
      }
    }

    once("writes") {
      Prop.const {
        runCore(useFlags = true, writeLimit = MaxWrites, burstSeconds = 1.0)

        // Creates the limiters in cache.
        val justEnoughWrites = (MaxWrites - 1).toInt
        val justEnough = (0 to justEnoughWrites).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(
              MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(justEnough: _*)),
              JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        // Refill the limiters.
        Thread.sleep(2000)

        // Test that we can use burst.
        // The test is complicated because of how the rate limiter works. If there
        // are available permits, a request can take as many permits as it wants
        // in one go. Reads will request 1 permit at a time so it's less subject
        // to this. Writes OTOH are requested in one shot. So we do two iterations:
        // the first one will empty the soft limiter and the second iteration will
        // notice it. The second iteration only succeeds because of the burst in
        // the hard limiter.
        for (n <- 0 to 1) {
          val burstWrites = (MaxWrites + 1).toInt
          val burst = (0 until burstWrites).map(_ => JSLong(0)).toIndexedSeq
          val burstQuery =
            runRawQuery(
              Do(
                MapF(Lambda("_" -> CreateF(cls.refObj)), JSArray(burst: _*)),
                JSNull),
              db.adminKey)
          burstQuery should respond(OK)
          // We only use burst on the second iteration.
          if (n == 1) {
            burstQuery.header("x-rate-limited-ops") shouldBe "write"
          }
        }

        // Now both limiters are spent, we should only get 429s.
        val tooManyQuery =
          runRawQuery(Do(CreateF(cls.refObj), JSNull), db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for write exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "write"
      }
    }

    once("compute") {
      Prop.const {
        runCore(useFlags = true, computeLimit = MaxCompute, burstSeconds = 1.0)

        val justEnoughCompute =
          ((MaxCompute - 10) * QueryMetrics.BaselineCompute).toInt
        val justEnough = (0 to justEnoughCompute).map(_ => JSLong(0)).toIndexedSeq
        val justEnoughQuery =
          runRawQuery(
            Do(MapF(Lambda("_" -> ""), JSArray(justEnough: _*)), JSNull),
            db.adminKey)
        justEnoughQuery should respond(OK)

        Thread.sleep(2000)

        val burstCompute = (MaxCompute * QueryMetrics.BaselineCompute / 2).toInt
        val burst = (0 to burstCompute).map(_ => JSLong(0)).toIndexedSeq
        val burstQuery =
          runRawQuery(
            Do(MapF(Lambda("x" -> AddF(Var("x"), 1)), JSArray(burst: _*)), JSNull),
            db.adminKey)
        burstQuery should respond(OK)
        burstQuery.header("x-rate-limited-ops") shouldBe "compute"

        val tooManyCompute = ((MaxCompute + 1) * QueryMetrics.BaselineCompute).toInt
        val tooMany = (0 to tooManyCompute).map(_ => JSLong(0)).toIndexedSeq
        val tooManyQuery =
          runRawQuery(
            Do(MapF(Lambda("x" -> AddF(Var("x"), 1)), JSArray(tooMany: _*)), JSNull),
            db.adminKey)
        tooManyQuery should respond(TooManyRequests)
        (tooManyQuery.json / "errors" / 0 / "description").as[String] should equal(
          "Rate limit for compute exceeded")
        tooManyQuery.header("x-rate-limited-ops") shouldBe "compute"
      }
    }
  }
}
