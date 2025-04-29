package fauna.api.test

import fauna.api.fql1.APIResponse
import fauna.api.fql2.FQL2Endpoint
import fauna.api.util.RequestLogging
import fauna.api.APIEndpoint
import fauna.atoms._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.LoggingConfig
import fauna.model.runtime.fql2.RedactedQuery
import fauna.net.http._
import fauna.snowflake.IDSource
import fauna.stats.{ QueryMetrics, StatsRequestBuffer }
import fauna.trace.{ QueryLogContext, QueryLogTags, TraceContext }
import io.netty.handler.codec.http.HttpMethod
import java.lang.{ Long => JLong }
import java.net.{ InetSocketAddress, URI }
import scala.collection.immutable.SeqMap
import scala.concurrent.duration.Duration
import scala.concurrent.Await

class RequestLoggingSpec extends Spec {

  val memoryLogger = new MemoryLogger
  val body = Body("""{ "new_id": null }""", ContentType.JSON)
  // a crude approx. of CoreApplication
  val jsBody = JSON.parse[JSValue](Await.result(body.data, Duration.Inf))

  object DummyApp extends RequestLogging {

    private[this] val logger = memoryLogger

    val networkHostID = "169.254.1.1"
    val replicaName = Some("us-west-1")
    val messageIDSource = new IDSource(() => 42)

    override protected def queryLog = logger
    override protected def slowLog = logger

    override def logRequest(
      req: Option[HttpRequest],
      res: APIEndpoint.Response,
      info: HttpRequestChannelInfo,
      ctx: Option[QueryLogContext] = None,
      auth: Option[JSValue] = None,
      body: Option[JSValue] = None,
      errors: Option[JSValue] = None,
      contentionInfo: Option[String] = None,
      statsBuf: Option[StatsRequestBuffer] = None,
      responseInfo: Option[APIEndpoint.ResponseInfo] = None,
      accountID: Option[String] = None,
      databaseGlobalIDHierarchy: Option[Seq[String]] = None,
      databaseNamePath: Option[Seq[String]] = None
    ) =
      super.logRequest(
        req,
        res,
        info,
        ctx,
        auth,
        body,
        errors,
        contentionInfo,
        statsBuf,
        responseInfo,
        accountID,
        databaseGlobalIDHierarchy,
        databaseNamePath
      )
  }

  before {
    LoggingConfig.slowQueryThreshold = 5000
    memoryLogger.reset()
  }

  val channelInfo = HttpRequestChannelInfo(new InetSocketAddress("localhost", 80))

  def parseMsg(msg: String): JSObject = {
    val buf = msg.toUTF8Buf

    try {
      JSON.parse[JSValue](buf).as[JSObject]
    } finally {
      buf.release()
    }
  }

  def checkBasics(code: Int, msg: JSObject) = {
    (msg / "response" / "code") should equal(JSLong(code))
    (msg / "ip") should equal(JSString("127.0.0.1"))
    (msg / "host") should equal(JSString(DummyApp.networkHostID))
    (msg / "replica") should equal(JSString(DummyApp.replicaName.get))
  }

  "RequestLogging" - {
    "works" in {
      val auth = JSObject("password" -> "sekritsquirrel")
      val headers = Seq(
        HTTPHeaders.FaunaDriver -> "driver",
        HTTPHeaders.FaunaSource -> "source",
        HTTPHeaders.MaxRetriesOnContention -> JLong.valueOf(2),
        HTTPHeaders.QueryTimeout -> JLong.valueOf(30),
        HTTPHeaders.ForwardedFor -> "10.0.0.1",
        HTTPHeaders.UserAgent -> "user agent",
        HTTPHeaders.RuntimeEnv -> "Netlify",
        HTTPHeaders.RuntimeEnvOS -> "linux",
        HTTPHeaders.RuntimeNodeEnvVersion -> "7.5.3",
        HTTPHeaders.DriverEnv -> "driver=javascript-4.0.1;runtime=chrome-89;env=Netlify;os=linux",
        HTTPHeaders.FaunaStreamID -> "1234",
        HTTPHeaders.PerformanceHints -> "true",
        HTTPHeaders.Typecheck -> "true",
        HTTPHeaders.Linearized -> "true",
        HTTPHeaders.LastSeenTxn -> JLong.valueOf(1234),
        HTTPHeaders.LastTxnTs -> JLong.valueOf(1234)
      )

      val req = HttpRequest(
        HttpMethod.POST,
        new URI("/"),
        headers,
        body
      )
      val resp = APIEndpoint.Response(
        APIResponse.EmptyResponse(200, headers).toHTTPResponse,
        redacted = Some(
          RedactedQuery(
            redacted = "i'm the redacted query",
            shape = "i'm the query shape")))
      val contentionInfo = "page_hits"
      val queryTime = 1234
      val metrics = QueryMetrics(queryTime, new StatsRequestBuffer)
      val responseInfo =
        APIEndpoint.ResponseInfo.Null
          .copy(txnTime = Timestamp.ofMicros(42), metrics = metrics)

      val accountID = 1234.toString()
      val gID1 = 12345.toString()
      val gID2 = 45678.toString()
      val dbGlobalIDPath = Seq(gID1, gID2)
      val dbName1 = "db-1"
      val dbName2 = "db-2"

      DummyApp.logRequest(
        Some(req),
        resp,
        channelInfo,
        None,
        Some(auth),
        Some(jsBody),
        None,
        Some(contentionInfo),
        responseInfo = Some(responseInfo),
        accountID = Some(accountID),
        databaseGlobalIDHierarchy = Some(dbGlobalIDPath),
        databaseNamePath = Some(Seq(dbName1, dbName2))
      )

      memoryLogger.messages should have size 1

      val msg = parseMsg(memoryLogger.messages.head)
      checkBasics(200, msg)

      (msg / "id").asOpt[JSValue] shouldNot be(Symbol("empty"))
      (msg / "account_id") should equal(JSString(accountID))
      (msg / "global_id_path") should equal(JSArray(gID1, gID2))
      (msg / "db_name_path") should equal(JSArray(dbName1, dbName2))
      (msg / "request" / "method") should equal(JSString("POST"))
      (msg / "request" / "path") should equal(JSString("/"))
      (msg / "request" / "driver") should equal(JSString("driver"))
      (msg / "request" / "source") should equal(JSString("source"))
      (msg / "request" / "auth") should equal(auth)
      (msg / "request" / "body") should equal(jsBody)
      (msg / "request" / "contention_info") should equal(JSString(contentionInfo))
      (msg / "request" / "extra" / "redacted") shouldBe JSString(
        "i'm the redacted query")
      (msg / "request" / "extra" / "shape") shouldBe JSString("i'm the query shape")

      (msg / "request" / "headers" / "x_max_retries") should equal(JSLong(2))
      (msg / "request" / "headers" / "x_query_timeout") should equal(JSLong(30))
      (msg / "request" / "headers" / "x_forwarded_for") should equal(
        JSString("10.0.0.1")
      )
      (msg / "request" / "headers" / "last_seen_txn") should equal(JSLong(1234))
      (msg / "request" / "headers" / "last_txn_ts") should equal(JSLong(1234))
      (msg / "request" / "headers" / "user_agent") should equal(
        JSString("user agent")
      )
      (msg / "request" / "headers" / "runtime_env_os") should equal(
        JSString("linux")
      )
      (msg / "request" / "headers" / "runtime_env") should equal(
        JSString("Netlify")
      )
      (msg / "request" / "headers" / "runtime_nodejs_env") should equal(
        JSString("7.5.3")
      )
      (msg / "request" / "headers" / "driver_env") should equal(
        JSString("driver=javascript-4.0.1;runtime=chrome-89;env=Netlify;os=linux")
      )
      (msg / "request" / "headers" / "performance_hints") should equal(
        JSString("true")
      )
      (msg / "request" / "headers" / "typecheck") should equal(
        JSString("true")
      )
      (msg / "request" / "headers" / "linearized") should equal(
        JSString("true")
      )

      (msg / "response" / "headers" / "x-fauna-stream-id") should equal(
        JSString("1234"))

      (msg / "ts") should equal(JSString("1970-01-01T00:00:00.000042Z"))

      (msg / "stats" / "query_compute_ops") should equal(JSLong(1))
      (msg / "stats" / "query_byte_read_ops") should equal(JSLong(0))
      (msg / "stats" / "query_time") should equal(JSLong(queryTime))
      (msg / "stats" / "query_byte_write_ops") should equal(JSLong(0))
      (msg / "stats" / "query_bytes_in") should equal(JSLong(0))
      (msg / "stats" / "query_bytes_out") should equal(JSLong(0))
      (msg / "stats" / "query_overhead_time") should equal(
        JSLong(queryTime - metrics.expectedQueryTime))
      (msg / "stats" / "storage_bytes_write") should equal(JSLong(0))
      (msg / "stats" / "storage_bytes_read") should equal(JSLong(0))
      (msg / "stats" / "txn_retries") should equal(JSLong(0))

      (msg / "response" / "errors").asOpt[JSValue] shouldBe empty

      (msg / "rate_limits_hit").asOpt[JSValue] shouldBe empty
    }

    "logs business metrics" in {
      val req = HttpRequest(
        HttpMethod.POST,
        new URI("/"),
        Seq.empty,
        body
      )

      val resp = APIEndpoint.Response(
        APIResponse.EmptyResponse(200, Seq.empty).toHTTPResponse,
        redacted = Some(
          RedactedQuery(
            redacted = "i'm the redacted query",
            shape = "i'm the query shape")))

      val stats = new StatsRequestBuffer
      val databaseCreates = 3
      val databaseUpdates = 7
      val databaseDeletes = 1
      val collectionCreates = 2
      val collectionUpdates = 8
      val collectionDeletes = 5
      val roleCreates = 3
      val roleUpdates = 9
      val roleDeletes = 1
      val functionCreates = 3
      val functionUpdates = 9
      val functionDeletes = 5
      val keyCreates = 1
      val keyUpdates = 3
      val keyDeletes = 2
      val docCreates = 7
      val docUpdates = 5
      val docDeletes = 10

      stats.count(QueryMetrics.DatabaseCreate, databaseCreates)
      stats.count(QueryMetrics.DatabaseUpdate, databaseUpdates)
      stats.count(QueryMetrics.DatabaseDelete, databaseDeletes)
      stats.count(QueryMetrics.CollectionCreate, collectionCreates)
      stats.count(QueryMetrics.CollectionUpdate, collectionUpdates)
      stats.count(QueryMetrics.CollectionDelete, collectionDeletes)
      stats.count(QueryMetrics.RoleCreate, roleCreates)
      stats.count(QueryMetrics.RoleUpdate, roleUpdates)
      stats.count(QueryMetrics.RoleDelete, roleDeletes)
      stats.count(QueryMetrics.FunctionCreate, functionCreates)
      stats.count(QueryMetrics.FunctionUpdate, functionUpdates)
      stats.count(QueryMetrics.FunctionDelete, functionDeletes)
      stats.count(QueryMetrics.KeyCreate, keyCreates)
      stats.count(QueryMetrics.KeyUpdate, keyUpdates)
      stats.count(QueryMetrics.KeyDelete, keyDeletes)
      stats.count(
        QueryMetrics.WriteCreate,
        docCreates + databaseCreates + collectionCreates + roleCreates + functionCreates + keyCreates)
      stats.count(
        QueryMetrics.WriteUpdate,
        docUpdates + databaseUpdates + collectionUpdates + roleUpdates + functionUpdates + keyUpdates)
      stats.count(
        QueryMetrics.WriteDelete,
        docDeletes + databaseDeletes + collectionDeletes + roleDeletes + functionDeletes + keyDeletes)

      val respInfo = APIEndpoint.ResponseInfo(
        Timestamp.MaxMicros,
        QueryMetrics(
          queryTime = 1,
          stats = stats
        ),
        QueryLogTags.empty,
        renderPretty = false
      )
      DummyApp.logRequest(
        Some(req),
        resp,
        channelInfo,
        statsBuf = Some(stats),
        responseInfo = Some(respInfo)
      )

      memoryLogger.messages should have size 1

      val msg = parseMsg(memoryLogger.messages.head)

      (msg / "business_metrics" / "database_creates")
        .as[Long] shouldEqual databaseCreates
      (msg / "business_metrics" / "database_updates")
        .as[Long] shouldEqual databaseUpdates
      (msg / "business_metrics" / "database_deletes")
        .as[Long] shouldEqual databaseDeletes
      (msg / "business_metrics" / "collection_creates")
        .as[Long] shouldEqual collectionCreates
      (msg / "business_metrics" / "collection_updates")
        .as[Long] shouldEqual collectionUpdates
      (msg / "business_metrics" / "collection_deletes")
        .as[Long] shouldEqual collectionDeletes
      (msg / "business_metrics" / "role_creates")
        .as[Long] shouldEqual roleCreates
      (msg / "business_metrics" / "role_updates")
        .as[Long] shouldEqual roleUpdates
      (msg / "business_metrics" / "role_deletes")
        .as[Long] shouldEqual roleDeletes
      (msg / "business_metrics" / "function_creates")
        .as[Long] shouldEqual functionCreates
      (msg / "business_metrics" / "function_updates")
        .as[Long] shouldEqual functionUpdates
      (msg / "business_metrics" / "function_deletes")
        .as[Long] shouldEqual functionDeletes
      (msg / "business_metrics" / "key_creates")
        .as[Long] shouldEqual keyCreates
      (msg / "business_metrics" / "key_updates")
        .as[Long] shouldEqual keyUpdates
      (msg / "business_metrics" / "key_deletes")
        .as[Long] shouldEqual keyDeletes
      (msg / "business_metrics" / "document_creates")
        .as[Long] shouldEqual docCreates
      (msg / "business_metrics" / "document_updates")
        .as[Long] shouldEqual docUpdates
      (msg / "business_metrics" / "document_deletes")
        .as[Long] shouldEqual docDeletes
    }

    "responses" - {

      "empty" in {
        DummyApp.logRequest(
          None,
          APIEndpoint.Response(APIResponse.EmptyResponse(200).toHTTPResponse),
          channelInfo
        )

        memoryLogger.messages should have size 1

        val msg = parseMsg(memoryLogger.messages.head)
        checkBasics(200, msg)

        // Omit when empty.
        (msg / "response" / "headers").asOpt[JSValue] shouldBe empty
      }

      "JSON" in {
        DummyApp.logRequest(
          None,
          APIEndpoint.Response(
            APIResponse.JSONResponse(200, JSObject.empty).toHTTPResponse),
          channelInfo
        )

        memoryLogger.messages should have size 1

        val msg = parseMsg(memoryLogger.messages.head)
        checkBasics(200, msg)
      }

      "errors" - {
        "not found" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.NotFound.toHTTPResponse),
            channelInfo)

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(404, msg)
        }

        "redirect" in {
          val url = "http://example.com"
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.Redirect(url).toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(302, msg)
        }

        "bad request" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.BadRequest.toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(400, msg)
        }

        "unauthorized" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.Unauthorized.toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(401, msg)
        }

        "internal error" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.InternalError("oops").toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(500, msg)
        }

        "internal error with an account ID" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.InternalError("oops").toHTTPResponse),
            channelInfo,
            accountID = Some("12345")
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(500, msg)
          (msg / "account_id") shouldBe JSString("12345")
        }

        "operator error" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.OperatorError("oops").toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(540, msg)
        }

        "request timeout" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.RequestTimeout("oops").toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(408, msg)
        }

        "contention" in {
          val qRes = APIResponse
            .TransactionContentionResponse(
              APIVersion.Default,
              ScopeID(0),
              DocID(SubID(0), CollectionID(0))
            )
            .toRenderedHTTPResponse(Timestamp.Min)
          qRes foreach { res =>
            DummyApp.logRequest(
              None,
              APIEndpoint.Response(res),
              channelInfo
            )

            memoryLogger.messages should have size 1

            val msg = parseMsg(memoryLogger.messages.head)
            checkBasics(409, msg)
          }
        }

        "service timeout" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.ServiceTimeout("oops").toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(503, msg)
        }

        "too many requests" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.TooManyRequests.toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(429, msg)
        }

        "request too large" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.RequestTooLarge.toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(413, msg)
        }

        "precondition failed" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.PreconditionFailed.toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(412, msg)
        }

        "hello timeout" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.HelloTimeout("oops").toHTTPResponse),
            channelInfo
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(500, msg)
        }

        "errors included" in {
          DummyApp.logRequest(
            None,
            APIEndpoint.Response(APIResponse.NotFound.toHTTPResponse),
            channelInfo,
            errors = Some(
              JSArray(
                JSObject(
                  "code" -> JSString("validation failed"),
                  "description" -> JSString("document data is not valid"))))
          )

          memoryLogger.messages should have size 1

          val msg = parseMsg(memoryLogger.messages.head)
          checkBasics(404, msg)

          val errors = msg / "response" / "errors"
          errors.as[Seq[JSValue]] should have size 1
          (errors / 0 / "code") should equal(JSString("validation failed"))
          (errors / 0 / "description") should equal(
            JSString("document data is not valid"))
        }
      }
    }

    "generate traceID & log tags" in {
      val auth = JSObject("password" -> "sekritsquirrel")
      val headers = Seq.empty

      val req = HttpRequest(
        HttpMethod.POST,
        new URI("/"),
        headers,
        body
      )
      val resp = APIResponse.EmptyResponse(200, headers).toHTTPResponse
      val queryTime = 1234
      val metrics = QueryMetrics(queryTime, new StatsRequestBuffer)
      val responseInfo =
        APIEndpoint.ResponseInfo.Null
          .copy(txnTime = Timestamp.ofMicros(42), metrics = metrics)

      DummyApp.logRequest(
        Some(req),
        APIEndpoint.Response(resp),
        channelInfo,
        Some(
          QueryLogContext(
            tags = QueryLogTags(SeqMap("hello" -> "world", "foo" -> "bar")),
            trace = TraceContext.random()
          )),
        Some(auth),
        Some(jsBody),
        None,
        None,
        responseInfo = Some(responseInfo)
      )

      memoryLogger.messages should have size 1

      val msg = parseMsg(memoryLogger.messages.head)
      checkBasics(200, msg)

      (msg / "trace").asOpt[JSValue] shouldNot be(Symbol("empty"))
      (msg / "trace" / "trace_id").asOpt[JSValue] shouldNot be(Symbol("empty"))
      (msg / "trace" / "span_id").asOpt[JSValue] shouldNot be(Symbol("empty"))
      (msg / "trace" / "flags").asOpt[JSValue] shouldNot be(Symbol("empty"))
      (msg / "tags").as[JSObject] should be(
        JSObject("hello" -> "world", "foo" -> "bar"))
    }

    "rate limits hit" in {
      val auth = JSObject("password" -> "sekritsquirrel")
      val headers = Seq.empty

      val req = HttpRequest(
        HttpMethod.POST,
        new URI("/"),
        headers,
        body
      )
      val resp = APIResponse.EmptyResponse(200, headers).toHTTPResponse
      val queryTime = 1234
      val statBuf = new StatsRequestBuffer
      statBuf.incr(QueryMetrics.RateLimitCompute)
      statBuf.incr(QueryMetrics.RateLimitRead)
      statBuf.incr(QueryMetrics.RateLimitWrite)
      val metrics = QueryMetrics(queryTime, statBuf)
      val responseInfo =
        APIEndpoint.ResponseInfo.Null
          .copy(txnTime = Timestamp.ofMicros(42), metrics = metrics)

      DummyApp.logRequest(
        Some(req),
        APIEndpoint.Response(resp),
        channelInfo,
        Some(
          QueryLogContext(
            tags = QueryLogTags(SeqMap("hello" -> "world", "foo" -> "bar")),
            trace = TraceContext.random()
          )),
        Some(auth),
        Some(jsBody),
        None,
        None,
        responseInfo = Some(responseInfo)
      )

      memoryLogger.messages should have size 1

      val msg = parseMsg(memoryLogger.messages.head)
      checkBasics(200, msg)

      (msg / "rate_limits_hit" / 0) should equal(JSString("compute"))
      (msg / "rate_limits_hit" / 1) should equal(JSString("read"))
      (msg / "rate_limits_hit" / 2) should equal(JSString("write"))
    }

    "fqlv10 is logging properly" in {
      val auth = JSObject("password" -> "sekritsquirrel")
      val headers = Seq.empty

      val req = HttpRequest(
        HttpMethod.POST,
        new URI(FQL2Endpoint.FQLv10Path),
        headers,
        body
      )
      val resp = APIResponse.EmptyResponse(200, headers).toHTTPResponse
      val queryTime = 1234
      val metrics = QueryMetrics(queryTime, new StatsRequestBuffer)
      val responseInfo =
        APIEndpoint.ResponseInfo.Null
          .copy(txnTime = Timestamp.ofMicros(42), metrics = metrics)

      DummyApp.logRequest(
        Some(req),
        APIEndpoint.Response(resp),
        channelInfo,
        None,
        Some(auth),
        Some(jsBody),
        None,
        None,
        responseInfo = Some(responseInfo)
      )

      memoryLogger.messages should have size 1

      val msg = parseMsg(memoryLogger.messages.head)
      checkBasics(200, msg)

      (msg / "request" / "api_version") should equal(
        JSString("10")
      )
    }

  }
}
