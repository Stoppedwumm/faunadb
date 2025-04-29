package fauna.api

import fauna.api.export.ExportDataEndpoint
import fauna.api.fql2._
import fauna.api.schema.SchemaEndpoint
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.config.CoreConfig
import fauna.exec._
import fauna.flags.{ MaxQueryTimeSeconds, QueryLimit, RunQueries }
import fauna.flags.AccountFlags
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.stream.StreamContext
import fauna.model.Database
import fauna.net.http._
import fauna.net.security.JWKProvider
import fauna.repo._
import fauna.repo.query.Query
import fauna.repo.service.rateLimits._
import fauna.stats._
import fauna.trace._
import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpMethod
import io.netty.util.AsciiString
import java.util.concurrent.{ ConcurrentHashMap, ThreadLocalRandom }
import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ Future, TimeoutException }
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.Success

object CoreApplication {

  /** Generating dynamic metric names based on response code on every request can slow things down
    * so we cache them here.
    */
  private[CoreApplication] val cachedResponseMetricNames =
    new ConcurrentHashMap[Int, String]

  val CORSAllowedHeaders = Seq(
    HTTPHeaders.AcceptEncoding,
    HTTPHeaders.Authorization,
    HTTPHeaders.ContentType,
    HTTPHeaders.DriverEnv,
    HTTPHeaders.FaunaDBAPIVersion,
    HTTPHeaders.FaunaDriver,
    HTTPHeaders.FaunaSource,
    HTTPHeaders.FaunaTags,
    HTTPHeaders.Format,
    HTTPHeaders.LastSeenTxn,
    HTTPHeaders.LastTxnTs,
    HTTPHeaders.Linearized,
    HTTPHeaders.MaxContentionRetries,
    HTTPHeaders.MaxRetriesOnContention,
    HTTPHeaders.QueryTags,
    HTTPHeaders.QueryTimeout,
    HTTPHeaders.QueryTimeoutMs,
    HTTPHeaders.RuntimeEnv,
    HTTPHeaders.RuntimeEnvOS,
    HTTPHeaders.RuntimeNodeEnvVersion,
    HTTPHeaders.TraceParent,
    HTTPHeaders.Typecheck,
    HTTPHeaders.PerformanceHints
  ) mkString ", "

  val CORSAllowedMethods = "GET, POST, PUT, PATCH, DELETE"

  // NB. not cached because it is never .toString'd.
  val CORSMaxAge = new AsciiString(1.day.toSeconds.toString)

  // FIXME: Below values are admittedly arbitrary; could be subject of a core config

  // Max time for Auth lookup. This is very generous
  val AuthLookupTimeout = 5.seconds
  // Maximum time to render the output
  val MaximumRenderingTimeout = 30.seconds
  // When not specified, this is the default query timeout
  val DefaultQueryTimeout = 2.minutes
  // Clients must not request longer query timeout than this
  val MaximumQueryTimeout = MaxQueryTimeSeconds.default.value.seconds
  val MaximumQueryTimeoutMillis = MaximumQueryTimeout.toMillis
  // Clients should wait at least this long to avoid 400s
  val AggressiveTimeout = 1.seconds
}

trait CoreAppContext {
  val config: CoreConfig
  val streamCtx: StreamContext
  val stats: StatsRecorder
}

abstract class CoreApplication extends Application with CoreAppContext {
  protected val repo: RepoContext
  protected val rootKeys: List[String]
  protected val jwkProvider: JWKProvider

  import CoreApplication._

  val networkHostID = config.networkHostID

  // logs

  private val traceLog = getLogger("trace")

  private val pendingReqs = new LongAdder
  StatsRecorder.polling(10.seconds) {
    this.stats.set("Queries.Pending", pendingReqs.sum)
  }

  // Since we don't have access to the auth object at the time where
  // we must attach stats or tracing info, we can infer the auth
  // decision based on the response code. We do not include execution
  // debug output with the response for codes 401, and conservatively,
  // 500.
  private def allowStatsOutput(res: HttpResponse) =
    res.code match {
      case 401 | 500 => false
      case _         => true
    }

  // HttpRequestHandler hooks

  def handleRequest(
    info: HttpRequestChannelInfo,
    httpReq: HttpRequest): Future[HttpResponse] = {

    implicit val ec = ImmediateExecutionContext

    val endpoint: APIEndpoint =
      httpReq.path match {
        case Some(FQL2Endpoint.FQLv10Path)              => FQL2Endpoint
        case Some("/environment/1")                     => EnvironmentEndpoint
        case Some(StreamEndpoint.Path)                  => StreamEndpoint
        case Some(FeedEndpoint.Path)                    => FeedEndpoint
        case Some(path) if path.startsWith("/schema/1") => SchemaEndpoint
        case Some(path) if path.startsWith("/export/1") => ExportDataEndpoint
        case _                                          => fql1.TMPEndpoint
      }

    val isPingEndpoint = httpReq.path.contains("/ping")
    val isErrorEndpoint = httpReq.path.contains("/error")
    val endpointMetricsTags = if (isPingEndpoint || isErrorEndpoint) {
      StatTags.Empty
    } else {
      endpoint.metricsTags
    }

    this.stats.count(APIEndpoint.Metrics.RequestsReceived, 1, endpointMetricsTags)

    if (
      httpReq.method == HttpMethod.OPTIONS && httpReq.containsHeader(
        HTTPHeaders.Origin)
    ) {
      // CORS preflight -- return a success response w/CORS headers.
      val preflightResponse = HttpResponse(200, NoBody, Nil)
      addCORSHeaders(httpReq, preflightResponse)
      return Future.successful(preflightResponse)
    }

    pendingReqs.increment
    val queryStart = Timing.start

    val statsBuffer = new StatsRequestBuffer
    val stats = StatsRecorder.Multi(
      Seq(
        this.stats,
        this.stats
          .scoped("Requests")
          .filtered(QueryMetrics.BytesReadWrite),
        statsBuffer))

    val reqRepo = repo.withStats(stats)

    // These volatile vars are used to lob some values out of their
    // original scope (which may be inside a Future being fulfilled
    // by another thread, hence the volatility). Mostly, they transmit
    // information to the query log.
    // TODO: Refactor query processing and banish this hack.
    @volatile var loggedReqBody: Option[JSValue] = None
    @volatile var loggedAuth: Option[JSValue] = None

    /** This will include the list of global ids for the database path the query was run for.
      * This list is written to the query log so that we can determine if we should be
      * shipping a given log line for a user that has our logs integration enabled.
      */
    @volatile var databaseGlobalIDPath: Seq[GlobalID] = Seq.empty
    @volatile var databaseNamePath: Seq[String] = Seq.empty
    @volatile var accountID: AccountID = AccountID.Root
    @volatile var contentionInfo = Option.empty[String]
    @volatile var errors = Option.empty[JSValue]
    @volatile var flags: AccountFlags = AccountFlags.forRoot

    def getMetrics() = QueryMetrics(
      queryTime = queryStart.elapsedMillis,
      stats = statsBuffer,
      minComputeOps = config.query_min_compute_ops,
      computeOverhead = config.computeOverhead,
      readOverhead = config.readOverhead,
      writeOverhead = config.writeOverhead,
      txnOverhead = config.txnOverhead
    )

    // TODO: this cannot be a private method, because we close over the above
    // vars to have access to them.
    def getAuth(
      authRepo: RepoContext,
      req: HttpRequest,
      reqInfo: APIEndpoint.RequestInfo): Future[Option[Auth]] =
      GlobalTracer.instance.withSpan("phase.authenticate") {
        val authQ = Auth.fromInfo(req.authorization, rootKeys, jwkProvider).flatMap {
          case None           => Query.none
          case a @ Some(auth) =>
            // Note that this account might be disabled, but we store it to log the
            // request.
            accountID = auth.accountID
            Database.isDisabled(auth.database.scopeID).flatMap {
              case true => Query.none
              case false =>
                auth.database.account.map { account =>
                  flags = account.flags

                  if (account.flags.get(RunQueries)) {
                    val rnd = ThreadLocalRandom.current()
                    if (rnd.nextDouble(1.0) < account.flags.get(QueryLimit)) {
                      a
                    } else {
                      throw LimitExceededException
                    }
                  } else {
                    // Do not proceed with the request!
                    throw AccountDisabledException
                  }
                }
            }
        } flatMapT { a =>
          authJSON(a) map { json =>
            loggedAuth = Some(json)
            accountID = a.accountID
            databaseGlobalIDPath = a.database.globalIDPath
            databaseNamePath = a.database.namePath
            Some(a)
          }
        }

        authRepo.run(
          authQ,
          reqInfo.minSnapTime,
          CoreApplication.AuthLookupTimeout.bound,
          AccountID.Root,
          ScopeID.RootID,
          PermissiveOpsLimiter) map {
          _._2
        }
      }

    // TODO: this cannot be a private method, because we close over the above
    // vars to have access to them.
    def recover(
      endpoint: APIEndpoint,
      recoverRepo: RepoContext,
      reqInfo: APIEndpoint.RequestInfo,
      resInfo: APIEndpoint.ResponseInfo,
      authF: Future[Option[Auth]],
      e: Throwable): Future[(APIEndpoint.Response, APIEndpoint.ResponseInfo)] = {

      // extract the auth if it is available
      val auth = authF.value match {
        case Some(Success(Some(auth))) => Some(auth)
        case _                         => None
      }

      val e0 = e match {
        case ce: ContentionException =>
          contentionInfo = Some(ce.info)
          ce
        case te: TimeoutException =>
          val elapsed = queryStart.elapsed
          def allowedOverhead =
            QueryMetrics.computeExpectedQueryTime(statsBuffer) * 2
          if (elapsed < AggressiveTimeout) {
            AggressiveTimeoutException(AggressiveTimeout, elapsed, te)
          } else if (elapsed < allowedOverhead) {
            AggressiveTimeoutException(allowedOverhead, elapsed, te)
          } else {
            te
          }
        case e: OpsLimitException =>
          OpsLimitExceptionForRendering(e.op, statsBuffer)
        case e: Throwable => e
      }

      def recoverF(e: Throwable) =
        recoverRepo
          .run(
            endpoint.recover(this, reqInfo, resInfo, auth, e),
            reqInfo.minSnapTime,
            // error rendering really should be fast, so use our own "Aggressive"
            // timeout threshold here.
            AggressiveTimeout.bound,
            getAccountID(auth),
            getScopeID(auth),
            getOpsLimiter(auth)
          )
          .map { case (_, r) => (r, resInfo) }

      recoverF(e0) recoverWith { case _: TimeoutException =>
        // try once to nicely handle a timeout exception in render.
        recoverF(new TimeoutException("Timeout rendering error response."))
      }
    }

    def execRequest(
      auth: Option[Auth],
      apiReq: endpoint.Request,
      reqInfo: APIEndpoint.RequestInfo,
      userTags: QueryLogTags,
      body: Option[ByteBuf]) = {

      val execRepo = {
        var r = reqRepo
        r = r.copy(
          queryMaxWidth = config.queryMaxWidth,
          queryWidthLogRatio = config.queryWidthLogRatio,
          queryMaxGlobalWidth = config.queryMaxGlobalWidth,
          queryGlobalWidthLogRatio = config.queryGlobalWidthLogRatio,
          queryMaxConcurrentReads = config.queryMaxConcurrentReads
        )

        auth.foreach { a =>
          r = r.copy(priorityGroup = a.priorityGroup)
        }
        reqInfo.maxContentionRetries.foreach { rt =>
          r = r.withRetryOnContention(rt + 1)
        }

        r
      }
      val requestBytes = body.fold(0L)(_.readableBytes)
      stats.count(QueryMetrics.BytesIn, requestBytes.toInt)

      loggedReqBody = body.map(sanitizedBody)

      val deadline =
        reqInfo.deadline min (auth match {
          case Some(_) => flags.get(MaxQueryTimeSeconds).seconds.bound
          case None    => CoreApplication.MaximumQueryTimeout.bound
        })

      GlobalTracer.instance.withSpan("phase.execution") {
        execRepo.run(
          endpoint.exec(this, reqInfo, apiReq, auth),
          reqInfo.minSnapTime,
          deadline,
          getAccountID(auth),
          getScopeID(auth),
          getOpsLimiter(auth))
      } flatMap { case (txnTime, (errorsJSON, renderQ)) =>
        GlobalTracer.instance.withSpan("phase.rendering") {

          errors = errorsJSON

          val resInfo =
            APIEndpoint.ResponseInfo(
              txnTime,
              getMetrics(),
              userTags,
              shouldRenderPretty(httpReq))

          val renderF =
            execRepo.run(
              renderQ(resInfo),
              reqInfo.minSnapTime,
              deadline,
              getAccountID(auth),
              getScopeID(auth),
              getOpsLimiter(auth))

          renderF map { case (_, r) => (r, resInfo) } recoverWith {
            case _: TimeoutException =>
              Future.failed(
                new TimeoutException("Timeout rendering output response."))
          }
        }
      }
    }

    val userTags = QueryLogTags.fromHTTPHeader(
      Option.when(httpReq.containsHeader(endpoint.tagsHeader))(
        httpReq.header(endpoint.tagsHeader)),
      config.tags_max_bytes,
      config.tags_max_pairs,
      config.tags_key_max_bytes,
      config.tags_value_max_bytes
    )

    def errResInfo() =
      APIEndpoint.ResponseInfo.Null
        .copy(txnTime = repo.clock.time, metrics = getMetrics())

    val tracedF = withTracing(httpReq) {

      val bodyF =
        GlobalTracer.instance.withSpan("phase.recv_request") {
          getReqBody(httpReq)
        }

      val reqInfo = endpoint.getRequestInfo(this, httpReq)

      val reqF = {
        val ri = reqInfo.getOrElse(APIEndpoint.RequestInfo.Null)
        endpoint.getRequest(this, httpReq, ri, bodyF)
      }

      (reqInfo, userTags) match {
        case (Left(errRes), _) =>
          Future.successful((errRes, errResInfo()))
        case (Right(reqInfo), Left(tagsErr)) =>
          recover(
            endpoint,
            reqRepo,
            reqInfo,
            errResInfo(),
            Future.successful(None),
            RequestParams.InvalidHeader(endpoint.tagsHeader, tagsErr))

        case (Right(reqInfo), Right(userTags)) =>
          val authF = getAuth(reqRepo, httpReq, reqInfo)

          val resF = (authF, reqF, bodyF) par { (auth, apiReq, body) =>
            apiReq match {
              case Right(apiReq) =>
                execRequest(auth, apiReq, reqInfo, userTags, body)
              case Left(res) =>
                Future.successful((res, errResInfo()))
            }
          }

          // handle exceptions which originate somewhere in request processing.
          resF recoverWith { case NonFatal(e) =>
            // synthetic API request with as much info as we have
            recover(endpoint, reqRepo, reqInfo, errResInfo(), authF, e)
          }
      }
    }

    // Expected throwables should be handled above within request processing. This
    // only ensures basic reporting of unexpected exceptions.
    tracedF recover { case NonFatal(e) =>
      getLogger.error(s"Unhandled exception in request processing. $e")
      logException(e)
      // We should never, ever hit this branch, so we don't bother to make this
      // endpoint protocol specific.
      (
        APIEndpoint.Response(
          HttpResponse(500, JSObject("error" -> "internal server error"))),
        errResInfo())
    } map { case (res, resInfo) =>
      pendingReqs.decrement

      resInfo.metrics.toStats(this.stats)

      // XXX: This could be generalized to account for
      // additional tag metadata; high cardinality tags are $$$$
      // though. Be careful.
      val tags = MMap.empty[String, String]
      val accountIDStr = accountID.toLong.toString
      if (accountID != Database.DefaultAccount) {
        // NB. Add account id here so it falls back into root span of the request
        GlobalTracer.instance.activeSpan foreach {
          _.addAttribute(Attributes.Customer.AccountID, accountIDStr)
        }
        tags += "account_id" -> accountIDStr
      }

      endpoint match {
        case FQL2Endpoint     => tags += "version" -> "FQLX"
        case fql1.TMPEndpoint => tags += "version" -> "FQL4"
        case _                => ()
      }
      val statTags = StatTags(tags.toSet)

      stats.distribution(QueryMetrics.QueryTime, resInfo.metrics.queryTime, statTags)
      val renderTime = queryStart.elapsedMillis - resInfo.metrics.queryTime
      stats.distribution(QueryMetrics.RenderTime, renderTime)

      val responseBytes = res.http.contentLength.getOrElse(0)
      stats.count(QueryMetrics.BytesOut, responseBytes)

      // Make sure to add CORS headers after other headers, in order to ensure
      // custom headers are exposed to browsers.
      addCORSHeaders(httpReq, res.http)

      if (config.logQueries) {
        val auth = loggedAuth orElse {
          AuthRender.render(httpReq.authorization)
        }

        logRequest(
          Some(httpReq),
          res,
          info,
          Some(
            QueryLogContext(
              httpReq.traceContext,
              userTags.getOrElse { QueryLogTags.empty })),
          auth,
          Some(loggedReqBody),
          errors,
          contentionInfo,
          Some(statsBuffer),
          Some(resInfo),
          accountID = Some(accountIDStr),
          databaseGlobalIDPath =
            Some(databaseGlobalIDPath.map(Database.encodeGlobalID(_))),
          databaseNamePath = Some(databaseNamePath)
        )
      }

      val httpRes = res.http
      this.stats.count(APIEndpoint.Metrics.RequestsProcessed, 1, endpointMetricsTags)
      this.stats.distribution(
        APIEndpoint.Metrics.ResponseTime,
        queryStart.elapsedMillis,
        endpointMetricsTags)

      // We use `-res.code` to just have a different key when it's a ping
      // request, which ensures we don't get conflicts with other response
      // codes.
      //
      // If we want any more detailed metrics, move this to CoreApplication
      this.stats.count(
        CoreApplication.cachedResponseMetricNames
          .computeIfAbsent(
            if (isPingEndpoint) {
              -httpRes.code
            } else {
              httpRes.code
            },
            { _ =>
              if (isPingEndpoint) {
                APIEndpoint.Metrics.pingResponseCode(httpRes.code)
              } else {
                APIEndpoint.Metrics.responseCode(httpRes.code)
              }
            }
          ),
        1,
        endpointMetricsTags
      )

      httpRes
    }
  }

  // helpers

  // only prettify the response if the user cares
  private def shouldRenderPretty(req: HttpRequest): Boolean = {
    val agent = req.getHeader(HTTPHeaders.UserAgent) getOrElse ""

    (agent contains "curl") ||
    (agent contains "HTTPie") ||
    (req containsHeader HTTPHeaders.FaunaDBFormattedJSON)
  }

  private def getAccountID(auth: Option[Auth]) =
    auth.fold(AccountID.Root)(_.accountID)

  private def getScopeID(auth: Option[Auth]) =
    auth.fold(ScopeID.RootID)(_.scopeID)

  private def getOpsLimiter(auth: Option[Auth]) = {
    auth.fold(PermissiveOpsLimiter: OpsLimiter)(_.limiter)
  }

  private def getReqBody(req: HttpRequest): Future[Option[ByteBuf]] =
    if (req.hasBody) {
      implicit val ec = ImmediateExecutionContext
      req.body.data map { Some(_) }
    } else {
      Future.successful(None)
    }

  private def withTracing(req: HttpRequest)(
    fn: => Future[(APIEndpoint.Response, APIEndpoint.ResponseInfo)])
    : Future[(APIEndpoint.Response, APIEndpoint.ResponseInfo)] = {
    if (req.tracingEnabled) {
      implicit val ec = ImmediateExecutionContext
      traceFuture(Future {
        traceMsg(s"REQUEST RECEIVED: ${req.toShortString}")
      } flatMap { _ =>
        fn
      }) { (log, t) =>
        if (config.logTraces) {
          traceLog.info("\n" + log.toString.trim)
        }

        Future.fromTry(t map { case (res, resInfo) =>
          traceMsg(s"RESPONSE SENT: ${res.http.toShortString}")

          val res0 = res.http.body match {
            case b @ Body(buf, _, _) if allowStatsOutput(res.http) =>
              val js = JSON
                .parse[JSValue](buf)
                .as[JSObject] :+ ("trace" -> log.toString.split("\n").toList)
              val body = b.copy(content = js.toByteBuf(true))
              res.copy(http = res.http.withBody(body))

            case _ => res
          }

          (res0, resInfo)
        })
      }
    } else {
      GuardFuture { fn }
    }
  }

  private def addCORSHeaders(req: HttpRequest, res: HttpResponse): Unit =
    if (req containsHeader HTTPHeaders.Origin) {
      res.setHeader(HTTPHeaders.AccessControlExposeHeaders, res.exposeHeaders)
      res.setHeader(
        HTTPHeaders.AccessControlAllowOrigin,
        req.header(HTTPHeaders.Origin))
      res.setHeader(HTTPHeaders.AccessControlAllowCredentials, "true")

      if (req.method == HttpMethod.OPTIONS) {
        res.setHeader(HTTPHeaders.AccessControlMaxAge, CORSMaxAge)
        res.setHeader(HTTPHeaders.AccessControlAllowHeaders, CORSAllowedHeaders)
        res.setHeader(HTTPHeaders.AccessControlAllowMethods, CORSAllowedMethods)
      }
    }

  // Logging

  private def authJSON(auth: Auth) =
    auth match {
      case null  => Query.value(JSNull)
      case other => AuthRender.render(other)
    }

  private val blankPW = JSString("***")
  private val credKeywords = List("password", "secret", "key_from_secret")

  def sanitizeCredentials(js: JSValue): JSValue =
    js match {
      case obj: JSObject =>
        JSObject(obj.value map {
          case (k, _) if credKeywords exists { k contains _ } => (k, blankPW)
          case (k, v) => (k, sanitizeCredentials(v))
        }: _*)
      case arr: JSArray => JSArray(arr.value map { sanitizeCredentials(_) }: _*)
      case v            => v
    }

  private def sanitizedBody(body: ByteBuf) =
    APIEndpoint.tryParseJSON[JSValue](body).toOption match {
      case None => JSString(body.toUTF8String)
      case Some(js) =>
        sanitizeCredentials(js) match {
          case o: JSObject => o - "data"
          case js          => js
        }
    }

  override protected def logException(e: Throwable): Unit = {
    traceError(e)
    super.logException(e)
  }
}
