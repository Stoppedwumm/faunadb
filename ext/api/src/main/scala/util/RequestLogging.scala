package fauna.api.util

import fauna.api.fql2.FQL2Endpoint
import fauna.api.APIEndpoint
import fauna.codex.json.JSValue
import fauna.codex.json2._
import fauna.logging._
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.util.CommonLogging
import fauna.net.http._
import fauna.snowflake.IDSource
import fauna.stats._
import fauna.trace._
import io.netty.util.AsciiString
import org.apache.logging.log4j.LogManager

object RequestLogging extends ExceptionLogging {
  lazy val log = LogManager.getLogger("query")
  lazy val slowLog = LogManager.getLogger("slow")

  /** Top-level key generated from an IDSource for each request. This
    * field is used as the idempotency key when aggregating logs,
    * where at-least-once delivery may result in duplicate log
    * entries.
    */
  final val MessageIDField = JSON.Escaped("id")

  /** Account id of the user making the query. */
  final val AccountIDField = JSON.Escaped("account_id")

  /** List of names for the database path the query was executed for.
    * This is needed for the customer logs we ship so we can include the database path.
    */
  final val DatabaseNamePathField = JSON.Escaped("db_name_path")

  /** Top-level key for metadata associated with the response to a
    * request.
    */
  final val ResponseField = JSON.Escaped("response")

  /** HTTP response code. Found in "response.code" in each log line.
    */
  final val CodeField = JSON.Escaped("code")

  /** Query errors. Found in "response.errors" for 4xx or 5xx responses.
    */
  final val ErrorsField = JSON.Escaped("errors")

  /** Top-level key containing the transaction time of the response in
    * ISO-8601 format.
    */
  final val TSField = JSON.Escaped("ts")

  /** Top-level key for metadata associated with the processed
    * request.
    */
  final val RequestField = JSON.Escaped("request")

  /** Correlation identifier for the request. Found in "request.id" in
    * each log line.
    */
  final val IDField = JSON.Escaped("id")

  /** Client IP which originated or forwarded the request. Found in
    * "request.ip" in each log line.
    */
  final val IPField = JSON.Escaped("ip")

  /** IP of the FaunaDB host which received the request. Found in
    * "request.host" in each log line.
    */
  final val HostField = JSON.Escaped("host")

  /** The replica name of the FaunaDB instance. Found in
    * "request.replica" in each log line.
    */
  final val ReplicaField = JSON.Escaped("replica")

  /** HTTP method of the request. Found in "request.method" in each
    * log line.
    */
  final val MethodField = JSON.Escaped("method")

  /** The path component of the request URI. Found in "request.path"
    * in each log line.
    */
  final val PathField = JSON.Escaped("path")

  /** The query component of the request URI, if any was
    * provided. Found in "request.query" in each log line.
    */
  final val QueryField = JSON.Escaped("query")

  /** FaunaDB driver identification string, if an official driver was
    * used. Found in "request.driver" in each log line.
    */
  final val DriverField = JSON.Escaped("driver")

  /** FaunaDB tool identification string, if a tool such as FDM or
    * Canary originated the request. Found in "request.source" in each
    * log line.
    */
  final val SourceField = JSON.Escaped("source")

  /** Runtime environment such as "Netlify", "AWS Lambda", etc.
    * Found in "request.headers.x_runtime_environment", if
    * provided.
    */
  final val RuntimeEnvField = JSON.Escaped("runtime_env")

  /** OS environment
    * Found in "request.headers.x_runtime_environment_os", if
    * provided.
    */
  final val RuntimeEnvOSField = JSON.Escaped("runtime_env_os")

  /** NodeJS version.
    * Found in "request.headers.x_nodejs_version", if
    * provided.
    */
  final val RuntimeNodeJSVersionField = JSON.Escaped("runtime_nodejs_env")

  /** Driver environment.
    * Contains the driver's environment information in cookie format.
    * Found in  request.headers.x_driver_env".
    */
  final val DriverEnvField = JSON.Escaped("driver_env")

  /** FaunaDB API version identification string. Found in "request.version"
    * in each log line.
    */
  final val ApiVersionField = JSON.Escaped("api_version")

  /** Key associated with request/response headers which may
    * affect query processing, such as client-specified timeout
    * or retry behavior, if any were provided. Found in "request.headers"
    * and "response.headers" in each log line, when applicable.
    */
  final val HeadersField = JSON.Escaped("headers")

  /** Client-provided limit to transaction retries under
    * contention. Found in "request.headers.x_max_retries", if
    * provided.
    */
  final val MaxRetriesField = JSON.Escaped("x_max_retries")

  /** Client-provided limit to query time in milliseconds. Found in
    * "request.headers.x_query_timeout", if provided.
    */
  final val QueryTimeoutField = JSON.Escaped("x_query_timeout")

  /** Client-provided originator identification string - typically an
    * IP address - if the client did not originate the request. Found
    * in "request.headers.x_forwarded_for", if provided.
    */
  final val ForwardedForField = JSON.Escaped("x_forwarded_for")

  /** Client-provided HTTP user agent identification string. Found in
    * "request.headers.user_agent", if provided.
    */
  final val UserAgentField = JSON.Escaped("user_agent")

  /** Client-provided timestamp representing the last transaction time
    * received from the database. For queries before v10.
    * Found in [[HTTPHeaders.LastSeenTxn]].
    */
  final val LastSeenTxn = JSON.Escaped("last_seen_txn")

  /** Same as above, but for v10.
    * Found in [[HTTPHeaders.LastTxnTs]].
    */
  final val LastTxnTs = JSON.Escaped("last_txn_ts")

  /** Client-provided flag to enable performance hints for the query. */
  final val PerformanceHints = JSON.Escaped("performance_hints")

  /** Client-provided flag to enable/disable typechecking. */
  final val TypeCheck = JSON.Escaped("typecheck")

  /** Client-provided flag to enable a query to be linearized. */
  final val Linearized = JSON.Escaped("linearized")

  /** Request authentication information, if any was provided. If the
    * request is unauthenticated, the log will contain `null`. Found
    * in "request.auth" in each log line.
    */
  final val AuthField = JSON.Escaped("auth")

  /** HTTP POST data provided in the request, if any was provided. If
    * no body is present, the log will contain `null`. Found in
    * "request.body" in each log line.
    */
  final val BodyField = JSON.Escaped("body")

  /** When a request encounters contention and exceeds the retry limit, this
    * field will contain information about the row under contention. Found in
    * "request.contention_info".
    */
  final val ContentionInfoField = JSON.Escaped("contention_info")

  /** The value of x-fauna-tags, used in tracing requests. Found in
    * "trace.tags"
    */
  final val TagsField = JSON.Escaped("tags")

  /** Top-level key for debugging information associated with the
    * request, if enabled.
    */
  final val DebugField = JSON.Escaped("debug")

  /** Total time spent processing this request. Found in
    * "stats.query_time" in each log line.
    */
  final val QueryTimeField = JSON.Escaped("query_time")

  /** Total number of bytes received from the client in this
    * request. Found in "stats.query_bytes_in" in each log line.
    */
  final val BytesInField = JSON.Escaped("query_bytes_in")

  /** Total number of bytes sent to the client in the response. Found
    * in "stats.query_bytes_out" in each log line.
    */
  final val BytesOutField = JSON.Escaped("query_bytes_out")

  /** Total number of retries due to contention encountered while
    * processing this request. Found in "stats.txn_retries" in each
    * log line.
    */
  final val RetriesField = JSON.Escaped("txn_retries")

  /** Fields used to render the business_metrics block.
    * These metrics are used by product to understand customer behavior and which
    * features are being used.
    */
  final val BusinessMetricsField = JSON.Escaped("business_metrics")
  final val DatabaseCreatesField = JSON.Escaped("database_creates")
  final val DatabaseUpdatesField = JSON.Escaped("database_updates")
  final val DatabaseDeletesField = JSON.Escaped("database_deletes")
  final val CollectionCreatesField = JSON.Escaped("collection_creates")
  final val CollectionUpdatesField = JSON.Escaped("collection_updates")
  final val CollectionDeletesField = JSON.Escaped("collection_deletes")
  final val RoleCreatesField = JSON.Escaped("role_creates")
  final val RoleUpdatesField = JSON.Escaped("role_updates")
  final val RoleDeletesField = JSON.Escaped("role_deletes")
  final val FunctionCreatesField = JSON.Escaped("function_creates")
  final val FunctionUpdatesField = JSON.Escaped("function_updates")
  final val FunctionDeletesField = JSON.Escaped("function_deletes")
  final val KeyCreatesField = JSON.Escaped("key_creates")
  final val KeyUpdatesField = JSON.Escaped("key_updates")
  final val KeyDeletesField = JSON.Escaped("key_deletes")
  final val DocumentCreatesField = JSON.Escaped("document_creates")
  final val DocumentUpdatesField = JSON.Escaped("document_updates")
  final val DocumentDeletesField = JSON.Escaped("document_deletes")

  final val ExpectedComputeTimeField = JSON.Escaped("query_expected_compute_time")
  final val ExpectedReadTimeField = JSON.Escaped("query_expected_read_time")
  final val ExpectedWriteTimeField = JSON.Escaped("query_expected_write_time")
  final val ExpectedQueryTimeField = JSON.Escaped("query_expected_query_time")

  /** "Overhead" refers to the difference between the time taken to
    * execute a query (query_time) and the time it was expected to
    * take (query_expected_query_time).
    *
    * Found in "stats.query_overhead_time" in each log line.
    */
  final val OverheadTimeField = JSON.Escaped("query_overhead_time")

  final val ComputeTimeField = JSON.Escaped("query_compute_time")
  final val ReadTimeField = JSON.Escaped("query_read_time")

  /** Round-trip time through the transaction log for successful
    * transactions. This value is not present if a transaction failed
    * to transit the log.
    *
    * Found in "stats.query_transaction_log_time" in each log line.
    */
  final val TransactionLogTimeField = JSON.Escaped("query_transaction_log_time")

  /** Stores the `redacted` and `shape` fields.
    */
  final val ExtraField = JSON.Escaped("extra")

  /** Shows a redacted version of the query. This hides the contents of any strings.
    */
  final val RedactedField = JSON.Escaped("redacted")

  /** Shows the shape of the query. This is similar to redacted, but all information
    * (numbers and strings) is removed. Identifiers are still shown.
    */
  final val ShapeField = JSON.Escaped("shape")

  /** Shows which limits were hit during the processing of the request.
    */
  final val RateLimitsField = JSON.Escaped("rate_limits_hit")
  final val RateLimitHitCompute = JSON.Escaped("compute")
  final val RateLimitHitRead = JSON.Escaped("read")
  final val RateLimitHitWrite = JSON.Escaped("write")

  /** Shows how the per-query read cache was used during query processing. */
  final val ReadCacheHits = JSON.Escaped("query_read_cache_hits")
  final val ReadCacheMisses = JSON.Escaped("query_read_cache_misses")
  final val ReadCacheBytesLoaded = JSON.Escaped("query_read_cache_bytes_loaded")

  final class Message(
    val id: Long,
    val hostID: String,
    val replicaName: Option[String],
    val req: Option[HttpRequest],
    val res: APIEndpoint.Response,
    val ctx: Option[QueryLogContext],
    val info: HttpRequestChannelInfo,
    val span: Option[Span] = None,
    val auth: Option[JSValue] = None,
    val body: Option[JSValue] = None,
    val errors: Option[JSValue] = None,
    val contentionInfo: Option[String] = None,
    val statsBuf: Option[StatsRequestBuffer] = None,
    val responseInfo: Option[APIEndpoint.ResponseInfo] = None,
    val accountID: Option[String] = None,
    val databaseGlobalIDPath: Option[Seq[String]] = None,
    val databaseNamePath: Option[Seq[String]] = None
  )

  implicit object MessageEncoder extends JSON.Encoder[Message] {

    def encode(out: JSON.Out, msg: Message): JSON.Out = {
      out.writeObjectStart()

      out.writeObjectField(
        MessageIDField,
        // Cast as a string to avoid limited/varying precision
        // integers in log pipelines.
        out.writeString(msg.id.toString))

      // Things like AdminApplication don't have a context, so we skip it if
      // not present.
      msg.ctx foreach { ctx =>
        CommonLogging.Trace.writeTrace(out, ctx.trace)

        out.writeObjectField(
          TagsField, {
            out.writeObjectStart()
            // Use the request ordering, because there isn't much reason to sort
            // this.
            ctx.tags.tags foreach { case (k, v) =>
              out.writeObjectField(new AsciiString(k), out.writeString(v))
            }
            out.writeObjectEnd()
          }
        )
      }

      msg.accountID foreach { acctID =>
        out.writeObjectField(
          AccountIDField,
          out.writeString(acctID)
        )
      }

      msg.databaseGlobalIDPath foreach { databaseGlobalIDPath =>
        out.writeObjectField(
          CommonLogging.GlobalIDPathField, {
            out.writeArrayStart()
            databaseGlobalIDPath foreach { out.writeString(_) }
            out.writeArrayEnd()
          })
      }

      msg.databaseNamePath foreach { databaseNamePath =>
        out.writeObjectField(
          DatabaseNamePathField, {
            out.writeArrayStart()
            databaseNamePath foreach { out.writeString(_) }
            out.writeArrayEnd()
          })
      }

      out.writeObjectField(
        ResponseField, {
          out.writeObjectStart()
          out.writeObjectField(CodeField, out.writeNumber(msg.res.http.code))
          writeResHeaders(out, msg.res.http)

          msg.errors.foreach { errs => // Omit if empty.
            out.writeObjectField(
              ErrorsField, {
                out.writeDelimiter()
                errs.writeTo(out.buf, false)
              })
          }
          out.writeObjectEnd()
        }
      )

      out.writeObjectField(
        IPField,
        out.writeString(msg.info.remoteAddr.getAddress.getHostAddress)
      )

      out.writeObjectField(HostField, out.writeString(msg.hostID))
      writeOptHeaderString(out, msg.replicaName, ReplicaField)

      // NB. TxnTime comes from the *response*
      msg.responseInfo.foreach { resInfo =>
        out.writeObjectField(TSField, out.writeString(resInfo.txnTime.toString))
      }

      msg.req foreach { req =>
        out.writeObjectField(
          RequestField, {
            out.writeObjectStart()

            out.writeObjectField(MethodField, out.writeString(req.method.name))

            writeOptHeaderString(out, req.path, PathField)

            writeOptHeaderString(out, req.query, QueryField)

            writeOptHeaderString(out, req, HTTPHeaders.FaunaDriver, DriverField)

            writeOptHeaderString(out, req, HTTPHeaders.FaunaSource, SourceField)

            // We don't require api_version in v10 but we still want to output it.
            if (req.path.getOrElse("/") == FQL2Endpoint.FQLv10Path) {
              out.writeObjectField(ApiVersionField, out.writeString("10"))
            } else {
              writeOptHeaderString(
                out,
                req,
                HTTPHeaders.FaunaDBAPIVersion,
                ApiVersionField)
            }

            out.writeObjectField(
              HeadersField, {
                out.writeObjectStart()
                writeReqHeaders(out, req)
                out.writeObjectEnd()
              }
            )

            out.writeObjectField(
              AuthField, {
                out.writeDelimiter()
                msg.auth.writeTo(out.buf, false)
              })

            out.writeObjectField(
              BodyField, {
                out.writeDelimiter()
                msg.body.writeTo(out.buf, false)
              })

            msg.res.redacted.foreach { redacted =>
              out.writeObjectField(
                ExtraField, {
                  out.writeObjectStart()
                  out.writeObjectField(
                    RedactedField,
                    out.writeString(redacted.redacted))
                  out.writeObjectField(ShapeField, out.writeString(redacted.shape))
                  out.writeObjectEnd()
                }
              )
            }

            // Log contention information when it's present
            writeOptHeaderString(out, msg.contentionInfo, ContentionInfoField)

            out.writeObjectEnd()
          }
        )
      }

      msg.responseInfo foreach { resInfo =>
        writeRateLimits(out, resInfo.metrics)
        writeStats(msg, out, msg.res.http, resInfo.metrics)
        writeFeatureMetrics(out, resInfo.metrics)
      }

      out.writeObjectEnd()
      out
    }
  }

  private def writeReqHeaders(out: JSONWriter, req: HttpRequest): Unit = {
    writeOptHeaderNumber(
      out,
      req,
      HTTPHeaders.MaxRetriesOnContention,
      MaxRetriesField)

    writeOptHeaderNumber(out, req, HTTPHeaders.QueryTimeout, QueryTimeoutField)

    writeOptHeaderString(out, req, HTTPHeaders.ForwardedFor, ForwardedForField)

    writeOptHeaderString(out, req, HTTPHeaders.UserAgent, UserAgentField)

    writeOptHeaderString(out, req, HTTPHeaders.RuntimeEnvOS, RuntimeEnvOSField)

    writeOptHeaderNumber(out, req, HTTPHeaders.LastSeenTxn, LastSeenTxn)

    writeOptHeaderNumber(out, req, HTTPHeaders.LastTxnTs, LastTxnTs)

    writeOptHeaderString(out, req, HTTPHeaders.PerformanceHints, PerformanceHints)

    writeOptHeaderString(out, req, HTTPHeaders.Typecheck, TypeCheck)

    writeOptHeaderString(out, req, HTTPHeaders.Linearized, Linearized)

    writeOptHeaderString(
      out,
      req,
      HTTPHeaders.RuntimeNodeEnvVersion,
      RuntimeNodeJSVersionField)

    writeOptHeaderString(out, req, HTTPHeaders.RuntimeEnv, RuntimeEnvField)

    writeOptHeaderString(out, req, HTTPHeaders.DriverEnv, DriverEnvField)
  }

  private def writeResHeaders(out: JSONWriter, res: HttpResponse): Unit =
    res.getHeader(HTTPHeaders.FaunaStreamID) foreach { streamID => // omit if empty
      out.writeObjectField(
        HeadersField, {
          out.writeObjectStart()
          out.writeObjectField(HTTPHeaders.FaunaStreamID, out.writeString(streamID))
          out.writeObjectEnd()
        })
    }

  private def writeOptHeaderString(
    out: JSONWriter,
    req: HttpRequest,
    h: AsciiString,
    f: JSON.Escaped): Unit =
    writeOptHeaderString(out, req.getHeader(h), f)

  private def writeOptHeaderString(
    out: JSONWriter,
    header: Option[String],
    f: JSON.Escaped): Unit =
    header foreach { t =>
      out.writeObjectField(f, out.writeString(t))
    }

  private def writeOptHeaderNumber(
    out: JSONWriter,
    req: HttpRequest,
    h: AsciiString,
    f: JSON.Escaped): Unit =
    req.getHeader(h) foreach { t =>
      val value =
        try {
          t.toLong
        } catch {
          // Use -1 as a sigil to indicate the header value couldn't be
          // converted.
          case _: NumberFormatException => -1L
        }

      out.writeObjectField(f, out.writeNumber(value))
    }

  private def writeRateLimits(out: JSONWriter, metrics: QueryMetrics): Unit = {
    var limited = Vector.empty[JSON.Escaped]
    if (metrics.rateLimitComputeHit) {
      limited :+= RateLimitHitCompute
    }
    if (metrics.rateLimitReadHit) {
      limited :+= RateLimitHitRead
    }
    if (metrics.rateLimitWriteHit) {
      limited :+= RateLimitHitWrite
    }

    if (limited.size > 0) {
      out.writeObjectField(
        RateLimitsField, {
          out.writeArrayStart()
          limited foreach { out.writeString(_) }
          out.writeArrayEnd()
        })
    }
  }

  private def writeFeatureMetrics(out: JSONWriter, metrics: QueryMetrics): Unit = {
    out.writeObjectField(
      BusinessMetricsField, {
        out.writeObjectStart()
        out.writeObjectField(
          DatabaseCreatesField,
          out.writeNumber(metrics.databaseCreates))
        out.writeObjectField(
          DatabaseUpdatesField,
          out.writeNumber(metrics.databaseUpdates))
        out.writeObjectField(
          DatabaseDeletesField,
          out.writeNumber(metrics.databaseDeletes))
        out.writeObjectField(
          CollectionCreatesField,
          out.writeNumber(metrics.collectionCreates))
        out.writeObjectField(
          CollectionUpdatesField,
          out.writeNumber(metrics.collectionUpdates))
        out.writeObjectField(
          CollectionDeletesField,
          out.writeNumber(metrics.collectionDeletes))
        out.writeObjectField(RoleCreatesField, out.writeNumber(metrics.roleCreates))
        out.writeObjectField(RoleUpdatesField, out.writeNumber(metrics.roleUpdates))
        out.writeObjectField(RoleDeletesField, out.writeNumber(metrics.roleDeletes))
        out.writeObjectField(
          FunctionCreatesField,
          out.writeNumber(metrics.functionCreates))
        out.writeObjectField(
          FunctionUpdatesField,
          out.writeNumber(metrics.functionUpdates))
        out.writeObjectField(
          FunctionDeletesField,
          out.writeNumber(metrics.functionDeletes))
        out.writeObjectField(KeyCreatesField, out.writeNumber(metrics.keyCreates))
        out.writeObjectField(KeyUpdatesField, out.writeNumber(metrics.keyUpdates))
        out.writeObjectField(KeyDeletesField, out.writeNumber(metrics.keyDeletes))
        out.writeObjectField(
          DocumentCreatesField,
          out.writeNumber(metrics.documentCreates))
        out.writeObjectField(
          DocumentUpdatesField,
          out.writeNumber(metrics.documentUpdates))
        out.writeObjectField(
          DocumentDeletesField,
          out.writeNumber(metrics.documentDeletes))
        out.writeObjectEnd()
      }
    )
  }

  private def writeStats(
    msg: Message,
    out: JSONWriter,
    res: HttpResponse,
    metrics: QueryMetrics): Unit =
    CommonLogging.Stats.writeStats(out, metrics, Some(res.status.code)) { out =>
      out.writeObjectField(QueryTimeField, out.writeNumber(metrics.queryTime))

      out.writeObjectField(BytesInField, out.writeNumber(metrics.queryBytesIn))
      val bytesOut = res.contentLength.getOrElse(0)
      out.writeObjectField(BytesOutField, out.writeNumber(bytesOut))

      out.writeObjectField(RetriesField, out.writeNumber(metrics.txnRetries))
      out.writeObjectField(ReadTimeField, out.writeNumber(metrics.readTime))
      out.writeObjectField(ComputeTimeField, out.writeNumber(metrics.computeTime))
      out.writeObjectField(
        ExpectedComputeTimeField,
        out.writeNumber(metrics.expectedComputeTime))
      out.writeObjectField(
        ExpectedReadTimeField,
        out.writeNumber(metrics.expectedReadTime))
      out.writeObjectField(
        ExpectedWriteTimeField,
        out.writeNumber(metrics.expectedWriteTime))
      out.writeObjectField(
        ExpectedQueryTimeField,
        out.writeNumber(metrics.expectedQueryTime))
      out.writeObjectField(
        OverheadTimeField,
        out.writeNumber(Math.max(0, metrics.queryTime - metrics.expectedQueryTime)))

      metrics.txnLogTime foreach { millis =>
        out.writeObjectField(TransactionLogTimeField, out.writeNumber(millis))
      }

      out.writeObjectField(ReadCacheHits, out.writeNumber(metrics.readCacheHits))
      out.writeObjectField(ReadCacheMisses, out.writeNumber(metrics.readCacheMisses))
      out.writeObjectField(
        ReadCacheBytesLoaded,
        out.writeNumber(metrics.readCacheBytesLoaded))

      msg.statsBuf.foreach { stats =>
        // FIXME: temp stats logging. remove when all stats below are reworked
        TempStats.foreach { case (stat, field) =>
          stats.timingOpt(stat).foreach { n =>
            out.writeObjectField(field, out.writeNumber(n))
          }
        }
      }
    }

  private val TempStats = {
    // FIXME: temporary typing CPU stats. remove when reworked into
    // QueryMetrics and/or compute ops
    Seq(
      FQLInterpreter.TypingTimeMetric -> "temp_query_typing_time",
      FQLInterpreter.TypingCPUTimeMetric -> "temp_query_typing_cpu_time",
      FQLInterpreter.EnvTypingTimeMetric -> "temp_query_env_typing_time",
      FQLInterpreter.EnvTypingCPUTimeMetric -> "temp_query_env_typing_cpu_time"
    ) ++
      // FIXME: temporary QEC stats. remove when reworked into QueryMetrics
      Seq("Eval", "Compute", "Wait", "Overhead")
        .map(st => s"Temp.Query.$st.Time" -> s"temp_query_${st.toLowerCase}_time")
  }.map { case (stat, field) => (stat, AsciiString.cached(field)) }
}

trait RequestLogging {

  import RequestLogging._

  def networkHostID: String
  def replicaName: Option[String]
  def messageIDSource: IDSource

  protected def queryLog = RequestLogging.log
  protected def slowLog = RequestLogging.slowLog

  protected def logRequest(
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
    databaseGlobalIDPath: Option[Seq[String]] = None,
    databaseNamePath: Option[Seq[String]] = None
  ): Unit = {

    val msg = JSONMessage(
      new Message(
        messageIDSource.getID,
        networkHostID,
        replicaName,
        req,
        res,
        ctx,
        info,
        GlobalTracer.instance.activeSpan,
        auth,
        body,
        errors,
        contentionInfo,
        statsBuf,
        responseInfo,
        accountID = accountID,
        databaseGlobalIDPath = databaseGlobalIDPath,
        databaseNamePath = databaseNamePath
      ))

    queryLog.info(msg)

    responseInfo.foreach { resInfo =>
      if (resInfo.metrics.queryTime >= LoggingConfig.slowQueryThreshold) {
        slowLog.info(msg)
      }
    }
  }
}
