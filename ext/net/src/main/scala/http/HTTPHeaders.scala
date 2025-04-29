package fauna.net.http

import io.netty.handler.codec.http.{ HttpHeaderNames => NettyHeaders }
import io.netty.util.AsciiString

/** Pre-allocated HTTP headers.
  */
object HTTPHeaders {
  val ByteReadOps =
    AsciiString.cached("x-byte-read-ops")

  val ByteWriteOps =
    AsciiString.cached("x-byte-write-ops")

  val ComputeOps =
    AsciiString.cached("x-compute-ops")

  // js driver 4.1.2
  val DriverEnv =
    AsciiString.cached("x-driver-env")

  // used to pass pre-shared-key to /export endpoints
  val ExportKey =
    AsciiString.cached("x-export-key")

  val FaunaDriver =
    AsciiString.cached("x-fauna-driver")

  val FaunaSource =
    AsciiString.cached("x-fauna-source")

  val FaunaShellBuiltin =
    AsciiString.cached("x-fauna-shell-builtin")

  val FaunaDBAPIVersion =
    AsciiString.cached("x-faunadb-api-version")

  val FaunaDBBuild =
    AsciiString.cached("x-faunadb-build")

  val FaunaDBFormattedJSON =
    AsciiString.cached("x-faunadb-formatted-json")

  val FaunaDBHost =
    AsciiString.cached("x-faunadb-host")

  val Format =
    AsciiString.cached("x-format")

  val ForwardedFor =
    AsciiString.cached("x-forwarded-for")

  val LastSeenTxn =
    AsciiString.cached("x-last-seen-txn")

  val LastTxnTs =
    AsciiString.cached("x-last-txn-ts")

  val Linearized =
    AsciiString.cached("x-linearized")

  val MaxContentionRetries =
    AsciiString.cached("x-max-contention-retries")

  val MaxRetriesOnContention =
    AsciiString.cached("x-max-retries-on-contention")

  val QueryBytesIn =
    AsciiString.cached("x-query-bytes-in")

  val QueryBytesOut =
    AsciiString.cached("x-query-bytes-out")

  val QueryTags =
    AsciiString.cached("x-query-tags")

  val QueryTime =
    AsciiString.cached("x-query-time")

  val QueryTimeout =
    AsciiString.cached("x-query-timeout")

  val QueryTimeoutMs =
    AsciiString.cached("x-query-timeout-ms")

  val RateLimitedOps =
    AsciiString.cached("x-rate-limited-ops")

  // js driver 4.1.1
  val RuntimeEnv =
    AsciiString.cached("x-runtime-environment")

  // js driver 4.1.1
  val RuntimeEnvOS =
    AsciiString.cached("x-runtime-environment-os")

  // js driver 4.1.1
  val RuntimeNodeEnvVersion =
    AsciiString.cached("x-nodejs-version")

  val StorageBytesRead =
    AsciiString.cached("x-storage-bytes-read")

  val StorageBytesWrite =
    AsciiString.cached("x-storage-bytes-write")

  val Trace =
    AsciiString.cached("x-trace")

  val TraceParent =
    AsciiString.cached("traceparent")

  val TraceSecret =
    AsciiString.cached("x-trace-secret")

  val Typecheck =
    AsciiString.cached("x-typecheck")

  val PerformanceHints =
    AsciiString.cached("x-performance-hints")

  val TxnRetries =
    AsciiString.cached("x-txn-retries")

  val TxnTime =
    AsciiString.cached("x-txn-time")

  val ExpectedQueryTime =
    AsciiString.cached("x-expected-query-time")

  val FaunaStreamID =
    AsciiString.cached("x-fauna-stream-id")

  val FaunaTags =
    AsciiString.cached("x-fauna-tags")

  val Accept = NettyHeaders.ACCEPT
  val AcceptCharset = NettyHeaders.ACCEPT_CHARSET
  val AcceptEncoding = NettyHeaders.ACCEPT_ENCODING
  val AcceptLanguage = NettyHeaders.ACCEPT_LANGUAGE
  val AcceptRanges = NettyHeaders.ACCEPT_RANGES
  val AcceptPatch = NettyHeaders.ACCEPT_PATCH
  val AccessControlAllowCredentials = NettyHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS
  val AccessControlAllowHeaders = NettyHeaders.ACCESS_CONTROL_ALLOW_HEADERS
  val AccessControlAllowMethods = NettyHeaders.ACCESS_CONTROL_ALLOW_METHODS
  val AccessControlAllowOrigin = NettyHeaders.ACCESS_CONTROL_ALLOW_ORIGIN
  val AccessControlExposeHeaders = NettyHeaders.ACCESS_CONTROL_EXPOSE_HEADERS
  val AccessControlMaxAge = NettyHeaders.ACCESS_CONTROL_MAX_AGE
  val AccessControlRequestHeaders = NettyHeaders.ACCESS_CONTROL_REQUEST_HEADERS
  val AccessControlRequestMethod = NettyHeaders.ACCESS_CONTROL_REQUEST_METHOD
  val Age = NettyHeaders.AGE
  val Allow = NettyHeaders.ALLOW
  val Authorization = NettyHeaders.AUTHORIZATION
  val CacheControl = NettyHeaders.CACHE_CONTROL
  val Connection = NettyHeaders.CONNECTION
  val ContentBase = NettyHeaders.CONTENT_BASE
  val ContentEncoding = NettyHeaders.CONTENT_ENCODING
  val ContentLanguage = NettyHeaders.CONTENT_LANGUAGE
  val ContentLength = NettyHeaders.CONTENT_LENGTH
  val ContentLocation = NettyHeaders.CONTENT_LOCATION
  val ContentTransferEncoding = NettyHeaders.CONTENT_TRANSFER_ENCODING
  val ContentDisposition = NettyHeaders.CONTENT_DISPOSITION
  val ContentMD5 = NettyHeaders.CONTENT_MD5
  val ContentRange = NettyHeaders.CONTENT_RANGE
  val ContentSecurityPolicy = NettyHeaders.CONTENT_SECURITY_POLICY
  val ContentType = NettyHeaders.CONTENT_TYPE
  val Cookie = NettyHeaders.COOKIE
  val Date = NettyHeaders.DATE
  val ETag = NettyHeaders.ETAG
  val Expect = NettyHeaders.EXPECT
  val Expires = NettyHeaders.EXPIRES
  val From = NettyHeaders.FROM
  val Host = NettyHeaders.HOST
  val IfMatch = NettyHeaders.IF_MATCH
  val IfModifiedSince = NettyHeaders.IF_MODIFIED_SINCE
  val IfNoneMatch = NettyHeaders.IF_NONE_MATCH
  val IfRange = NettyHeaders.IF_RANGE
  val IfUnmodifiedSince = NettyHeaders.IF_UNMODIFIED_SINCE
  val LastModified = NettyHeaders.LAST_MODIFIED
  val Location = NettyHeaders.LOCATION
  val MaxForwards = NettyHeaders.MAX_FORWARDS
  val Origin = NettyHeaders.ORIGIN
  val Pragma = NettyHeaders.PRAGMA
  val ProxyAuthenticate = NettyHeaders.PROXY_AUTHENTICATE
  val ProxyAuthorization = NettyHeaders.PROXY_AUTHORIZATION
  val Range = NettyHeaders.RANGE
  val Referrer = NettyHeaders.REFERER
  val RetryAfter = NettyHeaders.RETRY_AFTER
  val SecWebsocketKey1 = NettyHeaders.SEC_WEBSOCKET_KEY1
  val SecWebsocketKey2 = NettyHeaders.SEC_WEBSOCKET_KEY2
  val SecWebsocketLocation = NettyHeaders.SEC_WEBSOCKET_LOCATION
  val SecWebsocketOrigin = NettyHeaders.SEC_WEBSOCKET_ORIGIN
  val SecWebsocketProtocol = NettyHeaders.SEC_WEBSOCKET_PROTOCOL
  val SecWebsocketVersion = NettyHeaders.SEC_WEBSOCKET_VERSION
  val SecWebsocketKey = NettyHeaders.SEC_WEBSOCKET_KEY
  val SecWebsocketAccept = NettyHeaders.SEC_WEBSOCKET_ACCEPT
  val SecWebsocketExtensions = NettyHeaders.SEC_WEBSOCKET_EXTENSIONS
  val Server = NettyHeaders.SERVER
  val SetCookie = NettyHeaders.SET_COOKIE
  val SetCookie2 = NettyHeaders.SET_COOKIE2
  val TE = NettyHeaders.TE
  val Trailer = NettyHeaders.TRAILER
  val TransferEncoding = NettyHeaders.TRANSFER_ENCODING
  val Upgrade = NettyHeaders.UPGRADE
  val UserAgent = NettyHeaders.USER_AGENT
  val Vary = NettyHeaders.VARY
  val Via = NettyHeaders.VIA
  val Warning = NettyHeaders.WARNING
  val WebSocketLocation = NettyHeaders.WEBSOCKET_LOCATION
  val WebSocketOrigin = NettyHeaders.WEBSOCKET_ORIGIN
  val WebSocketProtocol = NettyHeaders.WEBSOCKET_PROTOCOL
  val WWWAuthenticate = NettyHeaders.WWW_AUTHENTICATE
  val XFrameOptions = NettyHeaders.X_FRAME_OPTIONS
}
