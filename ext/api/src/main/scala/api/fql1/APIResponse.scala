package fauna.api.fql1

import fauna.ast._
import fauna.atoms._
import fauna.atoms.APIVersion
import fauna.auth.{ Auth, EvalAuth }
import fauna.codex.json._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ RenderContext, RenderError }
import fauna.model.stream._
import fauna.model.SchemaNames.PendingNames
import fauna.net.http._
import fauna.repo.query.Query
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import io.netty.handler.codec.http.HttpMethod
import io.netty.util.AsciiString
import scala.util.control.NoStackTrace

case class ResponseError(response: APIResponse) extends NoStackTrace

sealed abstract class APIResponse {
  val code: Int
  val headers: Seq[(AsciiString, AnyRef)]

  def toRenderedHTTPResponse(transactionTS: Timestamp): Query[HttpResponse] =
    toRenderedHTTPResponse(false, Map.empty, transactionTS)

  def toRenderedHTTPResponse(
    pretty: Boolean,
    pending: PendingNames,
    transactionTS: Timestamp): Query[HttpResponse]
}

sealed abstract class PlainResponse extends APIResponse {
  protected def contentType: String = ContentType.JSON

  protected def encoded(pretty: Boolean): Option[ByteBuf]

  def toHTTPResponse: HttpResponse = toHTTPResponse(false)

  def toHTTPResponse(pretty: Boolean): HttpResponse =
    encoded(pretty) match {
      case None      => HttpResponse(code, NoBody, headers)
      case Some(buf) => HttpResponse(code, Body(buf, contentType), headers)
    }

  def toRenderedHTTPResponse(
    pretty: Boolean,
    pending: PendingNames,
    transactionTS: Timestamp): Query[HttpResponse] =
    Query.value(toHTTPResponse(pretty))
}

object APIResponse {

  private[this] val QueryRenderMetric = "Query.Response.Render.Time"

  private[this] val alloc = ByteBufAllocator.DEFAULT

  final case class EmptyResponse(
    code: Int,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty)
      extends PlainResponse {

    protected def encoded(pretty: Boolean) = None
  }

  object TextResponse {
    def apply(code: Int, body: String, headers: Seq[(AsciiString, AnyRef)]): TextResponse =
      apply(code, body.toUTF8Buf, headers)

    def apply(code: Int, body: ByteBuf): TextResponse = apply(code, body, Seq.empty)
    def apply(code: Int, body: String): TextResponse = apply(code, body, Seq.empty)
  }
  final case class TextResponse(
    code: Int,
    body: ByteBuf,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty)
      extends PlainResponse {

    override def contentType = ContentType.Text

    protected def encoded(pretty: Boolean) = Some(body)
  }

  final case class JSONResponse(
    code: Int,
    json: JSObject,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty)
      extends PlainResponse {

    protected def encoded(pretty: Boolean) = Some(json.writeTo(alloc.buffer, pretty))
  }

  final case class QueryResponse(
    code: Int,
    result: Literal,
    auth: Auth,
    version: APIVersion,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty)
      extends APIResponse {

    def toRenderedHTTPResponse(
      pretty: Boolean,
      pending: PendingNames,
      transactionTS: Timestamp): Query[HttpResponse] =
      Query.timing(QueryRenderMetric) {
        RenderContext.renderTo(auth,
                               version,
                               transactionTS,
                               pending,
                               alloc.buffer,
                               result,
                               pretty) map { buf =>
          HttpResponse(code, Body(buf, ContentType.JSON), headers)
        }
      }
  }

  /**
    * TransactionContentionResponse indicates the query failed due to contention on one or more documents.
    * `scope` and `id` identify one (arbitrary) document that was contended on.
    */
  case class TransactionContentionResponse(version: APIVersion, scope: ScopeID, id: DocID) extends APIResponse {
    val code = 409
    // The structure mimics a QueryErrorResponse.
    // TODO: Generalize rendering for more error response types.
    val result = ObjectL(
      "errors" -> ArrayL(List(
        ObjectL(
          "ref" -> RefL(scope, id),
          "code" -> StringL("contended transaction"),
          "description" -> StringL("Transaction was aborted due to detection of concurrent modification.")
        )
      ))
    )
    val headers = Seq.empty

    def toRenderedHTTPResponse(
      pretty: Boolean,
      pending: PendingNames,
      transactionTS: Timestamp): Query[HttpResponse] =
      Query.timing(QueryRenderMetric) {
        RenderContext.renderTo(EvalAuth(scope),
                               version,
                               transactionTS,
                               pending,
                               alloc.buffer,
                               result,
                               pretty) map { buf =>
          HttpResponse(code, Body(buf, ContentType.JSON), headers)
        }
      }
  }

  /**
    * TransactionContentionNoInfo indicates the query failed due to contention,
    * but no more information is available.
    */
  val TransactionContentionNoInfo = ErrorResponse(
    409,
    "contended transaction",
    "Transaction was aborted due to detection of concurrent modification.")

  case class QueryErrorResponse(
    errs: List[Error],
    auth: Auth,
    version: APIVersion,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty)
      extends APIResponse {
    val code = Error.statusCode(errs)

    val result = ObjectL("errors" -> errLiteral(errs))

    private def errLiteral(errs: List[Error]): ArrayL = {
      ArrayL(errs map (x => errLiteral(x)))
    }

    private def errLiteral(oe: Error): ObjectL = {
      val cause = if (oe.cause.isEmpty) {
        ArrayL(List.empty)
      } else {
        errLiteral(oe.cause)
      }
      val failures = oe.validationFailures map { vf =>
        ObjectL("field" -> ArrayL(vf.path map { StringL(_) }),
                "code" -> StringL(vf.code),
                "description" -> StringL(RenderError.encodeErrors(vf, version)))
      }

      val elems = List.newBuilder[(String, Literal)]
      elems += ("position" -> ArrayL(oe.position.toElems map {
        _.fold(LongL(_), StringL(_))
      }))
      elems += ("code" -> StringL(oe.code))
      elems += ("description" -> StringL(RenderError.encodeErrors(oe, version)))

      if (failures.nonEmpty) elems += ("failures" -> ArrayL(failures))
      if (cause.elems.nonEmpty) elems += ("cause" -> cause)

      ObjectL(elems.result())
    }

    def toRenderedHTTPResponse(
      pretty: Boolean,
      pending: PendingNames,
      transactionTS: Timestamp): Query[HttpResponse] =
      Query.timing(QueryRenderMetric) {
        RenderContext.renderTo(auth,
                               version,
                               transactionTS,
                               pending,
                               alloc.buffer,
                               result,
                               pretty) map { buf =>
          HttpResponse(code, Body(buf, ContentType.JSON), headers)
        }
      }
  }

  final case class StreamResponse(
    auth: Auth,
    version: APIVersion,
    fields: Set[EventField],
    streamable: StreamableL)
      extends APIResponse {

    val code = 200
    val headers = Nil

    def toRenderedHTTPResponse(
      pretty: Boolean,
      pending: PendingNames,
      transactionTS: Timestamp): Query[HttpResponse] =
      Query.timing(QueryRenderMetric) {
        RenderContext.renderTo(
          auth,
          version,
          transactionTS,
          pending,
          alloc.buffer,
          streamable,
          pretty) map { buf =>
          HttpResponse(code, Body(buf, ContentType.JSON), headers)
        }
      }

    def toRenderedEventsAsHTTPResponse(ctx: StreamContext): HttpResponse = {
      val sub = StreamSubscription(ctx, streamable)
      val (id, buffs) =
        StreamRenderContext.render(auth, version, fields, ctx, sub.events)
      val resHeaders = (HTTPHeaders.FaunaStreamID -> id.toLong.toString) :: headers
      HttpResponse(code, Chunked(buffs, ContentType.JSON), resHeaders)
    }
  }

  final case class ErrorResponse(
    code: Int,
    message: String,
    humanDesc: String,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty)
      extends PlainResponse {

    val json = {
      val b = JSObject.newBuilder

      b += "code" -> message
      b += "description" -> humanDesc

      JSObject("errors" -> JSArray(b.result()))
    }

    protected def encoded(pretty: Boolean) = Some(json.writeTo(alloc.buffer, pretty))
  }

  val NoContent = EmptyResponse(204)

  def MethodNotAllowed(allowed: Seq[HttpMethod]) = {
    val allowedStr = allowed mkString ", "
    ErrorResponse(405,
                  "method not allowed",
                  s"Allowed methods are $allowedStr.",
                  headers = Seq(HTTPHeaders.Allow -> allowedStr))
  }

  // Most lookups are generated from canonical URIs
  val NotFound = ErrorResponse(404, "not found", "Not Found")

  def Redirect(url: String) =
    ErrorResponse(302, "found", s"Found $url", headers = Seq(HTTPHeaders.Location -> url))

  val BadRequest = ErrorResponse(400, "bad request", "Bad Request")

  def BadRequest(desc: String) = ErrorResponse(400, "bad request", desc)

  def Forbidden(desc: String) = ErrorResponse(403, "forbidden", desc)

  def ProcessingTimeLimitExceeded(desc: String) =  ErrorResponse(440, "processing time limit exceeded", desc)

  val Unauthorized = ErrorResponse(
    401,
    "unauthorized",
    "Unauthorized",
    headers = Seq(HTTPHeaders.WWWAuthenticate -> "Basic realm=\"Unauthorized\""))

  val ContactSupport = "Please create a ticket at https://support.fauna.com"

  val UpgradeOrContactSupport =
    "Please upgrade your plan or contact support at https://support.fauna.com"

  /**
    * AccountDisabled is returned when a customer's account has the
    * RunQueries feature disabled. No queries are permitted after
    * authentication.
    */
  val AccountDisabled = ErrorResponse(
    410,
    "permission denied",
    s"Your account has been disabled. $ContactSupport")

  /** AccountShellDisabled is returned when a customer makes a request from the Fauna Shell
    * and has the RunShell feature disabled.
    */
  val AccountShellDisabled = ErrorResponse(
    410,
    "permission denied",
    s"The Fauna Shell has been disabled for your account. $ContactSupport"
  )

  /** V4Disabled is returned when a customer's account has the V4 feature
    * disabled. Only V10 queries after permitted after authentication.
    */
  val V4Disabled = ErrorResponse(
    410,
    "permission denied",
    s"FQL v4 requests have been disabled for your account. $ContactSupport"
  )

  /** IndexBuildsDisabled is returned when a customer's account has
    * the RunTasks feature disabled. No index builds may be created or
    * executed.
    */
  val IndexBuildsDisabled = ErrorResponse(
    410,
    "permission denied",
    s"Index builds have been disabled for your account. $ContactSupport")

  /**
    * InternalErrors represent most unexpected errors, and should be
    * reported.
    */
  def InternalError(desc: String) =
    ErrorResponse(500,
                  "internal server error",
                  s"$desc\n$ContactSupport")

  def InternalError(ex: Throwable) =
    ErrorResponse(500,
                  "internal server error",
                  s"${ex.toString}\n$ContactSupport")

  /**
    * This is distinguished from an InternalError by being "expected"
    * when a cluster misconfiguration has occurred.
    */
  def OperatorError(desc: String) = ErrorResponse(540, "operator error", desc)

  /**
    * A RequestTimeout is raised after network stack (OSI layers < 5) timeouts.
    */
  def RequestTimeout(desc: String) = ErrorResponse(408, "request timeout", desc)

  /**
    * A ServiceTimeout is an application-level timeout - typically an
    * unresponsive remote FaunaDB process.
    */
  def ServiceTimeout(desc: String) = ErrorResponse(503, "time out", desc)

  val TooManyRequests =
    ErrorResponse(429, "too many requests", "Too many pending requests.")

  def TooManyRequests(desc: String) =
    ErrorResponse(429, "too many requests", desc)

  val RequestTooLarge =
    ErrorResponse(413, "request too large", "Request entity is too large.")

  val PreconditionFailed =
    ErrorResponse(412, "precondition failed", "Precondition failed.")

  /**
    * Response for when [[fauna.cluster.Hello.sendHello]] times out.
    */
  def HelloTimeout(message: String) =
    ErrorResponse(500, "hello timeout", message)
}
