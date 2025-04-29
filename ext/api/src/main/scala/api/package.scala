package fauna.api

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.TimeoutException
import scala.util.control.NoStackTrace

object AggressiveTimeoutException {
  def msg(expected: FiniteDuration, actual: FiniteDuration): String = {
    s"Client set aggressive deadline: query needs ${expected.toMillis}, timed out within ${actual.toMillis}"
  }
}

case class AggressiveTimeoutException(
  expected: FiniteDuration,
  actual: FiniteDuration,
  cause: TimeoutException)
    extends Exception(AggressiveTimeoutException.msg(expected, actual))

/** Thrown immediately after authentication to indicate an account has been disabled.
  *
  * See CoreApplication, AccountDisabled, and RunQueries.
  */
object AccountDisabledException extends NoStackTrace

/** Thrown immediately after authentication if the account has been disabled for the shell AND the request
  * is coming from the shell.
  */
object AccountShellDisabledException extends NoStackTrace

object LimitExceededException extends NoStackTrace

sealed abstract class ProtocolErrorCode(val httpStatus: Int)

object ProtocolErrorCode {
  case object Unauthorized extends ProtocolErrorCode(401)
  case object Forbidden extends ProtocolErrorCode(403)
  case object NotFound extends ProtocolErrorCode(404)
  case object MethodNotAllowed extends ProtocolErrorCode(405)
  case object Conflict extends ProtocolErrorCode(409)
  case object Disabled extends ProtocolErrorCode(410)
  case object BadRequest extends ProtocolErrorCode(400)
  case object RequestTooLarge extends ProtocolErrorCode(413)
  case object TooManyRequests extends ProtocolErrorCode(429)
  // This uses a non-standard code. See
  // https://docs.google.com/document/d/1ttgi_qEUXRmKz5F_qeY0EdDFDl_NggxhJKsZoOEz3QA/edit
  case object ProcessingTimeLimitExceeded extends ProtocolErrorCode(440)
  case object InternalError extends ProtocolErrorCode(500)
  case object ServiceTimeout extends ProtocolErrorCode(503)
  case object OperatorError extends ProtocolErrorCode(540)
  case object ContendedTransaction extends ProtocolErrorCode(409)
}
