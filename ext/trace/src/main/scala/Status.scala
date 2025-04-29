package fauna.trace

import fauna.codex.json._
import java.util.ConcurrentModificationException
import java.util.concurrent.TimeoutException

object Status {
  val Default = OK("The operation completed successfully")

  implicit object JSEncoder extends JsonEncoder[Status] {
    def encodeTo(stream: JsonGenerator, value: Status): Unit =
      stream.put(JSObject("code" -> value.code, "description" -> value.description))
  }

  /**
    * Returns an appropriate Status for the provided HTTP code, as defined by OpenTelemetry[0].
    *
    * [0] https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/data-http.md
    */
  def forHTTP(code: Int, description: String): Status =
    code match {
      case i if i < 100 => UnknownError(description)
      case i if i >= 100 && i < 400 => Default
      case 401          => Unauthenticated(description)
      case 403          => PermissionDenied(description)
      case 404          => NotFound(description)
      case 429          => ResourceExhausted(description)
      case i if i < 500 => InvalidArgument(description)
      case 501          => Unimplemented(description)
      case 503          => Unavailable(description)
      case 504          => DeadlineExceeded(description)
      case i if i < 600 => InternalError(description)
      case _            => UnknownError(description)
    }

  /**
    * Returns an appropriate Status for common Java exceptions.
    */
  def forThrowable(ex: Throwable): Status = {
    val msg = String.valueOf(ex.getMessage)
    ex match {
      case _: ArithmeticException             => InvalidArgument(msg)
      case _: ConcurrentModificationException => Aborted(msg)
      case _: IllegalArgumentException        => InvalidArgument(msg)
      case _: IllegalStateException           => FailedPrecondition(msg)
      case _: NoSuchElementException          => NotFound(msg)
      case _: TimeoutException                => DeadlineExceeded(msg)
      case _: UnsupportedOperationException   => Unimplemented(msg)
      case _                                  => UnknownError(String.valueOf(ex))
    }
  }
}

/**
  * Status codes and descriptions for a Span. Codes match GRPC[0]
  * codes, following precedent set by the OpenTelemetry spec[1].
  *
  * [0] https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
  * [1] https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/api-tracing.md#status
  */
sealed abstract class Status(val code: Int) {
  def description: String
}

case class OK(description: String) extends Status(0)
case class Cancelled(description: String) extends Status(1)
case class UnknownError(description: String) extends Status(2)
case class InvalidArgument(description: String) extends Status(3)
case class DeadlineExceeded(description: String) extends Status(4)
case class NotFound(description: String) extends Status(5)
case class AlreadyExists(description: String) extends Status(6)
case class PermissionDenied(description: String) extends Status(7)
case class ResourceExhausted(description: String) extends Status(8)
case class FailedPrecondition(description: String) extends Status(9)
case class Aborted(description: String) extends Status(10)
case class OutOfRange(description: String) extends Status(11)
case class Unimplemented(description: String) extends Status(12)
case class InternalError(description: String) extends Status(13)
case class Unavailable(description: String) extends Status(14)
case class DataLoss(description: String) extends Status(15)

// NB. not defined by GRPC, follow OpenTelemetry
case class Unauthenticated(description: String) extends Status(16)
