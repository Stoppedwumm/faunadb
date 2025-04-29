package fauna.api

import fauna.atoms.APIVersion
import fauna.lang.Timestamp
import fauna.net.http.{ HTTPHeaders, HttpRequest }
import io.netty.util.AsciiString
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

// Auxilliary parameters derived from the request prior to query execution.
trait RequestParam[ParamT] {
  // The name of an HTTP header which may contain the parameter value.
  val headerName: AsciiString

  // The default value of the parameter
  val default: ParamT

  // Parses the string to obtain the value of the parameter. If the string cannot
  // be parsed into a valid value for the parameter, returns a String explaining why.
  def parse(str: String): Either[String, ParamT]

  // Extracts the value of the parameter from its HTTP request header if present,
  // otherwise None. Throws a RequestParams.Invalid exception if the header is
  // present but cannot be parsed into a valid value for the parameter.
  private def parseHeader(req: HttpRequest): Option[ParamT] =
    req.getHeader(headerName) map { headerVal =>
      parse(headerVal) match {
        case Left(err) =>
          throw RequestParams.InvalidHeader(headerName, err)
        case Right(v) => v
      }
    }

  // Makes use of the header if present, otherwise falls back to the value from
  // the request body. If either value is present, it must be valid or this may
  // throw a RequestParams.Invalid exception. If neither is present, uses the
  // default value.
  def apply(req: HttpRequest): ParamT =
    parseHeader(req) getOrElse default
}

object RequestParams {
  case class Invalid(message: String) extends Exception(message) with NoStackTrace
  def InvalidHeader(header: AsciiString, error: String) = Invalid(
    s"Invalid header '$header': $error")

  // FQL2 uses a path component for the version
  object APIVersionParam extends RequestParam[APIVersion] {
    val headerName = HTTPHeaders.FaunaDBAPIVersion
    val default = APIVersion.Default

    def parse(str: String) = str match {
      case APIVersion(v) => Right(v)
      case _             => Left(s"Unsupported or invalid API version '$str'.")
    }
  }

  // FQL1 wire protocol variant
  object LastSeenTxn extends RequestParam[Timestamp] {
    val headerName = HTTPHeaders.LastSeenTxn
    val default = Timestamp.Min

    def parse(str: String) = str.toLongOption match {
      case None    => Left(s"Invalid timestamp '$str'.")
      case Some(l) => Right(Timestamp.ofMicros(l))
    }
  }

  // FQL2 wire protocol variant
  object LastTxnTs extends RequestParam[Timestamp] {
    val headerName = HTTPHeaders.LastTxnTs
    val default = Timestamp.Epoch

    def parse(str: String) = str.toLongOption match {
      case None    => Left(s"Invalid timestamp '$str'.")
      case Some(l) => Right(Timestamp.ofMicros(l))
    }
  }

  protected trait AbstractTimeout extends RequestParam[FiniteDuration] {
    val default = CoreApplication.DefaultQueryTimeout

    def parse(str: String) =
      str.toLongOption match {
        case None => Left(s"Invalid number '$str'.")
        case Some(l) if l <= 0 =>
          Left(s"Must be greater than 0.")
        case Some(l) if l > CoreApplication.MaximumQueryTimeoutMillis =>
          Left(
            s"Must be less than or equal to ${CoreApplication.MaximumQueryTimeoutMillis}.")
        case Some(l) =>
          // .millis can throw if the Long is not +-(2^63-1)ns
          // but this is protected by checking against MaximumQueryTimeoutMillis
          Right(l.millis)
      }
  }

  // FQL1 wire protocol variant
  object QueryTimeout extends AbstractTimeout {
    val headerName = HTTPHeaders.QueryTimeout
  }

  // FQL2 wire protocol variant
  object QueryTimeoutMs extends AbstractTimeout {
    val headerName = HTTPHeaders.QueryTimeoutMs
  }

  protected trait AbstractContentionRetries extends RequestParam[Option[Int]] {
    val default = None

    def parse(str: String) =
      str.toIntOption match {
        case None                            => Left(s"Invalid number '$str'.")
        case Some(rts) if rts < 0 || rts > 7 => Left("Must be between 0 and 7.")
        case Some(rts)                       => Right(Some(rts))
      }
  }

  // FQL2 wire protocol variant
  object MaxContentionRetries extends AbstractContentionRetries {
    val headerName = HTTPHeaders.MaxContentionRetries
  }

  // FQL1 wire protocol variant
  object MaxRetriesOnContention extends AbstractContentionRetries {
    val headerName = HTTPHeaders.MaxRetriesOnContention
  }

  object Linearized extends RequestParam[Boolean] {
    val headerName = HTTPHeaders.Linearized
    val default = false

    def parse(str: String) =
      str.toBooleanOption match {
        case Some(bool) => Right(bool)
        case None       => Left("Must be a boolean value.")
      }
  }

  object Typecheck extends RequestParam[Option[Boolean]] {
    val headerName = HTTPHeaders.Typecheck
    val default = None

    def parse(str: String) =
      str.toBooleanOption match {
        case Some(bool) => Right(Some(bool))
        case None       => Left("Must be a boolean value.")
      }
  }

  object PerformanceHints extends RequestParam[Boolean] {
    val headerName = HTTPHeaders.PerformanceHints
    val default = false

    def parse(str: String) =
      str.toBooleanOption match {
        case Some(bool) => Right(bool)
        case None       => Left("Must be a boolean value.")
      }
  }
}
