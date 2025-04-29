package fauna.tools.error

import fauna.codex.json.{JSObject, JSValue}

/**
  * Types for known error messages that are returned from the admin server.
  */
sealed trait Error
object Error {

  case object Unauthorized extends Error
  case class BadRequest(description: String) extends Error
  case class ClusterUninitialized(description: String, statusCode: Int, json: JSValue) extends Error

  /**
    * Unknown messages that currently do not have types associated with them.
    * Custom text messages can be built from this type.
    */
  case class Unknown(code: String, description: String, statusCode: Int, json: JSValue) extends Error

  /**
    * Converts json that contains a list of errors it to a typed Error message.
    *
    * If the JSON is invalid, the error is converted to of type [[Error.Unknown]]
    */
  def toString(json: JSValue, statusCode: Int, message: ErrorMessage): String =
    toString(toErrors(json, statusCode), message)

  /**
    * Converts all errors to a single String output which can be used to print or log.
    */
  private[error] def toString(errors: Seq[Error],
                              message: ErrorMessage): String =
    if (errors.size == 1) {
      s"Failed: ${message.toString(errors.head)}"
    } else {
      errors.foldLeft("Failed:") {
        case (old, next) =>
          old + s"\n - ${message.toString(next)}"
      }
    }

  /**
    * Returns a typed Error message for a valid input error json or [[Error.Unknown]]
    * if the input JSObject is not in a valid error message format.
    */
  private[error] def toError(json: JSObject, statusCode: Int): Error =
    (json / "description").asOpt[String] flatMap {
      description =>
        (json / "code").asOpt[String] map {
          case "unauthorized" =>
            Error.Unauthorized
          case "bad request" =>
            Error.BadRequest(description)
          case "cluster uninitialized" =>
            Error.ClusterUninitialized(description, statusCode, json)
          case code =>
            Error.Unknown(code, description, statusCode, json)
        }
    } getOrElse {
      Error.Unknown("Unknown", "Unknown error message", statusCode, json)
    }

  /**
    * Returns a typed Error messages for a valid input errors json or [[Error.Unknown]]
    * if the input JSObject is not in a valid errors messages format.
    */
  private[error] def toErrors(json: JSValue, statusCode: Int): Seq[Error] =
    (json / "errors").asOpt[Seq[JSObject]] map {
      errors: Seq[JSObject] =>
        errors map {
          error =>
            Error.toError(error, statusCode)
        }
    } getOrElse {
      Seq(Error.Unknown("Unknown", "Unknown error message", statusCode, json))
    }

}
