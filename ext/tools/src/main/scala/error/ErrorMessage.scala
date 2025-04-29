package fauna.tools.error

import fauna.codex.json.JSValue

/**
  * A type that stores String messages for all [[Error]] type.
  *
  * Custom [[ErrorMessage]] can be created to display custom String
  * messages for errors returned from the server.
  */
sealed trait ErrorMessage {
  def toString(error: Error): String
}

object ErrorMessage {

  val replicaNameDefinedTwice = "Replica name can only be defined once. Please enter it via config or option."
  val replicaNameIsRequired = "Replica name is required."
  val unauthorized = "Unauthorized. Please check that the provided Root Key is correct."
  val helloTimeout = "Please make sure it is reachable and a member of an existing cluster."

  def clusterUninitialized(description: String) = s"$description\nUse faunadb-admin to join an existing cluster or initialize a new cluster."

  def unknown(json: JSValue, description: Option[String] = None) = {
    val descriptionString =
      description match {
        case Some(desc) => s"$desc "
        case None => ""
      }
    s"Unknown error. Please contact support. Message was $descriptionString$json"
  }

  /**
    * Common error messages that are returned from the Admin server.
    * Messages that are unknown get passed in as [[Error.Unknown]] type
    * for custom messages.
    */
  object Common extends ErrorMessage {
    override def toString(error: Error): String =
      error match {
        case Error.Unauthorized =>
          unauthorized

        case Error.BadRequest(description) =>
          description

        case Error.ClusterUninitialized(description, statusCode, json) =>
          if (statusCode == 503) {
            clusterUninitialized(description)
          } else {
            unknown(json, Some(description))
          }

        case Error.Unknown(_, description, _, json) =>
          unknown(json, Some(description))
      }
  }

  /**
    * Error messages for Join command.
    */
  object Join extends ErrorMessage {
    override def toString(error: Error): String =
      error match {
        //Only for join when a hello timeout occurs, convert it to helloTimeout message.
        case Error.Unknown("hello timeout", _, _, _) => helloTimeout
        //for any other code use the default.
        case error => Common.toString(error)
      }
  }
}
