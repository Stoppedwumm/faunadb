package fauna.prop

trait ResponseCodes {
  val OK = 200
  val Created = 201
  val NoContent = 204
  val Redirect = 302

  val BadRequest = 400
  val Unauthorized = 401
  val Forbidden = 403
  val NotFound = 404
  val MethodNotAllowed = 405
  val Conflict = 409
  val TransactionContention = Conflict
  val AccountDisabled = 410
  val PreconditionFailed = 412
  val TooManyRequests = 429
  val ProcessingTimeLimitExceeded = 440
  val InternalServerError = 500
  val ServiceUnavailable = 503
  val RequestEntityTooLarge = 413
}

trait Generators {
  val aName = Prop.alphaNumString(60, 60)
  val anIdentifier = Prop.identifierString(32, 32)
  val aUniqueLong = Prop.unique(Prop.long)
  val aUniqueName = Prop.unique(aName)
  val aUniqueIdentifier = Prop.unique(anIdentifier)
  val aUniqueString = Prop.unique(Prop.string)
}
