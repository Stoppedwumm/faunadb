package fauna.util.router

import io.netty.handler.codec.http.HttpMethod

sealed trait Endpoint[+T]

case object NotFound extends Endpoint[Nothing]
case class MethodNotAllowed(allowedMethods: Seq[HttpMethod]) extends Endpoint[Nothing]
case class Handler[+T](args: Seq[String], handler: T) extends Endpoint[T]
