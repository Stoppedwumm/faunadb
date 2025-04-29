package fauna.api.test

import io.netty.handler.codec.http.HttpMethod._
import fauna.util.router._

class RouterSpec extends Spec {

  "GenericRouter" - {

    val router = Router[String](
      (GET, "/", "home"),
      (GET, "/users", "get users"),
      (POST, "/users", "post users"),
      (PUT, "/users/{id}", "put users")
    )

    "returns the root route for /" in {
      router(GET, "/") should equal (Handler(List.empty, "home"))
    }

    "returns matched wildcard segments." in {
      router(PUT, "/users/123") should equal (Handler(List("123"), "put users"))
    }

    "returns matched wildcard with encoded segments." in {
      router(PUT, "/users/123%40456%2F789") should equal (Handler(List("123@456/789"), "put users"))
    }

    "returns no route if there is no handler for the path." in {
      router(GET, "/not_here") should equal (NotFound)
    }

    "returns not allowed if there is a handler, but the method is incorrect." in {
      router(PUT, "/users") should equal (MethodNotAllowed(Seq(GET, POST)))
    }
  }
}
