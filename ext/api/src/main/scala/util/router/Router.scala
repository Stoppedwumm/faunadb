package fauna.util.router

import io.netty.handler.codec.http.HttpMethod

object Router {
  def apply[T](route: (HttpMethod, String, T), routes: (HttpMethod, String, T)*) = {
    val root = (route +: routes) map {
      case (method, path, handler) => Node(method, path, handler)
    } reduceLeft {
      _ + _
    }

    new Router(root)
  }
}

class Router[T](private var root: Node[T]) {

  def addRoute(method: HttpMethod, pathString: String, handler: T) =
    root = root + Node(method, pathString, handler)

  def apply(method: HttpMethod, pathString: String): Endpoint[T] = {
    val path = TokenizePath(pathString)

    root(method, path)
  }

  def show = root.paths map {
    case (path, method) => s"$method:${" " * (6 - method.toString.length)} $path"
  } mkString ("\n")
}

trait RouteDSL[Req, Res] {
  val endpoints = new Router[(Seq[String], Req) => Res](new Node(Map.empty, Map.empty, None))

  def routePrefix = "/"

  // Route DSL

  protected val stringP = StringParam
  protected def longP = LongParam(0, Long.MaxValue)
  protected def longP(max: Long) = LongParam(0, max)
  protected def longP(min: Long, max: Long) = LongParam(min, max)

  protected val GET = new NoParamBuilder(HttpMethod.GET, routePrefix, endpoints)
  protected val OPTIONS = new NoParamBuilder(HttpMethod.OPTIONS, routePrefix, endpoints)
  protected val POST = new NoParamBuilder(HttpMethod.POST, routePrefix, endpoints)
  protected val PUT = new NoParamBuilder(HttpMethod.PUT, routePrefix, endpoints)
  protected val PATCH = new NoParamBuilder(HttpMethod.PATCH, routePrefix, endpoints)
  protected val DELETE = new NoParamBuilder(HttpMethod.DELETE, routePrefix, endpoints)
}
