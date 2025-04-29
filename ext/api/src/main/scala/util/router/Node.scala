package fauna.util.router

import fauna.trace.traceMsg
import io.netty.handler.codec.http.HttpMethod
import scala.collection.mutable.{ HashMap => MHashMap }

object Node {
  def apply[T](method: HttpMethod, pathString: String, handler: T) = {
    def recur(path: List[String]): Node[T] = path match {
      case Nil                 => new Node(Map(method -> ((Vector(), handler))), Map.empty, None)
      case Wildcard(_) :: rest => new Node(Map.empty, Map.empty, Some(recur(rest)))
      case component :: rest   => new Node(Map.empty, Map(component -> recur(rest)), None)
    }

    recur(TokenizePath(pathString))
  }
}

class Node[+T](
    val handlers: Map[HttpMethod, (Vector[String], T)],
    val tails: Map[String, Node[T]],
    val wildcard: Option[Node[T]]) {

  val allowedMethods = handlers.keySet.toSeq.sorted

  def apply(method: HttpMethod, path: List[String], args: Vector[String] = Vector()): Endpoint[T] =
    path match {
      case Nil =>
        handlers get method match {
          case Some((curried, handler)) =>
            traceMsg(s"  Routed to handler: ${handler}, args: ${curried ++ args}")
            Handler(curried ++ args, handler)
          case None =>
            traceMsg("  Route not found.")
            if (handlers.isEmpty) NotFound else MethodNotAllowed(allowedMethods)
        }
      case component :: rest =>
        tails get component match {
          case Some(node) =>
            node(method, rest, args)
          case None =>
            wildcard match {
              case Some(node) =>
                node(method, rest, args :+ component)
              case None =>
                traceMsg("Route not found.")
                NotFound
            }
        }
    }

  private[this] def mergeNodes[A](lhsOpt: Option[Node[A]], rhsOpt: Option[Node[A]]): Option[Node[A]] = {
    val merged = for (lhs <- lhsOpt; rhs <- rhsOpt) yield {
      val mergedHandlers = mergeHandlers(lhs.handlers, rhs.handlers)
      val mergedWildcard = mergeNodes(lhs.wildcard, rhs.wildcard)
      val mergedTails = mergeTails(lhs.tails, rhs.tails, mergedWildcard)

      new Node(mergedHandlers, mergedTails, mergedWildcard)
    }

    merged orElse rhsOpt orElse lhsOpt
  }

  private[this] def mergeHandlers[A](lhsHandlers: Map[HttpMethod, A], rhsHandlers: Map[HttpMethod, A]) = {
    lhsHandlers ++ rhsHandlers
  }

  private[this] def mergeTails[A](
    lhsTails: Map[String, Node[A]],
    rhsTails: Map[String, Node[A]],
    wildcard: Option[Node[A]]) = {

    val merged = new MHashMap[String, Node[A]]

    for (component <- lhsTails.keySet ++ rhsTails.keySet) {
      merged(component) = mergeNodes(
        wildcard map { _ curriedWith component },
        mergeNodes(
          lhsTails get component,
          rhsTails get component)).get
    }

    merged.toMap
  }

  private def curriedWith(arg: String): Node[T] = new Node(
    handlers map { case (m, (args, t)) => m -> ((args :+ arg, t)) },
    tails map { case (s, n) => s -> (n curriedWith arg) },
    wildcard map { _ curriedWith arg })

  def +[T1 >: T](other: Node[T1]) = mergeNodes(Some(this), Some(other)).get

  def paths: Seq[(String, HttpMethod)] = paths("")

  def paths(prefix: String): Seq[(String, HttpMethod)] = {
    val path = if (prefix.isEmpty) "/" else prefix
    (handlers.toSeq filter { _._2._1.isEmpty } sortBy { _._1.toString } map { case (method, _) => (path, method) }) ++
      (tails.toSeq sortBy { _._1 } flatMap { case (ps, node) => node.paths(s"$prefix/$ps") }) ++
      (wildcard.toList.flatMap { _.paths(s"$prefix/{arg}") })
  }
}
