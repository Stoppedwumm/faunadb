package fauna.api.fql1

import fauna.cluster._
import fauna.codex.json.JSObject
import fauna.flags.PingTracePercentage
import fauna.flags.RunQueries
import fauna.lang.Timing
import fauna.model._
import fauna.model.account.Account
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.trace.{ GlobalTracer, Server, Unavailable => TraceUnavailable }
import fauna.util.router._
import io.netty.handler.codec.http.HttpMethod
import java.util.concurrent.{ ThreadLocalRandom, TimeoutException }
import scala.concurrent.duration._

/** Implementation of "utility" endpoints.
  * TODO: This should not belong to the fql1 subpackage. Would _really_
  * like to get routing to the top level rather than that gross if statement
  * we currently have.
  */
object UtilRoutes extends RouteDSL[APIRequest, Query[APIResponse]] {

  val UtilPaths = Set("/ping", "/error", "/authorized")
  def isUtilPath(path: String) = UtilPaths.contains(path)

  def apply(req: APIRequest): Query[APIResponse] = {
    val epQ = Query(req.method match {
      case HttpMethod.HEAD => endpoints(HttpMethod.GET, req.path)
      case method          => endpoints(method, req.path)
    })

    val resQ = epQ flatMap {
      case NotFound                  => Query(APIResponse.NotFound)
      case MethodNotAllowed(allowed) => Query(APIResponse.MethodNotAllowed(allowed))
      case Handler(args, handler)    => handler(args, req)
    } recover { case _: RoutingException =>
      APIResponse.NotFound
    }

    resQ map { res =>
      if (req.method == HttpMethod.HEAD) {
        APIResponse.EmptyResponse(res.code, res.headers)
      } else {
        res
      }
    }
  }

  protected sealed abstract class Scope(val name: String) {
    override val toString = name.toLowerCase
  }
  protected case object All extends Scope("All")
  protected case object Write extends Scope("Write")
  protected case object Read extends Scope("Read")
  protected case object Node extends Scope("Node")

  GET / "ping" / { req =>
    def OK(scope: Scope) =
      APIResponse.JSONResponse(200, JSObject("resource" -> s"Scope $scope is OK"))

    def MissingData(scope: Scope) =
      APIResponse.InternalError(s"Scope $scope is missing data")

    def Timeout(scope: Scope, timeout: FiniteDuration) =
      APIResponse.ServiceTimeout(
        s"Scope $scope timed out after ${timeout.toMillis} milliseconds")

    def Unavailable(scope: Scope) =
      APIResponse.ServiceTimeout(s"Scope $scope is unavailable")

    def sampleDataQ(scope: Scope, linearized: Boolean) =
      Query.snapshotTime flatMap { snapshotTime =>
        val q = SampleData.check(snapshotTime)
        val lq = if (linearized) Query.linearized(q) else q
        lq map {
          case true  => OK(scope)
          case false => MissingData(scope)
        }
      }

    def runQ[T](repo: RepoContext, timeout: FiniteDuration, q: Query[T]): Query[T] =
      Query.future(repo.withQueryTimeout(timeout).result(q)) map { _.value }

    Query.repo flatMap { repo =>
      val (scope, defaultTimeout) = req.params.getOrElse("scope", "write") match {
        case "all"   => (All, 5000.millis) // all: full cluster
        case "write" => (Write, 1000.millis) // write: global write path
        case "read"  => (Read, 500.millis) // read: DC-local read path

        // prev aliases
        case "global" => (Write, 1000.millis) // write: global write path
        case "local"  => (Read, 500.millis) // read: DC-local read path

        case _ => (Node, 100.millis) // Node: single node health
      }

      if (
        CassandraService.instanceOpt.fold(false)(
          _.isRunning) && !CassandraService.shutdownRequested
      ) {
        val timeout = req.params.get("timeout") flatMap { _.toLongOption } map {
          _.millis
        } getOrElse defaultTimeout
        val timing = Timing.start

        repo.hostFlag(PingTracePercentage) flatMap { percentage =>
          val pingTracePercent = percentage.doubleValue

          val tracer = GlobalTracer.instance
          val spanBuilder = tracer
            .buildSpan("ping.write")
            .withKind(Server)
            .ignoreParent()

          if (
            scope == Write && pingTracePercent > 0 && ThreadLocalRandom
              .current()
              .nextDouble() <= pingTracePercent
          ) {
            spanBuilder.enableSampling()
          }

          val span = spanBuilder.start()
          val spanScope = tracer.activate(span)

          (scope match {
            case Node  => Query(OK(scope))
            case Read  => runQ(repo, timeout, sampleDataQ(scope, linearized = false))
            case Write => runQ(repo, timeout, sampleDataQ(scope, linearized = true))
            case All =>
              runQ(
                repo,
                timeout,
                repo.isClusterHealthyQ map {
                  if (_) OK(scope) else Unavailable(scope)
                })
          }) map { res =>
            repo.stats.timing(s"Ping.${scope.name}.Time", timing.elapsedMillis)
            res
          } recover {
            case _: TimeoutException =>
              val elapsed = timing.elapsedMillis
              repo.stats.timing(s"Ping.${scope.name}.Timeout.Time", elapsed)
              span.setStatus(TraceUnavailable("Timed Out"))
              Timeout(scope, timeout)

            case _: ClusterTopologyException =>
              span.setStatus(TraceUnavailable("Cluster Topology Exception"))
              Unavailable(scope)
          } ensure {
            spanScope foreach { _.close() }
            Query.unit
          }
        }
      } else {
        Query(Unavailable(scope))
      }
    }
  }

  GET / "error" / { _ =>
    sys.error("Sample error")
  }

  GET / "authorized" / { req =>
    req.getAuth match {
      case Some(auth) =>
        Account.flagForAccount(auth.accountID, RunQueries).flatMap {
          case false => Query.value(APIResponse.AccountDisabled)
          case true  =>
            // Return the account ID of the authenticated request
            // for Core Router to cache along with the key.
            Query.value(
              APIResponse.JSONResponse(
                200,
                JSObject(
                  "account" -> auth.database.accountID.toLong.toString,
                  "global_id" -> Database.encodeGlobalID(auth.database.globalID))))
        }

      case _ => Query.value(APIResponse.Unauthorized)
    }
  }
}
