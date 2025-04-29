package fauna.api.fql1

import com.fasterxml.jackson.core.exc.StreamConstraintsException
import fauna.api.APIEndpoint
import fauna.ast._
import fauna.atoms._
import fauna.auth.Auth
import fauna.config.CoreConfig
import fauna.flags.AllowV4Queries
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.account.Account
import fauna.model.schema.PublicCollectionID
import fauna.model.stream._
import fauna.model.LegacyRefParser.Path
import fauna.model.RefParser.RefScope
import fauna.net.http.HTTPHeaders
import fauna.net.RateLimiter
import fauna.repo._
import fauna.repo.query.Query
import fauna.repo.service.rateLimits._
import fauna.stats.QueryMetrics
import fauna.util.router._
import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpMethod
import scala.util.{ Failure, Success }

object FQL1Routes extends RouteDSL[APIRequest, Query[APIResponse]] {

  def isV4Enabled(req: APIRequest) = {
    // the CLI sends requests with this header and we exempt those requests.
    // this is not foolproof - the header can be added by any client - but
    // should serve to block users from v4 access in most cases while we
    // migrate the v4 calls the CLI makes to v10 syntax. afterwards, we can
    // remove this conditional.
    def isShell = req.getHeader(HTTPHeaders.FaunaShellBuiltin).contains("true")

    Account.flagForAccount(req.auth.accountID, AllowV4Queries).map { hasFlag =>
      hasFlag || isShell
    }
  }

  def apply(config: CoreConfig, req: APIRequest): Query[APIResponse] = {
    val handler: Option[() => Query[APIResponse]] = req.path match {
      case "/" | "" =>
        req.method match {
          case HttpMethod.GET  => Some(() => runQuery(req, true))
          case HttpMethod.POST => Some(() => runQuery(req, false))
          case _ => Some(() => allowed(HttpMethod.GET, HttpMethod.POST))
        }

      case "/stream" =>
        req.method match {
          case HttpMethod.POST => Some(() => runStream(req))
          case _               => Some(() => allowed(HttpMethod.POST))
        }

      case "/linearized" =>
        Some(() => linearized(req))

      case _ =>
        // The very old, now-undocumented V4 REST API
        parseRefPath(req).map(path => () => routeRefPath(config, req, path))
    }

    handler match {
      case None =>
        Query.value(APIResponse.NotFound)
      case Some(h) =>
        if (req.getAuth.isEmpty) {
          Query.value(APIResponse.Unauthorized)
        } else {
          isV4Enabled(req).flatMap {
            case false => Query.value(APIResponse.V4Disabled)
            case true  => h()
          }
        }
    }
  }

  private def linearized(req: APIRequest): Query[APIResponse] =
    req.method match {
      case HttpMethod.POST => runQuery(req, isGET = false, linearized = true)
      case _               => allowed(HttpMethod.POST)
    }

  private def parseRefPath(req: APIRequest): Option[RefParser.RefScope.Path] =
    Path.parse(req.path drop 1)

  private def routeRefPath(
    config: CoreConfig,
    req: APIRequest,
    refPath: RefParser.RefScope.Path): Query[APIResponse] =
    refPath match {
      case r: RefScope.Ref =>
        symbolizeRef(req, r) {
          case ref @ RefL(_, CollectionID(PublicCollectionID(_))) =>
            req.method match {
              case HttpMethod.GET => runPaginate(req, ref, false)
              case HttpMethod.POST =>
                runRESTWithBody(req) { ps =>
                  ObjectL("create" -> ref, "params" -> ps)
                }
              case _ => allowed(HttpMethod.GET, HttpMethod.POST)
            }

          case ref @ RefL(_, DocID(_, collID)) =>
            req.method match {
              case HttpMethod.GET => runREST(req, ObjectL("get" -> ref))
              case HttpMethod.POST =>
                runRESTWithBody(req) { ps =>
                  ObjectL("create" -> ref, "params" -> ps)
                }
              case HttpMethod.PUT =>
                collID match {
                  case KeyID.collID
                      if config.key_forbid_user_import && !Auth.isRoot(req.auth) =>
                    // Key requests are handled differently: updating or
                    // replacing a key
                    // document is forbidden because
                    // 1. Doing this can break your acces.
                    // 2. Users ought to manage keys with a key API. Keys are stored
                    // like regular
                    //    documents but they are special.
                    // 3. If a user imports a key from a database in one region to
                    // the same database
                    // in a different region, they can break our reouting, which can
                    // cause
                    //    (effectively) data corruption.
                    Query(APIResponse.Forbidden(
                      "Replacing a key with PUT is not allowed: create a replacement key, then delete this key."))
                  case _ =>
                    runRESTWithBody(req) { ps =>
                      ObjectL("replace" -> ref, "params" -> ps)
                    }
                }
              case HttpMethod.PATCH =>
                collID match {
                  case KeyID.collID
                      if config.key_forbid_user_import && !Auth.isRoot(req.auth) =>
                    // See the big comment in the PUT case.
                    Query(APIResponse.Forbidden(
                      "Updating a key with PATCH is not allowed: use the Update FQL function to change key's user metadata."))
                  case _ =>
                    runRESTWithBody(req) { ps =>
                      ObjectL("update" -> ref, "params" -> ps)
                    }
                }
              case HttpMethod.DELETE => runREST(req, ObjectL("delete" -> ref))
              case _ =>
                allowed(
                  HttpMethod.GET,
                  HttpMethod.POST,
                  HttpMethod.PUT,
                  HttpMethod.PATCH,
                  HttpMethod.DELETE)
            }

          case _ => Query(APIResponse.NotFound)
        }

      case RefScope.EventsRef(r) =>
        symbolizeRef(req, r) {
          case ref @ RefL(_, _) =>
            req.method match {
              case HttpMethod.GET => runPaginate(req, ref, true)
              case _              => allowed(HttpMethod.GET)
            }

          case _ => Query(APIResponse.NotFound)
        }

      case _: RefScope.EventRef => Query(APIResponse.NotFound)
    }

  private def allowed(allowed: HttpMethod*) =
    Query(APIResponse.MethodNotAllowed(allowed))

  private def symbolizeRef(req: APIRequest, ref: RefScope.Ref)(
    f: Literal => Query[APIResponse]): Query[APIResponse] =
    ModelData.lookupRef(req.auth, ref) flatMap {
      case None    => Query.value(APIResponse.NotFound)
      case Some(r) => f(r)
    }

  private def parseJSON(buf: ByteBuf): Either[List[Error], Literal] =
    APIEndpoint.tryParseJSON[Literal](buf) match {
      case Success(js) => Right(js)
      case Failure(_: java.lang.StackOverflowError) =>
        Left(List(InvalidJSON("Request body JSON exceeds nesting limit.")))
      case Failure(e: StreamConstraintsException)
          if e.getMessage contains "Document nesting depth" =>
        Left(List(InvalidJSON("Request body JSON exceeds nesting limit.")))
      case Failure(_) =>
        Left(List(InvalidJSON("Request body is not valid JSON.")))
    }

  private def parseBody(req: APIRequest): Query[Either[List[Error], Literal]] =
    Query.timing("REST.Body.Parse.Time") {
      req.body map { buf =>
        Query(parseJSON(buf)) flatMapT { r =>
          EscapesParser.parse(req.auth, r, req.version, RootPosition) map {
            _.toEither
          }
        }
      } getOrElse Query(Right(ObjectL.empty))
    }

  private def parseExpr(req: APIRequest)(
    js: => Option[ByteBuf]): Query[Either[List[Error], Expression]] =
    Query.timing("Query.Parse.Time") {
      js map { parseJSON(_) } match {
        case Some(Right(r)) => QueryParser.parse(req.auth, r, req.version)
        case Some(Left(es)) => Query(Left(es))
        case None           => Query(Left(List(QueryNotFound)))
      }
    }

  private def parseEventFields(
    req: APIRequest): Query[Either[List[Error], Set[EventField]]] = {

    val fields = Set.newBuilder[EventField]
    val errors = List.newBuilder[Error]

    def parseFields(fieldNames: Array[String]): Unit =
      fieldNames foreach { name =>
        val trimmed = name.trim
        EventField(trimmed) match {
          case Some(field) => fields += field
          case None =>
            errors += InvalidURLParam("fields", s"Invalid field name '$trimmed'")
        }
      }

    req.params.get("fields") match {
      case Some(param) => parseFields(param.split(","))
      case None        => fields ++= EventField.Defaults
    }

    val errs = errors.result()
    Query.value(Either.cond(errs.isEmpty, fields.result(), errs))
  }

  private def runQuery(
    req: APIRequest,
    isGET: Boolean,
    linearized: Boolean = false): Query[APIResponse] = {
    val expr = parseExpr(req) {
      if (isGET) {
        req.params.get("q") map { _.toUTF8Buf }
      } else {
        req.body
      }
    }

    val ec: Timestamp => EvalContext =
      if (isGET) {
        EvalContext.read(req.auth, _, req.version, "get requests")
      } else {
        EvalContext.write(req.auth, _, req.version)
      }

    val q = expr flatMapT { expr =>
      Query.snapshotTime flatMap { ec(_).timedEvalTopLevel(expr) }
    } flatMap {
      toResponse(req, _)
    }

    if (linearized) Query.linearized(q) else q
  }

  private def runStream(req: APIRequest): Query[APIResponse] =
    parseEventFields(req) flatMapT { fields =>
      parseExpr(req) { req.body } flatMapT { expr =>
        Query.snapshotTime flatMap { snapshotTS =>
          req.auth.limiter.tryAcquireStream() match {
            case RateLimiter.DelayUntil(_) =>
              Query.repo flatMap { repo =>
                repo.stats.incr(QueryMetrics.RateLimitStream)

                Query.state flatMap { state =>
                  Query.fatal(OpsLimitExceptionWithMetrics("streams", state.metrics))
                }
              }

            case _: RateLimiter.Acquired =>
              val ctx = EvalContext.read(
                req.auth,
                snapshotTS,
                req.version,
                "stream requests")
              ctx.timedEvalTopLevel(expr) flatMapT { l =>
                Query.value(Casts.Streamable(l, RootPosition)) mapT { streamable =>
                  (fields, streamable)
                }
              }
          }
        }
      }
    } flatMap {
      toStreamResponse(req, _)
    }

  private def runREST(req: APIRequest, q: Literal): Query[APIResponse] =
    Query.snapshotTime flatMap { snapshotTime =>
      EvalContext
        .write(req.auth, snapshotTime, req.version)
        .timedParseAndEvalTopLevel(q) flatMap {
        toResponse(req, _)
      }
    }

  private def runRESTWithBody(req: APIRequest)(
    f: Literal => Literal): Query[APIResponse] =
    parseBody(req) flatMapT { body =>
      val quoted = ObjectL("quote" -> body)
      Query.snapshotTime flatMap { snapshotTime =>
        EvalContext
          .write(req.auth, snapshotTime, req.version)
          .timedParseAndEvalTopLevel(f(quoted))
      }
    } flatMap {
      toResponse(req, _)
    }

  private def runPaginate(
    req: APIRequest,
    set: Literal,
    isEvents: Boolean): Query[APIResponse] =
    getPaginateParams(req) flatMap { params =>
      val (before, after, size) = params

      val b = List.newBuilder[(String, Literal)]

      b += ("paginate" -> set)
      b += ("size" -> (size getOrElse LongL(DefaultPageSize)))
      before foreach { v =>
        b += ("before" -> ObjectL("quote" -> v))
      }
      after foreach { v =>
        b += ("after" -> ObjectL("quote" -> v))
      }
      if (isEvents) b += ("events" -> TrueL)

      runREST(req, ObjectL(b.result()))
    }

  private def getPaginateParams(
    req: APIRequest): Query[(Option[Literal], Option[Literal], Option[Literal])] = {
    def parse(opt: Option[String]): Query[Option[Literal]] =
      Query(opt) flatMapT { curs =>
        val objQ = Query(parseJSON(curs.toUTF8Buf).toOption) flatMapT { r =>
          EscapesParser.parse(req.auth, r, req.version, RootPosition) map {
            _.toOption
          }
        }

        // allow raw TS. return partial event cursor
        val tsQ = Query(curs.toLongOption) mapT { ts =>
          ObjectL("ts" -> LongL(ts))
        }

        // allow raw ref. return RefL
        val refQ =
          EscapesParser.parseRef(req.auth, curs, req.version, RootPosition) map {
            _.toOption
          }

        objQ orElseT tsQ orElseT refQ
      }

    val beforeQ = parse(req.params.get("before"))
    val afterQ = parse(req.params.get("after"))
    val sizeQ = Query(req.params.get("size") flatMap { _.toLongOption } map LongL)

    (beforeQ, afterQ, sizeQ) par { (b, a, s) =>
      Query((b, a, s))
    }
  }

  private def toAPIResponse[A](req: APIRequest, q: Either[List[Error], A])(
    fn: A => APIResponse): Query[APIResponse] =
    q match {
      case Right(res) => Query(fn(res))
      case Left(errs) =>
        Query.repo flatMap { repo =>
          Query.state map { state =>
            val aborted = errs exists {
              case TransactionAbort(_, _) => true
              case _                      => false
            }

            if (aborted) {
              // commit any metrics accrued prior to a user-initiated
              // abort
              state.metrics.commitReads(repo.stats, req.auth.accountID)
            }

            // this throw will abort any pending writes within the transaction
            throw ResponseError(
              APIResponse.QueryErrorResponse(errs, req.auth, req.version))
          }
        }
    }

  private def toResponse(req: APIRequest, q: Either[List[Error], Literal]) =
    toAPIResponse(req, q) { lit =>
      val code = lit match {
        case VersionL(_, true) => 201
        case _                 => 200
      }

      val resource = ObjectL("resource" -> lit)
      APIResponse.QueryResponse(code, resource, req.auth, req.version)
    }

  private def toStreamResponse(
    req: APIRequest,
    q: Either[List[Error], (Set[EventField], StreamableL)]) =
    toAPIResponse(req, q) { case (fields, streamable) =>
      APIResponse.StreamResponse(req.auth, req.version, fields, streamable)
    }
}
