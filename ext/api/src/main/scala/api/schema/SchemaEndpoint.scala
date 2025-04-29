package fauna.api.schema

import fauna.api.{ APIEndpoint, CoreAppContext, ExceptionResponseHelpers }
import fauna.atoms.SchemaVersion
import fauna.auth.Auth
import fauna.codex.json.JSValue
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.serialization.FQL2ValueMaterializer
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.schema.{
  Result,
  SchemaError,
  SchemaErrorsException,
  SchemaManager,
  SchemaSource,
  SchemaStatus
}
import fauna.model.schema.fsl.SourceFile
import fauna.model.schema.manager.ValidateResult
import fauna.model.schema.SchemaIndexStatus
import fauna.model.Cache
import fauna.net.http.HttpRequest
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.util.router._
import fql.ast._
import fql.error.ParseError
import fql.parser.Parser
import fql.schema.SchemaDiff
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString
import scala.concurrent.{ ExecutionContext, Future }

object SchemaEndpoint
    extends APIEndpoint
    with RouteDSL[SchemaRequest.Authenticated, Query[Result[AnySchemaResponse]]] {
  import APIEndpoint._
  import Result.{ Err, Ok }
  import SchemaResponse._
  import SourceFile._

  type Request = SchemaRequest.Parsed

  def tagsHeader = AsciiString.EMPTY_STRING

  /** Used to tag endpoint specific metrics.
    * Allows us to do things like separate out 500s by endpoint.
    */
  override def endpointMetricName: String = "schema"

  override def routePrefix = "/schema/1"

  // FSL Files API

  GET / "files" / { req =>
    req.checkSchemaVersion() flatMapT { version =>
      // If `?staged=true` is sent, grab the latest. Otherwise, grab the active
      // schema.
      val sourcesQ = if (req.isStaged) {
        SchemaSource.getStaged(req.scopeID)
      } else {
        SchemaSource.getActive(req.scopeID)
      }

      sourcesQ map { sources =>
        SchemaSources(version, sources).toResult
      }
    }
  }

  GET / "files" / stringP / { (filename, req) =>
    req.checkSchemaVersion() flatMapT { version =>
      // If `?staged=true` is sent, grab the latest. Otherwise, grab the active
      // schema.
      val doc = if (req.isStaged) {
        SchemaSource.get(req.scopeID, filename)
      } else {
        SchemaStatus.forScope(req.scopeID).flatMap { status =>
          status.activeSchemaVersion match {
            case Some(_) => SchemaSource.getActive(req.scopeID, filename)
            case None    => SchemaSource.get(req.scopeID, filename)
          }
        }
      }

      doc
        .mapT { SourceContent(version, _).toResult }
        .getOrElseT { SchemaError.NotFound.toResult }
    }
  }

  POST / "update" / { req =>
    req.checkWritePermission() flatMapT { _ =>
      req.checkSchemaVersion() flatMapT { _ =>
        req.disallowTenantRoot() flatMapT { _ =>
          // NB: We allow pushing schema to the root database, so that the docker
          // container works.
          val stageQ = if (req.isStaged) {
            req.disallowRoot() flatMapT { _ =>
              SchemaStatus.pin(req.ctx).map(_ => Ok(()))
            }
          } else {
            Ok(()).toQuery
          }

          stageQ.flatMapT { _ =>
            SchemaManager.update(
              req.ctx,
              req.files,
              overrideMode = req.isOverride
            ) flatMapT { _ =>
              SchemaUpdated.toQuery
            }
          }
        }
      }
    }
  }

  POST / "diff" / { req => diff(req) }
  POST / "validate" / { req => diff(req) }

  def diff(req: SchemaRequest.Authenticated) = {
    req.checkSchemaVersion() flatMapT { version =>
      SchemaManager.validate(
        req.scopeID,
        req.files,
        overrideMode = req.isOverride,
        staged = req.isStaged
      ) mapT { res =>
        Diff.fromValidate(version, res, req)
      }
    }
  }

  GET / "status" / { req => status(req) }
  GET / "staged" / "status" / { req => status(req) }

  def status(req: SchemaRequest.Authenticated) = {
    req.checkSchemaVersion() flatMapT { version =>
      req.disallowTenantRoot() flatMapT { _ =>
        req.disallowRoot() flatMapT { _ =>
          req.diffFormat match {
            case Some(diffKind) =>
              SchemaManager.stagedDiff(req.scopeID).flatMapT { diff =>
                for {
                  before <- SchemaSource.activeFSLFiles(req.scopeID)
                  after  <- SchemaSource.stagedFSLFiles(req.scopeID)
                } yield {
                  val beforeSrc = before.view map { file =>
                    file.src -> file.content
                  }
                  val afterSrc = after.view map { file => file.src -> file.content }
                  Result.Ok(
                    StagedSummary(
                      version,
                      diff,
                      beforeSrc.toMap,
                      afterSrc.toMap,
                      req.colorKind,
                      diffKind))
                }
              }

            case None =>
              SchemaIndexStatus.forScope(req.scopeID).flatMap { status =>
                StagedStatus(version, status).toQuery
              }
          }
        }
      }
    }
  }

  POST / "commit" / { req => commit(req) }
  POST / "staged" / "commit" / { req => commit(req) }

  def commit(req: SchemaRequest.Authenticated) = {
    req.checkWritePermission() flatMapT { _ =>
      req.disallowTenantRoot() flatMapT { _ =>
        req.disallowRoot() flatMapT { _ =>
          req.checkSchemaVersion() flatMapT { _ =>
            SchemaStatus.commit(req.ctx).mapT { _ => SchemaUpdated }
          }
        }
      }
    }
  }

  POST / "abandon" / { req => abandon(req) }
  POST / "staged" / "abandon" / { req => abandon(req) }

  def abandon(req: SchemaRequest.Authenticated) = {
    req.checkWritePermission() flatMapT { _ =>
      req.disallowTenantRoot() flatMapT { _ =>
        req.disallowRoot() flatMapT { _ =>
          req.checkSchemaVersion() flatMapT { _ =>
            SchemaStatus.abandon(req.ctx).mapT { _ => SchemaUpdated }
          }
        }
      }
    }
  }

  // Single Schema Item API

  singleItemEndpoints("collections", "a collection", SchemaItem.Kind.Collection)
  singleItemEndpoints("functions", "a function", SchemaItem.Kind.Function)
  singleItemEndpoints("roles", "a role", SchemaItem.Kind.Role)

  singleItemEndpoints(
    "access_providers",
    "an access provider",
    SchemaItem.Kind.AccessProvider
  )

  private def singleItemEndpoints(
    resource: String,
    typeDesc: String,
    kind: SchemaItem.Kind
  ): Unit = {
    GET / "items" / resource / stringP / { (name, req) =>
      req.checkSchemaVersion() flatMapT { version =>
        lookupItem(req, name) flatMapT { case (doc, file, span) =>
          Result.adaptT {
            FQL2ValueMaterializer
              .materialize(req.ctx, doc)
              .mapT {
                SourceItem(
                  version,
                  file.filename,
                  span.extract(file.content),
                  span,
                  _
                )
              }
          }
        }
      }
    }

    // Add an item.
    POST / "items" / resource / { req =>
      if (req.isValidate) {
        Cache.getLastSeenSchema(req.scopeID).flatMap { version =>
          parseSingleSchemaItem(req).toQuery flatMapT { case (_, snippet) =>
            SchemaSource.getOrCreate(req.scopeID, Builtin.Main) flatMap { src =>
              val updated = src.file.append(snippet)
              validateSingleFSLItem(req, updated).mapT { res =>
                Diff.fromValidate(version.getOrElse(SchemaVersion.Min), res, req)
              }
            }
          }
        }
      } else {
        req.checkWritePermission() flatMapT { _ =>
          req.checkSchemaVersion() flatMapT { _ =>
            parseSingleSchemaItem(req).toQuery flatMapT { case (item, snippet) =>
              SchemaSource.getOrCreate(req.scopeID, Builtin.Main) flatMap { src =>
                updateSingleFSLItem(req, item.name.str, src.file.append(snippet))
                  .flatMapT {
                    case Some(update) =>
                      Result.adaptT {
                        FQL2ValueMaterializer
                          .materialize(req.ctx, update.doc)
                          .mapT {
                            ItemUpdated(
                              src.filename,
                              snippet,
                              update.item.span,
                              _
                            )
                          }
                      }
                    case None =>
                      SchemaError
                        .Unexpected("Snippet produced no schema updates")
                        .toQuery
                  }
              }
            }
          }
        }
      }
    }

    // Replace an item.
    POST / "items" / resource / stringP / { (name, req) =>
      if (req.isValidate) {
        Cache.getLastSeenSchema(req.scopeID).flatMap { version =>
          parseSingleSchemaItem(req).toQuery flatMapT { case (parsedItem, snippet) =>
            lookupItem(req, name) flatMapT { case (_, file, span) =>
              val finalName = parsedItem.name.str
              val updatedFile = file.patch(span, snippet)
              val renames =
                Option.when(name != finalName) {
                  (kind, finalName) -> name
                }

              validateSingleFSLItem(req, updatedFile, renames.toMap).mapT { res =>
                Diff.fromValidate(version.getOrElse(SchemaVersion.Min), res, req)
              }
            }
          }
        }
      } else {
        req.checkWritePermission() flatMapT { _ =>
          req.checkSchemaVersion() flatMapT { version =>
            parseSingleSchemaItem(req).toQuery flatMapT {
              case (parsedItem, snippet) =>
                lookupItem(req, name) flatMapT { case (doc, file, span) =>
                  val finalName = parsedItem.name.str
                  val updatedFile = file.patch(span, snippet)
                  val renames =
                    Option.when(name != finalName) {
                      (kind, finalName) -> name
                    }

                  updateSingleFSLItem(req, finalName, updatedFile, renames.toMap)
                    .flatMapT {
                      case Some(update) =>
                        Result.adaptT {
                          FQL2ValueMaterializer
                            .materialize(req.ctx, update.doc)
                            .mapT {
                              ItemUpdated(
                                file.filename,
                                snippet,
                                update.item.span,
                                _
                              )
                            }
                        }
                      case None =>
                        Result.adaptT {
                          FQL2ValueMaterializer
                            .materialize(req.ctx, doc)
                            .mapT {
                              SourceItem(
                                version,
                                file.filename,
                                snippet,
                                span,
                                _
                              )
                            }
                        }
                    }
                }
            }
          }
        }
      }
    }

    // Delete an item.
    DELETE / "items" / resource / stringP / { (name, req) =>
      if (req.isValidate) {
        Cache.getLastSeenSchema(req.scopeID).flatMap { version =>
          lookupItem(req, name) flatMapT { case (_, file, span) =>
            val updatedFile = file.remove(span)
            validateSingleFSLItem(req, updatedFile).mapT { res =>
              Diff.fromValidate(version.getOrElse(SchemaVersion.Min), res, req)
            }
          }
        }
      } else {
        req.checkWritePermission() flatMapT { _ =>
          req.checkSchemaVersion() flatMapT { _ =>
            lookupItem(req, name) flatMapT { case (_, file, span) =>
              val updatedFile = file.remove(span)
              updateSingleFSLItem(req, name, updatedFile) flatMapT {
                case Some(update) =>
                  Result.adaptT {
                    FQL2ValueMaterializer
                      .materialize(req.ctx, update.doc)
                      .mapT {
                        ItemUpdated(
                          file.filename,
                          span.extract(file.content),
                          span,
                          _
                        )
                      }
                  }
                case None =>
                  SchemaError
                    .Unexpected("Snippet produced no schema updates")
                    .toQuery
              }
            }
          }
        }
      }
    }

    def lookupItem(
      req: SchemaRequest.Authenticated,
      name: String
    ): Query[Result[(Value.Doc, SourceFile.FSL, Span)]] =
      SchemaSource.locateFSLItem(req.scopeID, kind, name) flatMapT {
        case None => SchemaError.NotFound.toQuery
        case Some((docID, file, span)) =>
          Query.value(Ok((Value.Doc(docID), file, span)))
      }

    def parseSingleSchemaItem(
      req: SchemaRequest.Authenticated): Result[(SchemaItem, String)] =
      req.fslSnippet match {
        case Some(fsl) =>
          Result
            .adapt(Parser.schemaItems(fsl, Src.FSL(fsl)))
            .flatMap {
              case Seq(item) if item.kind == kind => Ok((item, fsl))
              case Seq(_) =>
                SchemaError
                  .Validation(s"Wrong FSL snippet. Expected $typeDesc")
                  .toResult
              case other =>
                SchemaError
                  .Validation(s"Expected a single schema item, got: ${other.size}")
                  .toResult
            }
        case None =>
          SchemaError.Validation("No FSL snippet provided").toResult
      }

    def updateSingleFSLItem(
      req: SchemaRequest.Authenticated,
      finalName: String,
      file: SourceFile.FSL,
      renames: SchemaDiff.Renames = Map.empty
    ): Query[Result[Option[SchemaManager.UpdatedItem]]] = {
      SchemaManager
        .update(req.ctx, Seq(file), renames, overrideMode = false)
        .map { res =>
          res
            .mapErr { errs =>
              val parseErr = errs find {
                case SchemaError.FQLError(_: ParseError) => true
                case _                                   => false
              }

              if (parseErr.isEmpty) {
                SchemaError.SchemaSourceErrors(errs, Seq(file)) :: Nil
              } else {
                // Note that the single item endpoint parses and validates the
                // submited FSL before calling this method, therefore, if the
                // proposed file is invalid, it can only mean that the FSL previously
                // stored in-disk was invalid. This prevents the single item endpoint
                // from emiting spans outside of the proposed FSL while redirecting
                // users to the CLI in order to fix the broken files.
                SchemaError.InvalidStoredSchema :: Nil
              }
            }
            .map { updates =>
              updates find { update =>
                update.item.kind == kind && update.item.name.str == finalName
              }
            }
        }
    }

    def validateSingleFSLItem(
      req: SchemaRequest.Authenticated,
      file: SourceFile.FSL,
      renames: SchemaDiff.Renames = Map.empty
    ): Query[Result[ValidateResult]] = {
      SchemaManager
        .validate(req.ctx.scopeID, Seq(file), renames, overrideMode = false)
        .map { res =>
          res.mapErr { errs =>
            val parseErr = errs find {
              case SchemaError.FQLError(_: ParseError) => true
              case _                                   => false
            }

            if (parseErr.isEmpty) {
              SchemaError.SchemaSourceErrors(errs, Seq(file)) :: Nil
            } else {
              // Note that the single item endpoint parses and validates the
              // submited FSL before calling this method, therefore, if the
              // proposed file is invalid, it can only mean that the FSL previously
              // stored in-disk was invalid. This prevents the single item endpoint
              // from emiting spans outside of the proposed FSL while redirecting
              // users to the CLI in order to fix the broken files.
              SchemaError.InvalidStoredSchema :: Nil
            }
          }
        }
    }
  }

  private[this] val NoInfo = Right(RequestInfo.Null)

  def getRequestInfo(app: CoreAppContext, httpReq: HttpRequest) = NoInfo

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: RequestInfo,
    body: Future[Option[ByteBuf]])(implicit ec: ExecutionContext) = {

    val reqF = SchemaRequest.parse(httpReq, body)
    reqF
  }

  def exec(
    app: CoreAppContext,
    info: RequestInfo,
    req: Request,
    auth: Option[Auth]) = {

    type Render = (Option[JSValue], ResponseInfo => Query[Response])

    def res(sr: AnySchemaResponse): Query[Render] =
      Query.value((None, r => Query.value(sr.toResponse(r.txnTime))))

    auth match {
      case None => res(ErrorResponse.Unauthorized)
      case Some(auth) =>
        auth.checkReadSchemaPermission(auth.scopeID) flatMap {
          case false => res(ErrorResponse.Forbidden)
          case true =>
            endpoints(req.method, req.path.getOrElse(routePrefix)) match {
              case NotFound            => res(ErrorResponse.NotFound)
              case MethodNotAllowed(_) => res(ErrorResponse.MethodNotAllowed)
              case Handler(args, handler) =>
                val intrp = new FQLInterpreter(auth)
                handler(args, req.toAuthenticated(intrp))
                  .flatMapT { response =>
                    Result.adaptT(intrp.runPostEvalHooks()) mapT { _ =>
                      response
                    }
                  }
                  .flatMap {
                    case Ok(response) => res(response)
                    case Err(errs)    =>
                      // Abort query and discard writes.
                      Query.fail(new SchemaErrorsException(errs))
                  }
            }
        }
    }
  }

  def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable) =
    ex match {
      case se: SchemaErrorsException =>
        // ErrorResponse doesn't use the txn time.
        Query.value(ErrorResponse(se.errors).toResponse(Timestamp.Min))
      case other =>
        ExceptionResponseHelpers.toResponse(other, app.stats) { case (code, msg) =>
          Query.value(ErrorResponse(code, msg).toResponse(Timestamp.Min))
        }
    }
}
