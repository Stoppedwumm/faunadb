package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.schema.fsl._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.DataMode
import fauna.repo.Store
import fql.ast._

/** Represents a single source file in storage. Stored source fields are presumed to
  * be valid. Failures in parsing them throw IllegalStateException.
  *
  * Note that schema sources will be auto-generated at first read if none is present
  * at the curent scope.
  */
final case class SchemaSource(
  scope: ScopeID,
  id: SchemaSourceID,
  version: SchemaVersion,
  file: SourceFile.FSL) {

  def filename = file.filename
  def content = file.content
}

// TODO: validate file names can't start or end with '*' before saving
object SchemaSource {
  import Result.{ Err, Ok }
  import SourceFile.FSL

  private[this] lazy val log = getLogger()

  /** Returns the active schema. If there is no pinned version, and the schema is
    * out of date, it will be regenerated.
    */
  def getActive(scope: ScopeID): Query[Seq[SchemaSource]] = for {
    status <- SchemaStatus.forScope(scope)
    ts = status.activeSchemaVersion match {
      case Some(vers) => vers.ts
      case None       => Timestamp.MaxMicros
    }

    srcs <- getAt(scope, ts)
  } yield srcs

  /** Get or initialize the schema source for the given scope if schema definition is
    * enabled. Returns an empty sequence otherwise.
    */
  def getStaged(scope: ScopeID): Query[Seq[SchemaSource]] = {
    getAt(scope, Timestamp.MaxMicros)
  }

  /** Returns the schema at the given timestamp.
    *
    * If the timestamp is `Timestamp.MaxMicros`, the schema is regenerated.
    */
  def getAt(scope: ScopeID, ts: Timestamp): Query[Seq[SchemaSource]] = {
    val srcsQ = SchemaSourceID
      .getAllUserDefined(scope, ts)
      .collectMT { get(scope, _, ts) }
      .flattenT

    if (ts == Timestamp.MaxMicros) {
      srcsQ.flatMap {
        case src if src.nonEmpty => returnOrRegenerate(scope, src)
        case _                   => initSchemaSource(scope)
      }
    } else {
      srcsQ
    }
  }

  /** Gets the updated schema sources for the given scope. This should be called after
    * updating the model definition, to get the most updated view of schema. Prefer
    * `getStaged` otherwise.
    */
  def getStagedUncached(scope: ScopeID): Query[Seq[SchemaSource]] = {
    SchemaSourceID
      .getAllUserDefined(scope)
      .collectMT { get(scope, _) }
      .flattenT
      .flatMap {
        case src if src.nonEmpty => patchAndReplace(scope, src)
        case _                   => initSchemaSource(scope)
      }
  }

  private def initSchemaSource(scope: ScopeID): Query[Seq[SchemaSource]] =
    InternalCollection.SchemaSource(scope) flatMap { config =>
      SourceGenerator.deriveSchema(scope) flatMap { files =>
        files map { file =>
          val writeQ = Store.create(config.Schema, file.toData)
          writeQ map { SchemaSource(_) }
        } sequence
      }
    }

  def patchedSchema(scope: ScopeID): Query[Seq[SourceFile.FSL]] =
    SchemaSourceID
      .getAllUserDefined(scope)
      .collectMT { get(scope, _) }
      .flattenT
      .flatMap {
        case src if src.nonEmpty => SourceGenerator.patchSchema(scope, src)
        case _                   => SourceGenerator.deriveSchema(scope)
      }

  private def returnOrRegenerate(scope: ScopeID, srcs: Seq[SchemaSource]) = {
    val last = srcs.view.collect { _.version }.max

    val currQ =
      Cache.guardFromStalenessIf(scope, Cache.getLastSeenSchema(scope)) { curr =>
        curr.forall { _ < last } // refresh stale schema cache
      }

    currQ flatMap { curr =>
      if (curr.forall { _ <= last }) {
        Query.value(srcs)
      } else {
        patchAndReplace(scope, srcs)
      }
    }
  }

  private def patchAndReplace(scope: ScopeID, srcs: Seq[SchemaSource]) = {
    val srcByName = srcs.view map { src =>
      src.filename -> src
    } toMap

    InternalCollection.SchemaSource(scope) flatMap { config =>
      val schema = config.Schema
      SourceGenerator.patchSchema(scope, srcs) flatMap { files =>
        files map { file =>
          srcByName
            .get(file.filename)
            .map { src =>
              Store.replace(
                schema,
                src.id.toDocID,
                DataMode.Default,
                file.toData
              )
            }
            .getOrElse { Store.create(schema, file.toData) }
            .map { SchemaSource(_) }
        } sequence
      } map { patched =>
        val allSrcs =
          srcByName ++ patched.view.map { src =>
            src.filename -> src
          }
        allSrcs.values.toSeq sortBy { _.id } // preserve stable order
      }
    }
  }

  /** Return existing or create an empty builtin file. */
  def getOrCreate[F <: SourceFile](
    scope: ScopeID,
    builtin: SourceFile.Builtin[F]): Query[SchemaSource] =
    get(scope, builtin.filename) flatMap {
      case Some(src) => Query.value(src)
      case None =>
        InternalCollection.SchemaSource(scope) flatMap { config =>
          val file = SourceFile.empty(builtin)
          val writeQ = Store.create(config.Schema, file.toData)
          writeQ map { SchemaSource(_) }
        }
    }

  /** Return all FSL files for the given scope. */
  def stagedFSLFiles(scope: ScopeID): Query[Seq[FSL]] =
    getStaged(scope) map { srcs => srcs map { _.file } }

  def activeFSLFiles(scope: ScopeID): Query[Seq[FSL]] =
    getActive(scope) map { srcs => srcs map { _.file } }

  /** Locate an FSL item returning its document id, source file, and span. Note that
    * the file returned may contain invalid FSL. The item search fallback to the
    * `FSL.Node` tree which tolerates some forms of invalid FSL files.
    *
    * NB: This always looks at staged schema.
    */
  def locateFSLItem(
    scope: ScopeID,
    kind: SchemaItem.Kind,
    name: String): Query[Result[Option[(DocID, FSL, Span)]]] =
    getSchema(kind).docIDByNameStaged(scope, name) flatMap {
      case None => Query.value(Ok(None))
      case Some(docID) =>
        stagedFSLFiles(scope) map { files =>
          val iter = files.iterator
          var span = Option.empty[Span]
          var file: FSL = null
          var hasErr = false

          while (span.isEmpty && iter.hasNext) {
            file = iter.next()
            file.locateItem(kind, name) match {
              case Right(opt) => span = opt map { _.span }
              case Left(opt) =>
                span = opt map { _.span }
                val errs = file.parse().errOrElse(Nil)
                log.warn(s"Found invalid FSL in $scope: ${errs.mkString(", ")}")
                hasErr = true
            }
          }

          span match {
            case Some(span)     => Ok(Some((docID, file, span)))
            case None if hasErr => Err(SchemaError.InvalidStoredSchema)
            case None           => Ok(None)
          }
        }
    }

  def getDocID(scope: ScopeID, item: SchemaItem): Query[Option[DocID]] =
    getSchema(item.kind).docIDByNameStaged(scope, item.name.str)

  private[schema] def getSchema(kind: SchemaItem.Kind): SchemaCollection[_] =
    kind match {
      case SchemaItem.Kind.Collection     => SchemaCollection.Collection
      case SchemaItem.Kind.Function       => SchemaCollection.UserFunction
      case SchemaItem.Kind.Role           => SchemaCollection.Role
      case SchemaItem.Kind.AccessProvider => SchemaCollection.AccessProvider
    }

  // TODO: create an index for source by filename
  def get(scope: ScopeID, filename: String): Query[Option[SchemaSource]] =
    getStaged(scope) map { _.find { _.filename == filename } }

  def getActive(scope: ScopeID, filename: String): Query[Option[SchemaSource]] =
    getActive(scope) map { _.find { _.filename == filename } }

  def get(
    scope: ScopeID,
    id: SchemaSourceID,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[SchemaSource]] =
    InternalCollection.SchemaSource(scope).get(id, ts) mapT { SchemaSource(_) }

  def apply(live: Version.Live): SchemaSource =
    SchemaSource(
      live.parentScopeID,
      live.id.as[SchemaSourceID],
      SchemaVersion(live.ts.validTS),
      SourceFile(live.data)
    )

}
