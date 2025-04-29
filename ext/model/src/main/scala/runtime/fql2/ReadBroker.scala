package fauna.model.runtime.fql2

import fauna.atoms.{ CollectionID, IndexID, ScopeID, SubID }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Cache, Collection, Index, RuntimeEnv, SchemaNames }
import fauna.model.runtime.fql2.stdlib.CollectionDefCompanion
import fauna.model.runtime.Effect
import fauna.model.schema.{
  CollectionConfig,
  ComputedField,
  FieldPath,
  NamedCollectionID
}
import fauna.repo.doc.Version
import fauna.repo.query.{ Query, ReadCache }
import fauna.repo.schema.{ CollectionSchema, FieldSchema }
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.BiTimestamp
import fql.ast.{ Name, Span }
import fql.ast.display._
import fql.ast.Literal
import scala.collection.immutable.{ ArraySeq, SeqMap }

sealed trait AnyDocRef {
  def toValue: Value.Struct.Full

  /** Converts this ref to the `<coll>(<id>)` string format.
    */
  def toRefString: String
}

final case class DocRef(id: SubID, coll: Value.SingletonObject) extends AnyDocRef {
  def toValue =
    Value.Struct(
      SeqMap(
        "id" -> Value.ID(id.toLong),
        "coll" -> coll
      ))

  // NOTE: This is missing the collection's parent.
  def toRefString: String = s"${coll.name}(${id.toLong})"
}

final case class SchemaRef(name: String, coll: Value.SingletonObject)
    extends AnyDocRef {
  def toValue =
    Value.Struct(
      SeqMap(
        "name" -> Value.Str(name),
        "coll" -> coll
      ))

  // NOTE: This is missing the collection's parent.
  def toRefString: String = s"${coll.name}.byName(${Literal.Str(name).display})"
}

final case class LegacyRef(name: String, kind: String) extends AnyDocRef {
  // This will show up in the simple format, or if you manage to delete
  // a legacy ref in FQLX.
  def toValue =
    Value.Struct(
      SeqMap(
        "name" -> Value.Str(name),
        "coll" -> Value.Str(s"legacy $kind")
      ))

  def toRefString: String = s"[legacy $kind $name]"
}

/** Manages field lookups for documents. */
object ReadBroker {
  import FieldTable.R
  import ReadCache.CachedDoc.SrcHints

  private sealed trait DocData
  private object DocData {

    final case class Full(version: Version.Live, srcHints: SrcHints = SrcHints.Empty)
        extends DocData

    final case class Partial(
      prefix: ReadCache.Prefix,
      partial: ReadCache.CachedDoc.Partial)
        extends DocData
  }

  private def getBroker(
    env: RuntimeEnv,
    scope: ScopeID,
    id: CollectionID): Query[Option[ReadBroker]] =
    env.getCollection(scope, id).mapT(new ReadBroker(_))

  private def getBroker(
    ctx: FQLInterpCtx,
    id: CollectionID): Query[Option[ReadBroker]] =
    getBroker(ctx.env, ctx.scopeID, id)

  private def withBroker[A](ctx: FQLInterpCtx, doc: Value.Doc)(
    fn: ReadBroker => Query[R[A]]) =
    getBroker(ctx, doc.id.collID).flatMap {
      case Some(rb) => fn(rb)
      case None =>
        R.Null(
          Value.Null.Cause.CollectionDeleted(doc, ctx.stackTrace.currentStackFrame))
          .toQuery
    }

  /** Does `doc` exist? All errors count as not existing. */
  def exists(ctx: FQLInterpCtx, doc: Value.Doc): Query[Boolean] =
    guardFromNull(ctx, doc, Effect.Action.Function("exists")) map { _.isVal }

  /** Evaluate if `doc` is null. */
  def guardFromNull(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    action: Effect.Action
  ): Query[R[Unit]] =
    action.check(ctx, Effect.Read).flatMap {
      case Result.Ok(_) =>
        withBroker(ctx, doc) {
          _.getDocData(ctx, doc, action).map { doc =>
            doc.nullReceiver.fold(R.Val(()): R[Unit])(R.Null(_))
          }
        }
      case Result.Err(err) => R.Error(err).toQuery
    }

  def get(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    action: Effect.Action): Query[R[Version.Live]] =
    withBroker(ctx, doc) { _.getLiveVersion(ctx, doc, action) }

  def getVersion(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    action: Effect.Action): Query[R[Version]] =
    withBroker(ctx, doc) {
      _.checkedGet(ctx, doc, action)(
        ctx.env
          .Store(ctx.scopeID)
          .getVersion(doc.id, doc.readTS.getOrElse(ctx.readValidTime)),
        identity
      )
    }

  def hasField(ctx: FQLInterpCtx, doc: Value.Doc, name: Name): Query[Boolean] =
    getBroker(ctx, doc.id.collID).flatMap {
      case Some(rb) => rb.hasField(ctx, doc, name)
      case None     => Query.False
    }

  def getField(ctx: FQLInterpCtx, doc: Value.Doc, name: Name): Query[R[Value]] = {
    getBroker(ctx, doc.id.collID).flatMap {
      case Some(rb) => rb.getField(ctx, doc, name)
      case None     =>
        // FIXME: this is a bit gross. We need to recreate the logic of
        // handing ref fields here, hence duplicate handling of "id" and "coll".
        // We also assume that a deleted collection is a user-defined one, which
        // can only have "id", not "name".
        val res = name.str match {
          case "id"   => R.Val(Value.ID(doc.id.subID.toLong))
          case "coll" => R.Val(CollectionDefCompanion.Deleted)
          case _      => R.Null(Value.Null.Cause.CollectionDeleted(doc, name.span))
        }
        res.toQuery
    }
  }

  def getAllFields(
    ctx: FQLInterpCtx,
    doc: Value.Doc
  ): Query[Result[Value]] =
    getBroker(ctx, doc.id.collID).flatMap {
      case Some(rb) => rb.getAllFields(ctx, doc)
      case None     => Value.Null.collectionDeleted(doc, Span.Null).toQuery
    }

  // This is a bit tortured, for now.
  def getFieldLocatorFn(
    ctx: FQLInterpCtx,
    collID: CollectionID): Query[Option[String => List[String]]] =
    getBroker(ctx, collID) mapT { rb => field => rb.fieldLocation(field) }

  def materializeRef(ctx: FQLInterpCtx, doc: Value.Doc): Query[AnyDocRef] =
    materializeRef(ctx.env, ctx.scopeID, doc)

  /** This does _not_ require an interpreter. This method is only used externally
    * to render refs for contention errors.
    */
  @deprecated("Do not use without reasoning through schema lookup.", "")
  def materializeRef(scope: ScopeID, doc: Value.Doc): Query[AnyDocRef] =
    materializeRef(RuntimeEnv.Default, scope, doc)

  private def materializeRef(
    env: RuntimeEnv,
    scope: ScopeID,
    doc: Value.Doc): Query[AnyDocRef] =
    getBroker(env, scope, doc.id.collID).flatMap {
      case Some(rb) => rb.materializeRef(doc)
      case None => Query.value(DocRef(doc.id.subID, CollectionDefCompanion.Deleted))
    }

  def materializeStruct(
    ctx: FQLInterpCtx,
    struct: Value.Struct): Query[Value.Struct.Full] =
    materializeValue(ctx, struct).asInstanceOf[Query[Value.Struct.Full]]

  /** Materializes all structs within the given value, and populates all named docs. */
  def materializeValue(ctx: FQLInterpCtx, value: Value): Query[Value] =
    value match {
      case doc: Value.Doc =>
        ReadBroker.populateDoc(ctx, doc) flatMap {
          case R.Val(doc)    => Query.value(doc)
          case R.Null(cause) => Query.value(Value.Null(cause))
          case R.Error(e) =>
            Query.fail(
              new IllegalStateException(s"ReadBroker.populateDoc error: $e"))
        }

      case value: Value if value.isPersistable =>
        Query.value(value)

      case Value.Array(elems) =>
        val mat = elems map { elem => materializeValue(ctx, elem) }
        mat.sequence map { Value.Array.fromSpecific(_) }

      case Value.Struct.Full(fields, _, _, _) =>
        val mat = fields.values map { v => materializeValue(ctx, v) }
        mat.sequence map { vs => Value.Struct.fromSpecific(fields.keys.zip(vs)) }

      case partial: Value.Struct.Partial =>
        getBroker(ctx, partial.doc.id.collID) flatMap {
          case Some(rb) =>
            /** Because we are fully materializing the partial here, we know we should emit the hint that lead
              * to its materialization and don't need to check the paths.
              */
            rb.maybeEmitHint(
              ctx,
              partial.path,
              partial.srcHints,
              partial.accessSpan,
              checkPaths = false)
              .flatMap { _ =>
                rb.materializePartial(partial, ctx.allowFieldComputation)
              }
          case None =>
            Query.value(
              Value.Null.collectionDeleted(
                Value.Doc(partial.doc.id),
                ctx.stackTrace.currentStackFrame))
        }

      case _ => Query.value(value)
    }

  /** This does _not_ require an interpreter. This method is only used externally
    * to convert a v10 value into a v4 value when calling a v4 UDF.
    */
  @deprecated("Do not use without reasoning through schema lookup.", "")
  def materializePartial(scope: ScopeID, partial: Value.Struct.Partial) =
    getBroker(RuntimeEnv.Default, scope, partial.doc.id.collID).flatMap {
      case Some(rb) =>
        rb.materializePartial(partial, allowTSField = true)
      case None =>
        Query.fail(
          new IllegalStateException(
            s"No collection found for the given partial: $partial"))
    }

  // Extracts the value at `path` from `value`. This is used to extract index
  // terms and values from v10 fixed fields and computed fields. Generally, this
  // is the same process, but computed fields can't access `ts` (regular fields
  // shouldn't be able to but they have been for a long time).
  def extractValue(
    ctx: FQLInterpCtx,
    computed: Boolean,
    value: Value,
    path: FieldPath): Query[Option[Value]] = {
    (computed, path, value) match {
      case (true, Nil, Value.TransactionTime) =>
        // Indexing the transaction time with a computed field is disallowed,
        // and it should produce no terms / values.
        Query.none
      case (_, Nil, v) => Query.value(Some(v))
      case (_, Left(idx) :: path, Value.Array(es)) =>
        es.lift(idx.toInt) match {
          case Some(v) => extractValue(ctx, computed, v, path)
          case _       => Query.none
        }
      case (_, Right(field) :: path, Value.Struct.Full(fs, _, _, _)) =>
        fs.get(field) match {
          case Some(v) => extractValue(ctx, computed, v, path)
          case _       => Query.none
        }
      case (true, Right("ts") :: _, _: Value.Doc) =>
        // Using the ts field in an indexed computed field is disallowed, and
        // it should produce no terms / values.
        Query.none
      case (_, Right(field) :: path, d: Value.Doc) =>
        // Getting ref fields is OK. Anything else is a read and should return
        // none or null, so we simply try a ref field get.
        getBroker(ctx, d.id.collID) flatMapT {
          _.extractRefField(d, Name(field, Span.Null)) flatMap {
            case _: Value.Null => Query.none
            case v             => extractValue(ctx, computed, v, path)
          }
        }
      case _ => Query.none
    }
  }

  def extractField(ctx: FQLInterpCtx, version: Version, field: Name) =
    getBroker(ctx, version.id.collID) flatMap {
      case Some(rb) =>
        rb.extractField(version, field)
      case None =>
        R.Null(Value.Null.Cause.CollectionDeleted(Value.Doc(version.id), field.span))
          .toQuery
    }

  // This function is guaranteed to return R.Val or R.Null, never R.Error.
  def populateDoc(ctx: FQLInterpCtx, doc: Value.Doc): Query[R[Value.Doc]] =
    withBroker(ctx, doc) {
      _.id match {
        case IndexID.collID => R.Val(doc).toQuery
        case NamedCollectionID(_) =>
          SchemaNames
            .lookupCachedName(ctx.scopeID, doc.id)
            .map { name =>
              // FIXME: `name` is `None` when FQL4 creates a collection,
              // which doesn't cache the name.
              R.Val(
                doc.copy(name =
                  name.orElse(doc.name).orElse(Some("[new collection]"))))
            }
        case _ => R.Val(doc).toQuery
      }
    }

  /** Used to emit a hint for a given path access if performance hints are enabled and none of the SrcHints provided
    * could cover the field access.
    */
  def maybeEmitHint[T](
    ctx: FQLInterpCtx,
    path: ReadCache.Path,
    srcHints: SrcHints,
    span: Span,
    checkPaths: Boolean = true
  ): Query[Unit] = {
    if (!ctx.performanceDiagnosticsEnabled) {
      Query.unit
    } else {
      srcHints.indexSrcHints.headOption
        .map { _.collectionId }
        .map { collID =>
          getBroker(ctx, collID).flatMap {
            case Some(rb) => rb.maybeEmitHint(ctx, path, srcHints, span, checkPaths)
            case None     => Query.unit
          }
        }
        .getOrElse(Query.unit)
    }
  }

}

final class ReadBroker(val config: CollectionConfig) extends AnyVal {
  import FieldTable.R
  import ReadBroker._
  import ReadCache.CachedDoc.SrcHints
  import Result._

  def id = config.id
  def scope = config.parentScopeID
  def schema = config.Schema

  // These fields must be consistent with MaterializedValue's serialization
  def RefFields = id match {
    case NamedCollectionID(_) => ArraySeq("name", "coll")
    case _                    => ArraySeq("id", "coll")
  }
  def MetaFields = RefFields ++ CollectionSchema.MetaFields

  /** These represent our fields that we display to the user when a document is returned from a query.
    * We render these fields first so as to not have them mixed in with all of the user fields.
    */
  def DisplayedMetaFields = RefFields ++ CollectionSchema.DisplayedMetaFields

  private def logPartialConsistencyFailure(doc: Value.Doc, srcHints: SrcHints) =
    Collection.deriveMinValidTime(config.parentScopeID, config.id).map { mvt =>
      getLogger.warn(
        s"Index consistency failure: ${doc.id} not found @ ts=${doc.readTS} mvt=$mvt $srcHints")
      ()
    }

  // FIXME: push meta field h to StructSchema.
  private def schemaFieldExistence(field: Name): Option[Boolean] =
    if (MetaFields.contains(field.str)) {
      Some(true)
    } else {
      schema.struct.fields.get(field.str) match {
        case Some(f) if !f.optional                  => Some(true)
        case Some(_)                                 => None
        case None if schema.struct.wildcard.nonEmpty => None
        case None                                    => Some(false)
      }
    }

  def companionObject = config.companionObject

  def hasField(ctx: FQLInterpCtx, doc: Value.Doc, field: Name): Query[Boolean] =
    schemaFieldExistence(field) match {
      case Some(exists) => Query.value(exists)
      case None         => getField(ctx, doc, field) map { !_.isNull }
    }

  /** Used to emit a hint for a given path access if performance hints are enabled and none of the SrcHints provided
    * could cover the field access.
    *
    * There are times when we don't want to check the paths, the main scenario here is when a partial is returned from
    * the query and we know we have to fully materialize the partial. In that case we know we are done accessing and
    * use the checkPaths flag to ensure we emit the hint.
    */
  def maybeEmitHint[T](
    ctx: FQLInterpCtx,
    path: ReadCache.Path,
    srcHints: SrcHints,
    span: Span,
    checkPaths: Boolean = true
  ): Query[Unit] = {

    /** If indexSrcHints are empty it means the doc was sourced from a document read
      * which means that all fields are covered.
      */
    if (!ctx.performanceDiagnosticsEnabled || srcHints.indexSrcHints.isEmpty) {
      Query.unit
    } else {
      (checkPaths, isPathPossiblyCoveredInSrcs(path, srcHints)) match {
        case (false, (_, displayPath)) =>
          ctx.emitDiagnostic(
            Hints.NonCoveredIndexRead(
              srcHints,
              displayPath,
              span
            )
          )
        case (true, (false, displayPath)) =>
          ctx.emitDiagnostic(
            Hints.NonCoveredIndexRead(
              srcHints,
              displayPath,
              span
            )
          )
        case (_, (true, _)) => Query.unit
      }
    }
  }

  def maybeEmitDocHint(ctx: FQLInterpCtx, doc: Value.Doc): Query[Unit] = {
    doc.srcHint
      .map { srcHint =>
        if (ctx.performanceDiagnosticsEnabled) {
          ctx.emitDiagnostic(
            Hints
              .IndexSetMaterializedDoc(srcHint))
        } else {
          Query.unit
        }
      }
      .getOrElse(Query.unit)
  }

  /** Returns a tuple that is a boolean and a field path.
    * If the boolean is true the provided path is possibly covered by the provided srcs (more on that below).
    * If false the provided path is not covered by the provided srcs.
    * The field path provided as the 2nd part of the tuple is the user friendly display field path. In most cases this
    * means the field path without the prefixed .data field, unless the field is in conflict with our reserved top
    * level fields.
    *
    * There are 2 scenarios we check for here:
    * 1. The covered field path on a src hint is a prefix of the field path
    * In this scenario we know the field path is fully covered
    * 2. The field path is a prefix of a covered field path on a src hint
    * In this scenario, it is still possible that we end up going down a path that is fully covered by the src hint.
    * We return true here as we will find out through future access if we hit a non-covered path and will emit a hint at
    * that point.
    */
  def isPathPossiblyCoveredInSrcs(
    path: List[String],
    srcs: SrcHints): (Boolean, List[String]) = {
    val fieldPath = path match {
      case first :: tail => fieldLocation(first).appendedAll(tail)
      case _             => path
    }
    val isCovered = srcs.indexSrcHints.exists { s =>
      s.coveredFieldPaths.exists {
        case first +: tail =>
          val fullPath: List[ReadCache.Field] =
            fieldLocation(first).appendedAll(tail)
          fullPath.startsWith(fieldPath) || fieldPath.startsWith(fullPath)
        case _ => false
      }
    }
    (isCovered, schema.toDisplayPath(path))
  }

  // Evaluates a computed field, if this broker is allowed to.
  // Sometimes a broker shouldn't be allowed to access computed fields,
  // e.g. when being used to compute an index term or value.
  private def evalComputedField(
    ctx: FQLInterpCtx,
    cmpf: ComputedField,
    doc: Value.Doc): Query[Result[Value]] =
    if (ctx.allowFieldComputation) {
      cmpf.eval(ctx, doc)
    } else {
      Result
        .Err(QueryRuntimeFailure.InvalidComputedFieldAccess(ctx.stackTrace))
        .toQuery
    }

  def getField(ctx: FQLInterpCtx, doc: Value.Doc, field: Name): Query[R[Value]] =
    if (RefFields.contains(field.str)) {
      extractRefField(doc, field).map(R.Val(_))
    } else {
      getDocData(ctx, doc, Effect.Action.Field) flatMap {
        case R.Val(data) =>
          config.computedFields.get(field.str) match {
            case Some(cmpf) =>
              extractComputedField(ctx, doc, data, cmpf, field)
            case None =>
              extractDocDataField(ctx, doc, data, field)
          }
        case other @ R.Null(_)  => other.toQuery
        case other @ R.Error(_) => other.toQuery
      }
    }

  def getAllFields(
    ctx: FQLInterpCtx,
    doc: Value.Doc
  ): Query[Result[Value]] =
    getLiveVersion(ctx, doc, Effect.Action.AllFields) flatMap {
      case R.Val(vers) =>
        maybeEmitDocHint(ctx, doc)
          .flatMap { _ =>
            getAllFields(ctx, vers, doc.readTS)
          }
      case R.Null(nc)   => Value.Null(nc).toQuery
      case R.Error(err) => err.toQuery
    }

  def getAllFields(
    ctx: FQLInterpCtx,
    version: Version,
    validTS: Option[Timestamp],
    evalComputedFields: Boolean = true): Query[Result[Value.Struct.Full]] = {

    val doc = Value.Doc(version.id, readTS = validTS)

    def fieldIsNotInternal(field: String): Boolean =
      schema.struct.fields.get(field) forall { !_.internal }

    def getField0(field: String) =
      extractVersionField(
        doc,
        version,
        Name(field, Span.Null),
        ctx.allowFieldComputation) map {
        case R.Val(Value.Null(_)) => None
        case R.Val(value)         => Some(field -> value)
        case _                    => None
      }

    def getFields0(fields: Iterable[String]) =
      fields.map { getField0(_) }.sequence map { _.flatten }

    val metadataQ = getFields0(DisplayedMetaFields)
    val structFields0 = schema.struct.fields.keys
    val structFields = if (schema.isUserCollection) {
      structFields0.filter { _ != CollectionSchema.DataField }
    } else {
      structFields0
    }
    val versionFieldsQ = getFields0(structFields)
    val computedFieldsQ = if (evalComputedFields) {
      config.computedFields.view
        .map { case (field, q) =>
          evalComputedField(ctx, q, doc) mapT { field -> _ }
        }
        .toSeq
        .sequenceT
    } else {
      Query.value(Ok(List.empty))
    }

    (metadataQ, versionFieldsQ, computedFieldsQ) par {
      case (_, _, err @ Err(_)) => err.toQuery
      case (metaFields, versionFields, Ok(computedFields)) =>
        val wildcardFields =
          if (schema.struct.wildcard.isEmpty) {
            Seq.empty
          } else {
            val seen =
              metaFields.view
                .map { _._1 }
                .concat(versionFields.view map { _._1 })
                .concat(computedFields.view map { _._1 })
                .toSet

            schema.toExternalData(version.data).fields.elems collect {
              case (field, value) if !seen(field) && fieldIsNotInternal(field) =>
                field -> Value.fromIR(value, Span.Null, validTS)
            }
          }

        Value.Struct
          .fromSpecific(
            metaFields.view
              .concat(versionFields)
              .concat(computedFields)
              .concat(wildcardFields)
          )
          .toQuery
    }
  }

  private def lookupLegacyIndex(doc: Value.Doc): Query[LegacyRef] =
    Index.get(scope, IndexID(doc.id.subID.toLong)).map {
      case Some(index) => LegacyRef(index.name, "index")
      case None        => LegacyRef("<deleted>", "index")
    }

  def materializeRef(doc: Value.Doc): Query[AnyDocRef] =
    id match {
      case IndexID.collID => lookupLegacyIndex(doc)
      case NamedCollectionID(_) =>
        SchemaNames
          .lookupCachedName(scope, doc.id)
          .map { name =>
            // FIXME: `name` is `None` when FQL4 creates a collection,
            // which doesn't cache the name.
            SchemaRef(
              name.getOrElse(doc.name.getOrElse("[new collection]")),
              companionObject)
          }
      case _ =>
        Query.value(DocRef(doc.id.subID, companionObject))
    }

  def materializePartial(
    partial: Value.Struct.Partial,
    allowTSField: Boolean): Query[Value.Struct.Full] =
    withDocReadTracking {
      Store.rewindAndGet(
        partial.prefix,
        schema,
        partial.doc.id,
        partial.doc.readTS
      )
    } flatMap {
      case None =>
        logPartialConsistencyFailure(partial.doc, partial.srcHints).flatMap { _ =>
          // FIXME: Index mismatch! Should we return Value.Null?
          Query.fail(
            new IllegalStateException(
              s"No document found for the given partial: $partial"))
        }

      case Some(version) =>
        extractVersionField(
          partial.doc,
          version,
          Name(partial.path.head, Span.Null),
          allowTSField
        ) map {
          case R.Val(value: Value.Struct.Full) =>
            partial.path.tail.foldLeft(value) { case (struct, field) =>
              struct.fields.get(field) match {
                case Some(next: Value.Struct.Full) => next
                case Some(other) =>
                  throw new IllegalStateException(
                    s"Partial value did not materialize to a struct type: $partial. " +
                      s"Materialized value at field '$field': $other.")
                case None =>
                  throw new IllegalStateException(
                    s"Partial value did not materialize to a struct type: $partial. " +
                      s"Missing field '$field'.")
              }
            }
          case other =>
            throw new IllegalStateException(
              s"Partial value did not materialize to a struct type: $partial. " +
                s"Materialized value at field '${partial.path.head}': $other.")
        }
    }

  def extractRefField(doc: Value.Doc, field: Name): Query[Value] =
    id match {
      case IndexID.collID =>
        Query.value(Value.Null.missingField(doc, field))
      case _ =>
        field.str match {
          case "id"   => Query.value(Value.ID(doc.id.subID.toLong))
          case "coll" => Query.value(companionObject)
          case "name" =>
            doc.versionOverride
              .map(vers => Query.value(Value.Str(SchemaNames.findName(vers))))
              .getOrElse(
                // FIXME: Use staged schema for this reverse lookup.
                SchemaNames
                  .lookupCachedName(scope, doc.id)
                  .map {
                    case Some(name) => Value.Str(name)
                    case None =>
                      doc.name match {
                        case Some(name) => Value.Str(name)
                        // only shows up when you have a null ref from storage.
                        case None => Value.Null.missingField(doc, field)
                      }
                  })
          case _ => Query.value(Value.Null.missingField(doc, field))
        }
    }

  // Extracts a field for use as a (fixed) index term from a version.
  private def extractField(version: Version, field: Name): Query[R[Value]] =
    extractVersionField(Value.Doc(version.id), version, field)

  private def extractVersionField(
    doc: Value.Doc,
    version: Version,
    field: Name,
    allowTSField: Boolean = true): Query[R[Value]] = {
    def castTimestamp(ts: BiTimestamp): Value =
      ts.validTSOpt match {
        case Some(ts) => Value.Time(ts)
        case None     => Value.TransactionTime
      }

    if (RefFields.contains(field.str)) {
      extractRefField(doc, field) map { R.Val(_) }
    } else {
      field.str match {
        case CollectionSchema.TSField =>
          if (allowTSField) {
            Query.value(R.Val(castTimestamp(version.ts)))
          } else {
            Query.value(
              R.Error(QueryRuntimeFailure.InvalidTimestampFieldAccess(
                FQLInterpreter.StackTrace(Nil))))
          }
        case _ =>
          extractFieldPath(doc, field) { path =>
            Query.value {
              version.data.fields.get(path) map {
                Value.fromIR(_, field.span, doc.readTS)
              }
            }
          }
      }
    }
  }

  // Extracts a computed field value, either by evaluating it or retrieving it
  // from a partial in the read cache (because it's covered by an index read).
  private def extractComputedField(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    data: DocData,
    cmpf: ComputedField,
    field: Name): Query[R[Value]] = data match {
    case DocData.Partial(prefix, partial) =>
      // Cf. CollectionIndexMethod.collectPartialPaths.
      val path = List(field.str)
      partial.project(path) match {
        // We cannot trust cached nulls because CF evaluation failure can
        // result in null values in the index.
        case Some(v: ReadCache.Fragment.Value) if !v.unwrap.isNull =>
          R.Val(v.unwrap).toQuery
        case Some(s: ReadCache.Fragment.Struct) =>
          R.Val(Value.Struct.Partial(doc, prefix, path, s, field.span)).toQuery
        case _ =>
          evalComputedField(ctx, cmpf, doc) map {
            case Ok(value) => R.Val(value)
            case Err(qf)   => R.Error(qf)
          }
      }
    case _ =>
      evalComputedField(ctx, cmpf, doc) map {
        case Ok(value) => R.Val(value)
        case Err(qf)   => R.Error(qf)
      }
  }

  private def extractDocDataField(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    data: DocData,
    field: Name): Query[R[Value]] = {

    data match {
      case dd @ DocData.Full(version, _) =>
        val fieldQ =
          extractVersionField(doc, version, field, ctx.allowFieldComputation)
            .flatMap {
              case R.Val(value: Value.Struct.Full)
                  if ctx.performanceDiagnosticsEnabled =>
                extractFieldPath(doc, field) { path =>
                  Query.value(
                    Some(
                      value.copy(
                        path = path,
                        srcHints = dd.srcHints,
                        accessSpan = field.span)))
                }
              case v =>
                Query.value(v)
            }

        maybeEmitHint(ctx, List(field.str), dd.srcHints, field.span).flatMap { _ =>
          fieldQ
        }

      case DocData.Partial(prefix, partial) =>
        extractFieldPath(doc, field) { path =>
          partial.project(path) match {
            case Some(v: ReadCache.Fragment.Value) =>
              Query.some(v.unwrap)

            case Some(s: ReadCache.Fragment.Struct) =>
              val struct = Value.Struct.Partial(doc, prefix, path, s, field.span)
              struct.srcHints = partial.srcHints
              Query.some(struct)

            case None =>
              val readQ = withDocReadTracking {
                Store.get(schema, doc.id, doc.readTS.getOrElse(ctx.readValidTime))
              } flatMap {
                case None =>
                  logPartialConsistencyFailure(doc, partial.srcHints)
                    .map(_ => None)
                case Some(vers) =>
                  extractVersionField(
                    doc,
                    vers,
                    field,
                    ctx.allowFieldComputation) map {
                    case R.Val(v) => Some(v)
                    case _        => None
                  }
              }

              maybeEmitHint(ctx, path, partial.srcHints, field.span).flatMap { _ =>
                readQ
              }
          }
        }
    }
  }

  private def extractFieldPath[A](doc: Value.Doc, field: Name)(
    extract: List[String] => Query[Option[Value]]): Query[R[Value]] = {

    val structField = schema.struct.field(field.str)
    if (structField.exists { _.internal }) {
      Query.value(R.Val(Value.Null.missingField(doc, field)))
    } else {
      extract(fieldLocation(field.str, structField)) flatMapT { value =>
        structField.fold(Query.some(value)) {
          _.readField(value).map { Some(_) }
        }
      } getOrElseT {
        Value.Null.missingField(doc, field)
      } map { R.Val(_) }
    }
  }

  def fieldLocation(field: String): List[String] =
    fieldLocation(field, schema.struct.field(field))

  private def fieldLocation(
    field: String,
    structField: Option[FieldSchema]): List[String] = {

    val finalName =
      structField.flatMap { _.aliasTo }.getOrElse(field)

    if (schema.isUserCollection && !MetaFields.contains(field)) {
      CollectionSchema.DataField :: finalName :: Nil
    } else {
      finalName :: Nil
    }
  }

  private def getLiveVersion(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    action: Effect.Action
  ): Query[R[Version.Live]] =
    checkedGet(ctx, doc, action)(
      ctx.env
        .Store(ctx.scopeID)
        .get(doc.id, doc.readTS.getOrElse(ctx.readValidTime)),
      identity
    )

  private def getDocData(
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    action: Effect.Action
  ): Query[R[DocData]] = {
    import ReadCache.{ CachedDoc => CD }

    def getDocData0: Query[Option[DocData]] =
      Store.writePrefix(scope, doc.id) flatMap { prefix =>
        val validTS = doc.readTS.getOrElse(ctx.readValidTime)

        Store.peekCache(prefix, scope, doc.id, Some(validTS)) flatMapT {
          case v: CD.Version =>
            Query.some(DocData.Full(v.decode(schema.migrations), v.srcHints))
          case p: CD.Partial     => Query.some(DocData.Partial(prefix, p))
          case _: CD.DocNotFound => Query.none
        } orElseT {
          Store.get(schema, doc.id, validTS) mapT { DocData.Full(_) }
        }
      }

    checkedGet(ctx, doc, action)(getDocData0, DocData.Full(_))
  }

  private def checkedGet[A](
    ctx: FQLInterpCtx,
    doc: Value.Doc,
    action: Effect.Action)(
    get: => Query[Option[A]],
    map: Version.Live => A): Query[R[A]] = {
    import Value.Null.Cause._

    def checkedGet0: Query[R[A]] = {
      // FIXME: cache perm lookup.
      val checkQ =
        doc.readTS match {
          case Some(_) => ctx.auth.checkHistoryReadPermission(scope, doc.id)
          case None =>
            ctx.auth.checkReadPermission(scope, doc.id, ctx.systemValidTime)
        }

      def currStack =
        ctx.stackTrace.currentStackFrame

      val resQ =
        (checkQ, get) par {
          case (false, _) => R.Null(ReadPermissionDenied(doc.id, currStack)).toQuery
          case (_, None)  => R.Null(DocNotFound(doc, schema.name, currStack)).toQuery
          case (_, Some(data)) => R.Val(data).toQuery
        }

      // NOTE: There's an edge case when named collections are deleted and recreated
      // in the same transaction: a stale id-by-name cache entry might return the ID
      // of the deleted collection. Therefore, we guard against staleness on null
      // returns for named collections here.
      Cache.guardFromStalenessIf(scope, resQ) { res =>
        doc.id.collID match {
          case NamedCollectionID(_) => !res.isVal
          case _                    => false
        }
      }
    }

    // If there's an override set, we allow reading the document. This lets computed
    // fields work, as indexed ones cannot read, but they need to access their
    // document.
    doc.versionOverride match {
      case Some(version) =>
        version match {
          case l: Version.Live => R.Val(map(l)).toQuery
          case _: Version.Deleted =>
            R.Null(DocDeleted(doc, schema.name, ctx.stackTrace.currentStackFrame))
              .toQuery
        }
      case None =>
        action.check(ctx, Effect.Read).flatMap {
          case Result.Ok(_)  => withDocReadTracking(checkedGet0)
          case Result.Err(e) => R.Error(e).toQuery
        }
    }
  }

  @inline private def withDocReadTracking[A](get0: => Query[A]): Query[A] =
    Query.withBytesReadDelta(get0) flatMap { case (res, delta) =>
      // NB. Even if the doc doesn't exist, it still costs the row key byte size.
      delta.fold(Query.unit)(Query.incrDocuments(_)) map { _ => res }
    }

}
