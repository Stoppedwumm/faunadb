package fauna.model.runtime.fql2.stdlib

import fauna.atoms.{ CollectionID, DocID, SubID }
import fauna.model.runtime.fql2._
import fauna.model.schema.index.{ CollectionIndex, UserDefinedIndex }
import fauna.model.Index
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.storage.index.IndexTerm
import fauna.storage.ir.DocIDV
import fql.ast.Span
import scala.collection.immutable.ArraySeq

final case class CollectionIndexMethod(companion: UserCollectionCompanion)
    extends AnyVal {

  import CollectionIndexMethod._

  private[stdlib] def impl(
    ctx: FQLInterpCtx,
    name: String,
    index: UserDefinedIndex,
    params: IndexedSeq[Value],
    terms: IndexedSeq[Value],
    range: Option[Value]
  ) =
    if (index.queryable) {
      valueSet(ctx, name, index, params, terms, range)
    } else {
      QueryRuntimeFailure
        .IndexNotQueryable(companion.coll.name, name, ctx.stackTrace)
        .toQuery
    }

  private def valueSet(
    ctx: FQLInterpCtx,
    indexName: String,
    userIdx: UserDefinedIndex,
    params: IndexedSeq[Value],
    terms: IndexedSeq[Value],
    range: Option[Value]
  ): Query[Result[ValueSet]] =
    Result.guardM {
      Index.get(ctx.scopeID, userIdx.indexID) flatMap {
        case Some(index) if index.isCollectionIndex =>
          val idxTerms = terms.view.zipWithIndex.map { case (v, i) =>
            Value.toIR(v) match {
              case Right(value) => IndexTerm(value)
              case _ =>
                val pname = s"term${i + 1}"
                Result.fail(
                  QueryRuntimeFailure.InvalidArgument(
                    pname,
                    "Argument cannot be used as lookup term",
                    ctx.stackTrace))
            }
          }.toVector

          // This is weird. We'll try to improve it later.
          // TODO: Improve this.
          ReadBroker.getFieldLocatorFn(ctx, companion.coll.id) map {
            case Some(locFn) =>
              collectRange(
                "range",
                companion.coll.id,
                userIdx.values,
                range,
                ctx.stackTrace
              ) map { idxRange =>
                IndexSet(
                  companion,
                  indexName,
                  params,
                  index,
                  idxTerms,
                  ctx.userValidTime,
                  ctx.stackTrace.currentStackFrame,
                  idxRange,
                  collectCoveredValues(userIdx.terms, userIdx.values)(locFn)
                )
              }
            case None =>
              throw new IllegalStateException(
                s"No collection found for user-defined index '$indexName' (${userIdx.indexID})")
          }

        case _ =>
          throw new IllegalStateException(
            s"No collection index ${userIdx.indexID} found for user-defined index '$indexName'.")
      }
    }
}

object CollectionIndexMethod {
  private[stdlib] def collectRange(
    argName: String,
    collID: CollectionID,
    covered: Vector[CollectionIndex.Value],
    range: Option[Value],
    stackTrace: FQLInterpreter.StackTrace
  ): Result[IndexSet.Range] =
    Result.guard {
      def invalidRangeBound(field: String) =
        Result.fail(
          QueryRuntimeFailure.InvalidArgument(
            argName,
            s"The `$field` field should be either an indexable value or an array of values",
            stackTrace
          ))

      def collectElem(
        field: String,
        elem: Value,
        idxValue: CollectionIndex.Value) = {

        // Special-case ID field handling.
        if (idxValue.isIDField) {
          val doc = elem match {
            case d: Value.Doc => d.id
            // Nulls are inclusive
            case _: Value.Null => DocID.MinValue
            // Allow ID-ish values or skip the field.
            case v =>
              TypeTag.ID
                .cast(v)
                .map(id => DocID(SubID(id.value), collID))
                .getOrElse(DocID.MaxValue)
          }
          DocIDV(doc)
        } else {
          Value.toIR(elem) match {
            case Right(ir) => ir
            case _         => invalidRangeBound(field)
          }
        }
      }

      def collectElems(field: String, elems: ArraySeq[Value]) =
        if (elems.size > covered.size) {
          Result.fail(
            QueryRuntimeFailure.InvalidArgument(
              argName,
              s"The field `$field` can have at most ${covered.size} values",
              stackTrace
            ))
        } else {
          val terms = elems.view
            .zip(covered)
            .map { case (value, iv) => collectElem(field, value, iv) }
            .toVector
          IndexSet.Prefix(terms)
        }

      def parsePrefix(field: String, value: Value) =
        // if no covered values, range on docID
        if (covered.size == 0) {
          def invalidDocID() = Result.fail(
            QueryRuntimeFailure.InvalidArgument(
              argName,
              s"The `$field` field must be a document id.",
              stackTrace
            ))

          def fromSub(id: Long) = DocID(SubID(id), collID)

          val docID = value match {
            case Value.Doc(id, _, _, _, _) => id
            case Value.ID(id)              => fromSub(id)
            case Value.Int(id)             => fromSub(id)
            case Value.Long(id)            => fromSub(id)
            case Value.Str(str) =>
              str.toLongOption.map(fromSub).getOrElse(invalidDocID())
            case _ => invalidDocID()
          }
          IndexSet.Prefix(Vector(DocIDV(docID)))
        } else {
          value match {
            case arr @ Value.Array(elems) if covered.size == 1 =>
              // An array which is a prefix of another lexically sorts to the
              // left. However, if an index covers a single field, we still want
              // array "to" arguments to behave as prefixes. Add a null at the
              // end so this prefix becomes inclusive with respect to non-null
              // elements.
              val inclusive = if (field == "to") {
                Value.Array(elems :+ Value.Null(Span.Null))
              } else {
                arr
              }
              collectElems(field, ArraySeq(inclusive))
            case Value.Array(elems) => collectElems(field, elems)
            case v                  => collectElems(field, ArraySeq(v))
          }
        }

      def invalidRangeConfig =
        Result.fail(
          QueryRuntimeFailure.InvalidArgument(
            argName,
            "should be an object contining `{ from }`, `{ to }`, or `{ from, to }` fields",
            stackTrace
          ))

      val res = range match {
        case None => IndexSet.Range.Unbounded
        case Some(config: Value.Struct.Full) =>
          val lhs = config.fields.get("from").map { parsePrefix("from", _) }
          val rhs = config.fields.get("to").map { parsePrefix("to", _) }

          if (lhs.isEmpty && rhs.isEmpty) {
            invalidRangeConfig
          } else {
            IndexSet.Range(lhs, rhs)
          }

        case Some(_) =>
          invalidRangeConfig
      }

      Result.Ok(res)
    }

  /** Returns a sequence of covered values compatible with partial read cache based on
    * the index's definition.
    *
    * Generally, a field's path will contain the data key; however, computed fields
    * are always top-level and never under data. This distinction in path
    * distinguishes cached computed field values from cached defined field values
    * with the same name.
    */
  private def collectCoveredValues(
    terms: Vector[CollectionIndex.Term],
    values: Vector[CollectionIndex.Value])(
    fieldLocation: String => List[String]): Seq[IndexSet.CoveredValue] = {

    def collect0(entry: CollectionIndex.Entry): IndexSet.CoveredValue =
      if (entry.isMVA || entry.isIDField || entry.fieldPath.isEmpty) {
        IndexSet.CoveredValue(entry, partialReadPath = None)
      } else {
        val fieldPath = entry.fieldPath
        val iter = fieldPath.iterator
        val path = List.newBuilder[String]
        var first = true

        while (iter.hasNext) {
          iter.next() match {
            case Left(_) =>
              return IndexSet.CoveredValue(entry, partialReadPath = None)

            case Right(pStr) =>
              if (first && !entry.field.isComputed) {
                path ++= fieldLocation(pStr)
                first = false
              } else {
                path += pStr
              }
          }
        }

        IndexSet.CoveredValue(entry, partialReadPath = Some(path.result()))
      }

    (terms.view ++ values.view).map { collect0(_) }.toSeq
  }
}
