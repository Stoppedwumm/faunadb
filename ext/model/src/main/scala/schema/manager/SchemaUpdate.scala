package fauna.model.schema.manager

import fauna.atoms.ScopeID
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpCtx, ReadBroker, WriteBroker }
import fauna.model.schema._
import fauna.model.schema.fsl.SourceFile
import fauna.repo.query.Query
import fauna.repo.schema.DataMode
import fauna.repo.values.Value
import fauna.repo.Store
import fql.ast._
import fql.schema.{ Diff, SchemaDiff }

trait SchemaUpdate { self: SchemaManager =>
  import SchemaManager.UpdatedItem

  /** Given a list of proposed source files, update stored files and database schema to
    * match it.
    */
  def update(
    ctx: FQLInterpCtx,
    files: Seq[SourceFile.FSL],
    renames: SchemaDiff.Renames = Map.empty,
    overrideMode: Boolean = true
  ): Query[Result[Seq[UpdatedItem]]] = {
    validate(ctx.scopeID, files, renames, overrideMode) flatMapT { res =>
      for {
        srcs         <- SchemaSource.getStaged(ctx.scopeID)
        _            <- updateStoredFiles(ctx.scopeID, srcs, files, overrideMode)
        updatedItems <- updateDBSchema(ctx, res.diffs)
      } yield updatedItems
    }
  }

  private def updateStoredFiles(
    scope: ScopeID,
    prev: Seq[SchemaSource],
    files: Seq[SourceFile.FSL],
    overrideMode: Boolean
  ): Query[Unit] = {
    val idByFilename =
      prev.groupMapReduce(_.filename)(_.id.toDocID) { (a, b) =>
        throw new IllegalStateException(
          s"source ids $a and $b have the same filename"
        ) // should not happen
      }

    val recvFilenames = files.map { _.filename }.toSet

    InternalCollection.SchemaSource(scope) flatMap { config =>
      val coll = config.Schema
      val changes = files.map { file =>
        idByFilename.get(file.filename) match {
          case Some(id) if file.isBlank => Store.internalDelete(coll, id)
          case Some(id) => Store.replace(coll, id, DataMode.Default, file.toData)
          case None     => Store.create(coll, file.toData)
        }
      }

      // Deletes of sources not present in the received files.
      val removes = if (overrideMode) {
        prev.collect {
          case src if !recvFilenames.contains(src.filename) =>
            Store.internalDelete(coll, src.id.toDocID)
        }
      } else {
        Seq.empty
      }

      (changes ++ removes).join
    }
  }

  private def updateDBSchema(
    ctx: FQLInterpCtx,
    diffs: Seq[Diff]
  ): Query[Result[Seq[UpdatedItem]]] = {

    val resQ = diffs map { diff =>
      val docQ = diff match {
        case Diff.Add(it)            => upsert(ctx, it)
        case Diff.Modify(b, a, _, _) => update(ctx, b, a)
        case Diff.Remove(it)         => remove(ctx, it)
      }
      docQ mapT { UpdatedItem(diff.item, _) }
    }

    resQ.sequence map { Result.collectFailures(_) }
  }

  private def upsert(ctx: FQLInterpCtx, item: SchemaItem): Query[Result[Value.Doc]] =
    lookupDocID(ctx, item) flatMap {
      case None =>
        Result.adaptT {
          // typically the write broker is called through a method call from the
          // interpreter
          // when this happens the interpreter adds the span to the stack trace so it
          // is there in the write broker method. I think we need to add it here to
          // be in line with that strategy.
          // unless maybe the spans don't matter here? we likely end up surfacing
          // those when we run into issues
          WriteBroker
            .createDocument(
              ctx.withStackFrame(item.span),
              SchemaSource.getSchema(item.kind).id,
              DataMode.Default,
              toStruct(item, None)
            )
        }
      case Some(doc) =>
        lookupData(ctx, doc) flatMap { userData =>
          Result.adaptT {
            WriteBroker
              .replaceDocument(
                ctx.withStackFrame(item.span),
                doc,
                DataMode.Default,
                toStruct(item, userData)
              )
          }
        }
    }

  private def update(
    ctx: FQLInterpCtx,
    before: SchemaItem,
    after: SchemaItem
  ): Query[Result[Value.Doc]] =
    lookupDocID(ctx, before) flatMap {
      case Some(doc) =>
        lookupData(ctx, doc) flatMap { userData =>
          Result.adaptT {
            WriteBroker
              .replaceDocument(
                ctx.withStackFrame(after.span),
                doc,
                DataMode.Default,
                toStruct(after, userData)
              )
          }
        }
      case None =>
        SchemaError
          .Unexpected(s"Not found `${before.kind}`: ${before.name.str}")
          .toQuery
    }

  private def remove(
    ctx: FQLInterpCtx,
    item: SchemaItem
  ): Query[Result[Value.Doc]] =
    lookupDocID(ctx, item) flatMap {
      case Some(doc) =>
        Result.adaptT(WriteBroker.deleteDocument(ctx.withStackFrame(item.span), doc))
      case None =>
        SchemaError
          .Unexpected(s"Not found `${item.kind}`: ${item.name.str}")
          .toQuery
    }

  private def lookupDocID(
    ctx: FQLInterpCtx,
    item: SchemaItem): Query[Option[Value.Doc]] = {

    SchemaSource.getDocID(ctx.scopeID, item) mapT { id =>
      Value.Doc(id)
    }
  }

  private def lookupData(ctx: FQLInterpCtx, doc: Value.Doc): Query[Option[Value]] =
    ReadBroker.getField(ctx, doc, Name("data", Span.Null)) map { _.liftValue }
}
