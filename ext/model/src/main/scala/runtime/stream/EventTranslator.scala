package fauna.model.runtime.stream

import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model.runtime.fql2.IndexSet
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.service.stream.TxnResult
import fauna.repo.values.Value
import fauna.storage.index.{ IndexTerm, IndexTuple, IndexValue }
import fauna.storage.ops.{ SetAdd, VersionAdd }

object EventTranslator {

  private trait Collector {
    def collectStreamableValue(txn: TxnResult): Seq[StreamValue.Unresolved]
  }

  private object Collector {
    object Docs extends Collector {
      def collectStreamableValue(txn: TxnResult) = {
        var cursor = Value.EventSource.Cursor.MinValue
        txn.writes.view.collect {
          case w: VersionAdd if w.action.isCreate =>
            cursor = cursor.next(txn.txnTS)
            StreamValue.DocVersion(
              Version.Live(
                w.scope,
                w.id,
                w.writeTS,
                w.action,
                w.schemaVersion,
                w.data,
                w.diff
              ),
              cursor
            )
          case w: VersionAdd =>
            cursor = cursor.next(txn.txnTS)
            StreamValue.DocVersion(
              Version.Deleted(
                w.scope,
                w.id,
                w.writeTS,
                w.schemaVersion,
                w.diff
              ),
              cursor
            )
        }.toSeq
      }
    }

    final class Set(
      terms: Vector[IndexTerm],
      coveredValues: Seq[IndexSet.CoveredValue])
        extends Collector {

      private lazy val hasMVAValue =
        coveredValues.drop(terms.size) exists { _.entry.isMVA }

      def collectStreamableValue(txn: TxnResult) = {
        // NB. MVAs break parwise derivation. Fallback to unresolved docs.
        var cursor = Value.EventSource.Cursor.MinValue
        if (hasMVAValue) {
          txn.writes.iterator
            .collect { case w: SetAdd =>
              cursor = cursor.next(txn.txnTS)
              StreamValue.UnresolvedDoc(w.scope, w.doc, cursor)
            }
            .distinctBy { _.docID }
            .toSeq
        } else {
          StreamValue.reduceIndexRows(
            terms,
            coveredValues,
            txn.writes.view
              .collect { case w: SetAdd =>
                val tuple = IndexTuple(w.scope, w.doc, w.values, w.ttl)
                IndexValue(tuple, w.writeTS.resolve(txn.txnTS), w.action)
              }
              .toSeq
              /** When the index is partitioned the order in which we combine the partitioned writes is non-deterministic.
                * This means that without sorting here we can end up with unstable cursor ordering.
                */
              .sorted(IndexValue.ByValueOrdering)
          )
        }
      }
    }
  }

  def forDocs(
    auth: Auth,
    filter: EventFilter = EventFilter.empty,
    transformer: EventTransformer = EventTransformer.empty) =
    new EventTranslator(auth, filter, transformer, Collector.Docs)

  def forSets(
    auth: Auth,
    terms: Vector[IndexTerm],
    coveredValues: Seq[IndexSet.CoveredValue],
    filter: EventFilter = EventFilter.empty,
    transformer: EventTransformer = EventTransformer.empty
  ) = {
    val collector = new Collector.Set(terms, coveredValues)
    new EventTranslator(auth, filter, transformer, collector)
  }
}

final class EventTranslator private (
  auth: Auth,
  filter: EventFilter,
  transformer: EventTransformer,
  collector: EventTranslator.Collector
) {

  private lazy val valueTranslator =
    new StreamValueTranslator(auth, filter, transformer)

  def translate(txn: TxnResult): Query[Seq[Event.Processed]] =
    if (txn.writes.isEmpty) { // start event.
      Query.value(
        Seq(
          Event.Processed(
            Event.Status(txn.txnTS),
            Event.Metrics.Empty
          )))
    } else {
      translateTxnResult(txn)
    }

  private def translateTxnResult(txn: TxnResult): Query[Seq[Event.Processed]] =
    collector
      .collectStreamableValue(txn)
      .map { value =>
        // Either is used here to ensure that sequenceT stops at the first error.
        valueTranslator.translate(value) map { _.toEither }
      }
      .sequenceT
      .map {
        case Right(events) => events
        case Left(err)     => err :: Nil
      }
}
