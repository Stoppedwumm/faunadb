package fauna.model.runtime.stream

import fauna.atoms.{ DocID, ScopeID }
import fauna.auth.Auth
import fauna.lang.{ Timestamp, Timing }
import fauna.lang.syntax._
import fauna.model.runtime.fql2.IndexSet
import fauna.model.RuntimeEnv
import fauna.repo.{ PagedQuery, Store }
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.storage.api.set.Scalar
import fauna.storage.index.IndexValue
import fauna.storage.VersionID
import scala.collection.mutable.{ SeqMap => MSeqMap }

object EventReplayer {

  private trait HistoryReader {
    type Values = Iterable[StreamValue.Unresolved]
    def canReadAt(ts: Timestamp): Query[Boolean]

    def read(
      from: Timestamp,
      to: Option[Timestamp],
      pageSize: Int
    ): PagedQuery[Values]
  }

  private object HistoryReader {

    final case class Versions(scope: ScopeID, id: DocID) extends HistoryReader {

      def canReadAt(ts: Timestamp) =
        Query.repo flatMap {
          _.mvtProvider.get(scope, id.collID) map { mvt => ts >= mvt }
        }

      def read(from: Timestamp, to: Option[Timestamp], pageSize: Int) =
        Query.readAndIncrStats(Query.incrDocuments(_)) {
          RuntimeEnv.Default
            .Store(scope)
            .versions(
              id = id,
              // Include the writes from the subscribing transaction.
              from = VersionID.MinValue.copy(validTS = from),
              // Include the writes from the streaming start txn time.
              to = to.fold(VersionID.MaxValue) { ts =>
                VersionID.MaxValue.copy(validTS = ts.nextMicro)
              },
              pageSize = pageSize,
              reverse = true
            )
            .reduceStreamT(Value.EventSource.Cursor.MinValue) {
              case (None, cursor) => (Nil, cursor)
              case (Some(version), cursor) =>
                val cursor0 = cursor.next(version.ts.validTS)
                (StreamValue.DocVersion(version, cursor0) :: Nil, cursor0)
            }
        }
    }

    final case class Sets(set: IndexSet) extends HistoryReader {

      private lazy val hasMVAValue =
        set.coveredValues.drop(set.terms.size) exists { _.entry.isMVA }

      def canReadAt(ts: Timestamp) =
        Query.repo flatMap {
          _.mvtProvider.get(set.config, set.terms map { _.value: Scalar }) map {
            mvts => mvts.all forall { mvt => ts >= mvt }
          }
        }

      def read(from: Timestamp, to: Option[Timestamp], pageSize: Int) =
        Query.readAndIncrStats(Query.addSets(1, set.config.partitions.toInt, _)) {

          /** A Note on the HistoricalIndex bounds:
            * HistoricalIndex entries within a row are sorted first by time followed by action followed by value.
            * These are sorted in reverse order which means that the default way to scan the historical index is down
            * from the most recent Add with the highest values to the furthest in the past Remove with the lowest values.
            * The order can be thought of in a simplified form as follows:
            * time 5, Add, DocID3
            * time 5, Add, DocID2
            * time 5, Remove , DocID3
            * time 5, Remove , DocID2
            * time 3, Add, DocID2
            * time 2, Add, DocID30
            * time 2, Remove, DocID30
            *
            * When we do the historical index scan below, we are scanning in ascending order, so effectively from the
            * bottom of the example above to the top. We start at some TS provided by the stream and scan upwards.
            * Because we are starting at the bottom, we want start our from bound at the prevNano from our desired start
            * ts. Because our timestamps are not assigned at the nano granularity, this ensures that we will see all
            * results from the desired transaction without including any from any transaction prior.
            * For our to bound we use a similar approach to ensure we receive all relevant events.
            * We use IndexValue.MaxValue by default, which uses Unresolved for its valid ts, which will include all
            * values on disk, regardless of action. If a to is provided, we increment the ts with nextMicro to ensure we
            * include all events from the to ts.
            * This ensures that we include all of the historical index entries within the requested time bound.
            */
          val indexFrom = IndexValue.MinValue.atValidTS(from.prevNano)
          val indexTo = to.fold(IndexValue.MaxValue) { ts =>
            IndexValue.MaxValue.atValidTS(ts.nextMicro)
          }

          val historyQ =
            Store
              .historicalIndex(
                set.config,
                set.terms,
                // Include the writes from the subscribing transaction.
                from = indexFrom,
                // Include the writes from the streaming start txn time.
                to = indexTo,
                pageSize = pageSize,
                ascending = true
              )

          if (hasMVAValue) {
            historyQ
              .alignByT { case (a, b) =>
                a.ts.validTS == b.ts.validTS
              }
              .spansT { case (a, b) => a.ts.validTS == b.ts.validTS }
              .mapT { indexRows =>
                var cursor = Value.EventSource.Cursor.MinValue
                indexRows
                  .foldLeft(MSeqMap.empty[DocID, StreamValue.UnresolvedDoc]) {
                    case (docMap, iv) =>
                      cursor = cursor.next(iv.ts.validTS)
                      if (docMap.contains(iv.docID)) {
                        // since we are mapping to unresolved documents in the mva
                        // case,
                        // we don't need to worry about combining the index results.
                        // In this case
                        docMap
                      } else {
                        val streamVal = StreamValue.UnresolvedDoc(
                          set.config.scopeID,
                          iv.docID,
                          cursor
                        )
                        docMap.update(iv.docID, streamVal)
                        docMap
                      }
                  }
                  .values
              }
          } else {

            /** alignByT ensures that a given ts won't span multiple pages.
              * spansT makes it such that each page provided will only have elements of the same valid ts.
              * This allows us to map over each page and merge the events for a given ts.
              */
            historyQ
              .alignByT { case (a, b) =>
                a.ts.validTS == b.ts.validTS
              }
              .spansT { case (a, b) => a.ts.validTS == b.ts.validTS }
              .mapT { StreamValue.reduceIndexRows(set.terms, set.coveredValues, _) }
          }
        }
    }
  }
}

final class EventReplayer(
  auth: Auth,
  filter: EventFilter = EventFilter.empty,
  transformer: EventTransformer = EventTransformer.empty) {
  import EventReplayer._

  private lazy val valueTranslator =
    new StreamValueTranslator(auth, filter, transformer)

  def replay(
    set: IndexSet,
    from: Value.EventSource.Cursor,
    to: Option[Timestamp],
    pageSize: Int
  ): PagedQuery[Iterable[Event.Processed]] =
    replay(HistoryReader.Sets(set), from, to, pageSize)

  def replay(
    scopeID: ScopeID,
    docID: DocID,
    from: Value.EventSource.Cursor,
    to: Option[Timestamp],
    pageSize: Int
  ): PagedQuery[Iterable[Event.Processed]] =
    replay(HistoryReader.Versions(scopeID, docID), from, to, pageSize)

  private def replay(
    reader: HistoryReader,
    from: Value.EventSource.Cursor,
    to: Option[Timestamp],
    pageSize: Int
  ): PagedQuery[Iterable[Event.Processed]] =
    reader.canReadAt(from.ts) flatMap { allowed =>
      if (!allowed) {
        PagedQuery(
          List(
            Event.Processed(Event.Status(from.ts), Event.Metrics.Empty),
            Event.Processed(
              Event.Error(
                code = Event.Error.Code.InvalidStreamStartTime,
                message =
                  s"Stream start time ${from.ts} is too far in the past. Recreate the stream and try again."),
              Event.Metrics.Empty
            )
          ))
      } else {
        // XXX: what should we do with historical writes
        val startMetricsTimer = Timing.start
        Query.withMetrics {
          reader
            .read(from.ts, to, pageSize)
            .dropWhileT { _.cursor <= from }
        } flatMap { case (values, stateMetrics) =>
          val startMetrics =
            Event.Metrics(
              stateMetrics,
              startMetricsTimer.elapsedMillis
            )

          val status = Event.Processed(Event.Status(from.ts), startMetrics)
          val eventsQ = values mapValuesM { valueTranslator.translate(_) }
          PagedQuery(status :: Nil, eventsQ)
        }
      }
    }
}
