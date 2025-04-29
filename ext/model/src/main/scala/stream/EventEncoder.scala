package fauna.model.stream

import fauna.ast._
import fauna.atoms.{ IndexID, ScopeID }
import fauna.lang._
import fauna.repo.doc.Version
import fauna.repo.IndexConfig
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._

object EventEncoder {

  def apply(txnTS: Timestamp, fields: Set[EventField]): EventEncoder =
    new EventEncoder(txnTS, fields)
}

final class EventEncoder(txnTS: Timestamp, fields: Set[EventField]) {

  private final class EventBuilder {

    private[this] val res = List.newBuilder[(String, Literal)]
    res.sizeHint(fields.size)

    def add(field: EventField)(value: => Literal): Unit =
      if (fields.contains(field)) {
        res += field.name -> value
      }

    def result(): Literal =
      ObjectL(res.result())
  }

  def encode(event: StreamEvent): Literal =
    ObjectL(
      "type" -> event.eventType.toLiteral,
      "txn" -> LongL(txnTS.micros),
      "event" -> eventToLiteral(event)
    )

  private def eventToLiteral(event: StreamEvent): Literal =
    event match {
      case StreamStart(ts) =>
        LongL(ts.micros)

      case StreamError(code, desc) =>
        ObjectL(
          "code" -> StringL(code),
          "description" -> StringL(desc)
        )

      case event: VersionAdded =>
        val res = new EventBuilder()
        val write = event.write

        // Events are streamed before applied to the storage, therefore, their
        // timestamps are still unresolved and have to be resolved here.
        val resolvedTS = write.writeTS.resolve(txnTS)

        val (data, diff) = write.action match {
          case Create | Update =>
            (write.data, write.diff)

          case Delete =>
            // swap data and diff from removed versions so the rendered event
            // can display data from deleted version as expected
            val diff = write.diff.getOrElse(Diff.empty)
            (Data(diff.fields), Some(Diff.empty))
        }

        val version = Version.Live(
          write.scope,
          write.id,
          resolvedTS,
          write.action,
          write.schemaVersion,
          data,
          diff
        )

        val baseFields = MapV(
          "ref" -> DocIDV(write.id),
          "ts" -> LongV(resolvedTS.validTS.micros)
        )

        res.add(EventField.Action) {
          write.action match {
            case Create if write.diff.isEmpty => ActionL(Create)
            case Create | Update              => ActionL(Update)
            case Delete                       => ActionL(Delete)
          }
        }

        res.add(EventField.Document) {
          Literal(
            write.scope,
            baseFields.merge(
              readableData(version)
            ))
        }

        res.add(EventField.Diff) {
          Literal(
            write.scope,
            baseFields.merge(
              readableData(version.event)
                .remove(List("ts")) // don't override resolved ts
            ))
        }

        res.add(EventField.Prev) {
          Literal(
            write.scope,
            baseFields.merge(
              readableData(
                version.withData(
                  version.prevData().getOrElse(Data.empty)
                )))
          )
        }

        res.result()

      case VersionRemoved(write) =>
        val res = new EventBuilder()
        res.add(EventField.Action) { ActionL(write.action) }
        res.add(EventField.Document) {
          ObjectL(
            "ref" -> RefL(write.scope, write.id),
            "ts" -> LongL(write.writeTS.resolve(txnTS).validTS.micros)
          )
        }
        res.result()

      case DocumentRemoved(write) =>
        val res = new EventBuilder()
        res.add(EventField.Action) { ActionL(Delete) }
        res.add(EventField.Document) {
          ObjectL(
            "ref" -> RefL(write.scope, write.id),
            "ts" -> LongL(txnTS.micros)
          )
        }
        res.result()

      case SetAdded(write, isPartitioned) =>
        val res = new EventBuilder()
        res.add(EventField.Action) { ActionL(write.action) }
        res.add(EventField.Document) {
          ObjectL(
            "ref" -> RefL(write.scope, write.doc),
            "ts" -> LongL(write.writeTS.resolve(txnTS).validTS.micros)
          )
        }
        res.add(EventField.Index) {
          renderIndex(
            write.scope,
            write.index,
            write.terms,
            write.values,
            isPartitioned
          )
        }
        res.result()

      case SetRemoved(write, isPartitioned) =>
        val res = new EventBuilder()
        res.add(EventField.Action) { ActionL(write.action) }
        res.add(EventField.Document) {
          ObjectL(
            "ref" -> RefL(write.scope, write.doc),
            "ts" -> LongL(write.writeTS.resolve(txnTS).validTS.micros)
          )
        }
        res.add(EventField.Index) {
          renderIndex(
            write.scope,
            write.index,
            write.terms,
            write.values,
            isPartitioned
          )
        }
        res.result()
    }

  private def renderIndex(
    scopeID: ScopeID,
    indexID: IndexID,
    terms: Vector[IRValue],
    values: Vector[IndexTerm],
    isPartitioned: Boolean): Literal = {

    val termsToRender =
      if (isPartitioned) {
        IndexConfig.dropPartitionKey(terms)
      } else {
        terms
      }

    ObjectL(
      "ref" -> Literal(scopeID, DocIDV(indexID.toDocID)),
      "terms" -> Literal(scopeID, ArrayV(termsToRender)),
      "values" -> Literal(scopeID, ArrayV(values.map { _.value }))
    )
  }

  private def readableData(version: Version): MapV =
    ReadAdaptor(version.id.collID)
      .readableData(version)
      .fields

  private def readableData(event: DocEvent): MapV =
    ReadAdaptor(event.docID.collID)
      .readableData(event)
      .fields
}
