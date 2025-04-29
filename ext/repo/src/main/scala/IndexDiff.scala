package fauna.repo

import fauna.atoms._
import fauna.codex.cbor._
import fauna.codex.json2._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.storage._
import fauna.storage.api._
import fauna.storage.cassandra._
import fauna.storage.index._
import io.netty.buffer._
import org.apache.cassandra.db.ColumnFamilyStore

object IndexDiff {
  object Event {
    final val ActionField = JSON.Escaped("action")
    final val CollectionField = JSON.Escaped("collection_id")
    final val DocumentField = JSON.Escaped("doc_id")
    final val IndexField = JSON.Escaped("index_id")
    final val ScopeField = JSON.Escaped("scope_id")
    final val SubIDField = JSON.Escaped("sub_id")
    final val TTLField = JSON.Escaped("ttl")
    final val TermsField = JSON.Escaped("terms")
    final val TransactionTSField = JSON.Escaped("transaction_ts")
    final val ValidTSField = JSON.Escaped("valid_ts")
    final val ValuesField = JSON.Escaped("values")
  }

  sealed abstract class Event {
    import Event._

    def row: IndexRow

    def toJSON: String = {
      val alloc = ByteBufAllocator.DEFAULT
      val buf = alloc.ioBuffer

      try {
        val out = JSONWriter(buf)

        out.writeObjectStart()

        out.writeObjectField(
          ScopeField,
          out.writeString(row.key.scope.toLong.toString))

        out.writeObjectField(IndexField, out.writeString(row.key.id.toLong.toString))

        out.writeObjectField(
          TermsField, {
            out.writeArrayStart()

            row.key.terms foreach { term =>
              writeTerm(out, term)
            }

            out.writeArrayEnd()
          })

        out.writeObjectField(
          DocumentField, {
            out.writeObjectStart()

            out.writeObjectField(
              CollectionField,
              out.writeString(row.value.docID.collID.toLong.toString))

            out.writeObjectField(
              SubIDField,
              out.writeString(row.value.docID.subID.toLong.toString))

            out.writeObjectEnd()
          }
        )

        out.writeObjectField(
          ValuesField, {
            out.writeArrayStart()

            row.value.tuple.values foreach { value =>
              writeTerm(out, value)
            }

            out.writeArrayEnd()
          })

        out.writeObjectField(
          TTLField,
          row.value.tuple.ttl foreach { ts => out.writeString(ts.toString) })

        out.writeObjectField(
          ValidTSField,
          out.writeString(row.value.ts.validTS.toString))

        out.writeObjectField(
          TransactionTSField,
          out.writeString(row.value.ts.transactionTS.toString))

        out.writeObjectField(ActionField, out.writeString(row.value.action.toString))

        out.writeObjectEnd()

        buf.toUTF8String
      } finally {
        buf.release()
      }
    }

    private def writeTerm(out: JSONWriter, term: IndexTerm) = {
      val alloc = ByteBufAllocator.DEFAULT
      val buf = alloc.ioBuffer

      try {
        out.writeString(CBOR.encode(buf, term).toHexString)
      } finally {
        buf.release()
      }
    }
  }

  // Secondary has this row, but Primary does not.
  final case class Add(row: IndexRow) extends Event

  // Primary has this row, but Secondary does not.
  final case class Remove(row: IndexRow) extends Event
}

/** Computes the difference between two index column families. The
  * result is a patch which when applied to primary, creates a
  * snapshot equal to secondary.
  */
final class IndexDiff(
  primary: ColumnFamilyStore,
  secondary: ColumnFamilyStore,
  mvts: Map[ScopeID, MVTMap],
  snapshotTS: Timestamp,
  segment: Segment = Segment.All,
  selector: Selector = Selector.All) {

  // XXX: HistoricalIndex should also work without changes,
  // but I'm specifically writing this for SI.
  require(
    primary.name == Tables.SortedIndex.CFName ||
      primary.name == Tables.SortedIndex.CFName2)

  require(
    secondary.name == Tables.SortedIndex.CFName ||
      secondary.name == Tables.SortedIndex.CFName2)

  private[this] val logger = getLogger()

  def run(): Seq[IndexDiff.Event] = {
    // XXX: If this becomes too unwieldy, change this class
    // to an Iterator[IndexDiff.Event] or similar.
    val events = Seq.newBuilder[IndexDiff.Event]
    val counter = new IteratorStatsCounter

    val pri = mkIter(counter, primary)
    val sec = mkIter(counter, secondary)

    while (pri.hasNext) {
      var p = pri.next()

      if (!sec.hasNext) {
        // Primary has a cell which is missing in secondary.
        events += remove(p)
      } else {
        var s = sec.next()

        while (s.value.tuple > p.value.tuple) {
          // Secondary has at least one cell which is missing in
          // primary.
          events += add(s)

          if (sec.hasNext) {
            s = sec.next()
          } else {
            // break...
            s = p
          }
        }

        while (p.value.tuple > s.value.tuple) {
          // Primary has at least one cell which is missing in
          // secondary.
          events += remove(p)

          if (pri.hasNext) {
            p = pri.next()
          } else {
            // break...
            s = p
          }
        }
      }
    }

    while (sec.hasNext) {
      // Secondary has at least one cell which is missing in
      // primary.
      val s = sec.next()
      events += add(s)
    }

    events.result()
  }

  private def add(row: IndexRow): IndexDiff.Event = {
    logger.warn(s"Primary is missing $row!")
    IndexDiff.Add(row)
  }

  private def remove(row: IndexRow): IndexDiff.Event = {
    logger.warn(s"Secondary is missing $row!")
    IndexDiff.Remove(row)
  }

  private def mkIter(
    counter: IteratorStatsCounter,
    cfs: ColumnFamilyStore): IndexIterator = {
    val citer =
      new CassandraIterator(counter, cfs, ScanBounds(segment), selector, snapshotTS)

    IndexIterator(citer, mvts)
  }
}
