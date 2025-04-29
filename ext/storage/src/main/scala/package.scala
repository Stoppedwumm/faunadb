package fauna

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.storage.ops.{ InsertDataValueWrite, Write }
import io.netty.buffer.{ ByteBuf, Unpooled }
import org.apache.cassandra.db.{ Cell => CCell }
import scala.concurrent.duration._

package storage {
  case class SchemaViolationException(msg: String) extends Exception(msg)
  case class SchemaConfigurationException(msg: String) extends Exception(msg)
  case class ComponentTooLargeException(msg: String) extends Exception(msg)

  /** An ordering of results returned by a read op, relative to some natural ordering.
    * Read ops supporting an order must explain its meaning.
    *
    * !!! WARNING !!!
    *
    * Ordering defaults to *DESCENDING*, against the norm. Reversing
    * the natural ordering of a query requires an *ASCENDING*
    * order. This happens to work in FaunaDB because a "forward" query
    * is always descending in time - the time component in each CF
    * comparator is reversed - and a "reverse" is ascending in
    * time. However, this conflicts with the _natural_ ordering of the
    * CF (viz. its comparator).
    *
    * This is difficult to correct due to the Boolean encoding on the
    * wire.
    */
  sealed trait Order {
    def reverse = this match {
      case Order.Ascending  => Order.Descending
      case Order.Descending => Order.Ascending
    }
  }

  object Order {
    case object Ascending extends Order
    case object Descending extends Order

    implicit def Codec =
      CBOR.AliasCodec[Order, Boolean](
        if (_) Ascending else Descending,
        _ == Ascending
      )
  }

  object Cell {
    implicit val codec = CBOR.TupleCodec[Cell]

    /** Construct a Fauna Cell from a C* Cell. */
    def apply(cell: CCell): Cell =
      Cell(
        Unpooled.wrappedBuffer(cell.name.toByteBuffer),
        Unpooled.wrappedBuffer(cell.value),
        Timestamp.ofMicros(cell.timestamp()))
  }

  /**
    * A Cell is the most fundamental unit of storage, consisting of a
    * key/value pair and a valid time.
    */
  case class Cell(name: ByteBuf, value: ByteBuf, ts: Timestamp) {
    def byteSize: Int = name.readableBytes + value.readableBytes

    override def toString = s"Cell(${name.toHexString}, ${value.toHexString}, $ts)"
  }

  /**
    * A Row is a sequence of cells each having the same row key, at a
    * particular valid time.
    */
  final case class Row(cfs: Vector[(String, Vector[Cell])], ts: Timestamp)

  object ScanBounds {
    val All = apply(Segment.All)

    implicit val codec = CBOR.TupleCodec[ScanBounds]

    def apply(left: ByteBuf, right: Location): ScanBounds = ScanBounds(Left(left), right)
    def apply(left: Location, right: Location): ScanBounds = ScanBounds(Right(left), right)

    def apply(seg: Segment): ScanBounds = ScanBounds(Right(seg.left), seg.right)
  }

  /**
    * ScanBounds is a range of locations [left, right), used to define
    * a range scan ("table scan").
    *
    * NOTE: When providing a ByteBuf, the encoded representation
    * should represent a row key, which can be located on the token
    * ring, not an entire cell name.
    */
  case class ScanBounds(left: Either[ByteBuf, Location], right: Location) {
    override def toString = {
      val leftStr = left.fold(_.toHexString, { l => s"Location(${l.token})" })
      s"ScanBounds($leftStr, Location(${right.token}))"
    }
  }

  object ScanSlice {
    def apply(cf: String, bounds: ScanBounds): ScanSlice =
      ScanSlice(cf, bounds, Unpooled.EMPTY_BUFFER)
  }

  /**
    * ScanSlice is a range scan which begins within - "slices" into -
    * the first row of the bounds' left location, and proceeds
    * similarly to ScanBounds thereafter.
    */
  case class ScanSlice(cf: String, bounds: ScanBounds, startCol: ByteBuf) {
    override def toString = s"ScanSlice($cf, $bounds, ${startCol.toHexString})"
  }

  object ScanRow {
    implicit val codec = CBOR.TupleCodec[ScanRow]
  }


  /**
    * A ScanRow is similar to a Row, but may only originate from a
    * single column family. It is produced by range scans (see
    * ScanBounds and ScanSlice).
    */
  case class ScanRow(cf: String, key: ByteBuf, cells: Vector[Cell]) {
    override def toString = s"ScanRow($cf, ${key.toHexString}, $cells)"
  }

  object TxnRead {
    def inDefaultRegion(b: ByteBuf) = TxnRead(b, RegionID.DefaultID)

    implicit val codec = new CBOR.Codec[TxnRead] {
      override def encode(stream: CBOR.Out, r: TxnRead) = {
        if (r.region == RegionID.DefaultID) {
          // For default region, keep writing the old wire format (just the row key)
          // to ensure backwards compatibility with older nodes during upgrade.
          CBOR.encode(stream, r.rowKey)
        } else {
          // Otherwise encode as TupleCodec would.
          stream.writeArrayStart(2)
          CBOR.encode(stream, r.rowKey)
          CBOR.encode(stream, r.region)
        }
      }

      override def decode(stream: CBOR.In) = {
        object CompatibilitySwitch extends PartialCBORSwitch[TxnRead](s"${TypeLabels.ByteStringLabel} or ${TypeLabels.ArrayLabel}") {
          // For upgrade compatibility (being able to read old transaction logs)
          // preserve ability to read just a byte array.
          override def readBytes(bytes: ByteBuf, stream: CBORParser) =
            inDefaultRegion(stream.bufRetentionPolicy.read(bytes))

          // Also be able to decode as TupleCodec would
          override def readArrayStart(length: Long, stream: CBORParser) =
            if (length == 2) {
              TxnRead(CBOR.decode[ByteBuf](stream), CBOR.decode[RegionID](stream))
            } else {
              throw CBORInvalidLengthException(length, 2)
            }
        }

        stream.read(CompatibilitySwitch)
      }
    }
  }

  final case class TxnRead(rowKey: ByteBuf, region: RegionID) extends Ordered[TxnRead] {
    override def compare(o: TxnRead) = {
      val c = rowKey compareTo o.rowKey
      if (c != 0) {
        c
      } else {
        region compareTo o.region
      }
    }

    override def toString =
      s"TxnRead(${CBOR.showBuffer(rowKey)}, $region)"
  }
}

package object storage {
  /**
    * The type of transactions composed of Write iops in this storage
    * engine.
    */
  type Txn = (Map[TxnRead, Timestamp], Vector[Write])

  /**
    * An implementation of fauna.tx.transaction.MismatchResetTxnFactory
    *
    * When a mismatch happens, we can infer two things: 1 or more dependent
    * reads were torn, and very likely tore whatever the transaction was
    * writing. Resetting the timestamps of all reads or writes in the
    * transaction will fix the read tears themselves and proactively fix what
    * the original transaction wrote.
    */
  def mismatchResetTxn(txn: Txn): Option[Txn] = {
    val (reads, writes) = txn
    val b = Vector.newBuilder[Write]

    def addReset(key: ByteBuf) =
      if (key.isReadable) {
        // The only purpose of this write is to trigger the storage engine to
        // insert a real row timestamp at the txntime. TTL is short because we
        // don't need these to hang around.
        b += InsertDataValueWrite(
          Tables.RowTimestamps.CFName,
          key,
          Tables.RowTimestamps.encode(Timestamp.Epoch),
          Unpooled.EMPTY_BUFFER,
          Some(1.second))
      }

    reads.keys foreach { r => addReset(r.rowKey) }
    writes foreach { w => addReset(w.rowKey) }

    val resets = b.result()

    if (resets.isEmpty) None else Some((Map.empty, resets))
  }
}
