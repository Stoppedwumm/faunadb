package fauna.storage.api

import fauna.codex.cbor.CBOR
import fauna.lang._
import fauna.storage._
import fauna.storage.api._
import fauna.storage.index.{ IndexTerm, IndexTuple, IndexValue }
import fauna.storage.ir.IRValue
import scala.language.implicitConversions

package object set {
  type Term = Scalar

  // Sentinel "none" value for an index entry, which will not be emitted in
  // snapshots because it is a delete.
  val CollectionAtTSSentinel: IndexValue =
    IndexValue(IndexTuple.MinValue, Unresolved, Remove)
}

package set {

  final case class Scalar(unwrap: IRValue) extends AnyVal

  object Scalar {

    implicit def apply[A](wrapped: A)(implicit view: A => IRValue): Scalar =
      new Scalar(wrapped)

    implicit val Codec =
      CBOR.AliasCodec[Scalar, IRValue](
        Scalar(_),
        _.unwrap
      )
  }

  /** Common implementation of read cursors over indexed values. */
  abstract class IndexValueCursor[C <: Read.Cursor[IndexValue, C]](
    ordering: Ordering[IndexValue])
      extends Read.Cursor.OnDisk[IndexValue, C]()(ordering) {

    protected final def validTS(v: IndexValue) = v.ts.validTS

    protected final def adjust(v: IndexValue, mvt: Timestamp, floor: Boolean) =
      v.copy(ts = AtValid(mvt), action = if (floor) Add else Remove)
  }

  object SetPadding {

    /** Pad the given tuples based on a sequence of covered values reverse flags. The
      * sequence MUST have the same size as the underlying set's covered values. Each
      * given boolean represents the set covered value `reverse` flag (IN ORDER).
      *
      * NB. Failing to provide an accurate sequence of covered values reverse flags
      * will result in wrong padding.
      */
    def pad(
      max: IndexTuple,
      min: IndexTuple,
      coveredValuesReverseFlag: Iterable[Boolean]): (IndexTuple, IndexTuple) = {

      def pad0(tuple: IndexTuple, floor: Boolean) = {
        if (coveredValuesReverseFlag.sizeIs <= tuple.values.size) {
          tuple // no padding needed
        } else {
          val padding =
            coveredValuesReverseFlag.iterator.drop(tuple.values.size) map { rev =>
              val term = if (floor ^ rev) IndexTerm.MinValue else IndexTerm.MaxValue
              term.copy(reverse = rev)
            }
          tuple.copy(values = tuple.values ++ padding)
        }
      }

      (pad0(max, floor = false), pad0(min, floor = true))
    }

    /** Pad the given indexed values tuples. See overloaded `SetPadding.pad(..)`. */
    def pad(
      max: IndexValue,
      min: IndexValue,
      coveredValuesReverseFlag: Iterable[Boolean]): (IndexValue, IndexValue) = {
      val (tupleMax, tupleMin) = pad(max.tuple, min.tuple, coveredValuesReverseFlag)
      (max.withTuple(tupleMax), min.withTuple(tupleMin))
    }
  }
}
