package fauna.repo.service.stream

import fauna.atoms._
import fauna.codex.cbor._
import fauna.storage._
import fauna.storage.ir._
import fauna.storage.ops._
import io.netty.buffer.ByteBuf

/** StreamKey uniquely identifies a single stream. */
sealed trait StreamKey {
  import StreamKey._

  def rowKey: ByteBuf =
    this match {
      case DocKey(scope, id)        => Tables.Versions.rowKeyByteBuf(scope, id)
      case SetKey(scope, id, terms) => Tables.Indexes.rowKey(scope, id, terms)
    }
}

object StreamKey {

  final case class DocKey(scope: ScopeID, docID: DocID) extends StreamKey

  final case class SetKey(scope: ScopeID, indexID: IndexID, terms: Vector[IRValue])
      extends StreamKey

  def apply(write: Write): Option[StreamKey] =
    write match {
      case w: VersionAdd    => Some(DocKey(w.scope, w.id))
      case w: VersionRemove => Some(DocKey(w.scope, w.id))
      case w: DocRemove     => Some(DocKey(w.scope, w.id))
      case w: SetAdd        => Some(SetKey(w.scope, w.index, w.terms))
      case w: SetRemove     => Some(SetKey(w.scope, w.index, w.terms))
      case _                => None
    }

  def groupWritesByKey(writes: Vector[Write]): Map[StreamKey, Vector[Write]] =
    if (writes.isEmpty) {
      Map.empty
    } else {
      val writesAndKeys =
        writes flatMap { write =>
          StreamKey(write) map { key =>
            key -> write
          }
        }
      writesAndKeys.groupMap(_._1)(_._2)
    }

  implicit val codec =
    CBOR.SumCodec[StreamKey](
      CBOR.TupleCodec[DocKey],
      CBOR.TupleCodec[SetKey],
    )
}
