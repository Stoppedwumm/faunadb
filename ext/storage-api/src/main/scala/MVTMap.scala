package fauna.storage.api

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang._
import scala.language.implicitConversions
import scala.util.control.NoStackTrace

/** Thrown by `MVTMap` when no MVT is known for the given collection ID. */
final case class UnknownCollectionMVT(collID: CollectionID)
    extends Exception(s"Unknown MVT for $collID")
    with NoStackTrace

/** A CBOR encodable map from CollectionID => MVT (timestamp) */
sealed trait MVTMap {

  def all: Iterable[Timestamp]

  def apply(collID: CollectionID): Option[Timestamp]

  final def contains(collID: CollectionID) =
    apply(collID).isDefined

  @throws[UnknownCollectionMVT]
  final def getOrFail(collID: CollectionID) =
    apply(collID) getOrElse {
      // TODO: return this error to coordinators to signal cache staleness.
      throw UnknownCollectionMVT(collID)
    }

  /** Returns the first invalid MVT higher than the given valid time, if any. */
  private[api] def findInvalid(validTS: Timestamp): Option[(CollectionID, Timestamp)]
}

object MVTMap {

  private type M = Map[CollectionID, Timestamp]

  /** An `MVTMap` that returns `Epoch` for all lookups. */
  case object Default extends MVTMap {
    def all = Iterable(Timestamp.Epoch)
    def apply(collID: CollectionID) = Some(Timestamp.Epoch)
    private[api] def findInvalid(validTS: Timestamp) = None
  }

  /** Wraps a scala map into a `MVTMap`. */
  final case class Mapping(toMap: M) extends MVTMap {
    def all = toMap.values
    def apply(collID: CollectionID) = toMap.get(collID)
    private[api] def findInvalid(validTS: Timestamp) =
      toMap.find { case (_, mvt) => mvt > validTS }
  }

  implicit def apply(map: Map[CollectionID, Timestamp]): MVTMap =
    Mapping(map)

  implicit val Codec =
    CBOR.SumCodec[MVTMap](
      CBOR.SingletonCodec(Default),
      CBOR.AliasCodec[Mapping, M](Mapping(_), _.toMap)
    )
}
