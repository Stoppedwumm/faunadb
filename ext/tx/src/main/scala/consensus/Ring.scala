package fauna.tx.consensus

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import scala.collection.{ Map => IMap }

object Ring {
  implicit val CBORCodec = CBOR.AliasCodec[Ring, Vector[HostID]](r => Ring(r.toSet), _.all.toVector)
}

/**
  * Handles cluster membership & cluster transition states.
  */
case class Ring(all: Set[HostID]) {

  private val quorum = size / 2 + 1

  def size = all.size
  def contains(id: HostID) = all contains id
  def peers(self: HostID) = all - self

  // member state

  def addMember(ids: HostID*) = Ring(all ++ ids)

  def removeMember(ids: HostID*) = Ring(all -- ids)

  // quorum-based value selection

  def hasQuorum(ids: scala.collection.Set[HostID]) = filter(ids).size >= quorum

  /**
    * Returns a hostID that majority of nodes agree on.
    *
    * Eg: if [[all]] is Seq(node1, node2, node3) and ids are Seq(node1, node1, node2)
    * this means majority of the nodes agree on node1 which is returned.
    */
  def quorumValue[T](values: Iterable[T]): Option[T] =
    values.groupBy(identity) collectFirst {
      case (value, groupIDs) if groupIDs.size >= quorum =>
        value
    }

  def filter(ids: scala.collection.Set[HostID]) = all & ids

  def minCommitted[V](elems: IMap[HostID, V])(implicit
    ord: Ordering[V]): Option[V] = {
    // descending order by value
    val sorted = elems.iterator
      .filter { case (k, _) => all contains k }
      .toSeq
      .sortWith { case ((_, a), (_, b)) => ord.lt(b, a) }

    sorted.iterator.map { _._2 }.drop(quorum - 1).nextOption()
  }
}
