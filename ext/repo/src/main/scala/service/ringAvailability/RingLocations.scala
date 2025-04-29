package fauna.repo.service.ringAvailability

import fauna.atoms.{ HostID, Segment }

object RingLocations {

  /** Returns the percentage of the ring that's available across all replicas.
    *
    * @return A Double representing the percentage of available Locations on the ring.
    */
  def percentAvailable(
    dataReplicas: => Set[String],
    hostsByReplica: String => Set[HostID],
    isAlive: HostID => Boolean,
    segmentsByHost: HostID => Seq[Segment]): Double = {

    val upSegments = dataReplicas
      .flatMap(r => {
        hostsByReplica(r)
          .filter(h => isAlive(h))
          .flatMap(h => segmentsByHost(h))
      })
      .toSeq

    val normalized = Segment.normalize(upSegments)
    val all = Vector(Segment.All)
    val diff = Segment.diff(all, normalized)
    val diffLength = BigDecimal(Segment.sumLength(diff))
    val ringLength = BigDecimal(Segment.RingLength)

    ((ringLength - diffLength) / ringLength).toDouble * 100
  }
}
