package fauna.repo.cassandra

import fauna.atoms._
import fauna.cluster.Membership
import fauna.cluster.topology.SegmentOwnership
import org.apache.cassandra.dht.{ LongToken, Token }
import org.apache.cassandra.service.StorageService
import scala.collection.mutable.{ Map => MMap }
import scala.jdk.CollectionConverters._

object CassandraTopologiesInitializer {
  private class RingBuilder {
    val lb = Vector.newBuilder[Location]
    val hb = Vector.newBuilder[HostID]
    var lastLoc: Option[Location] = None

    def update(host: HostID, loc: Location) = {
      require(lastLoc forall { _.compare(loc) < 0} )
      lastLoc = Some(loc)
      lb += loc
      hb += host
    }

    def result = {
      val hosts = hb.result()
      // C* marks segments with their right-exclusive tokens; we mark them with left inclusive.
      // Therefore, a C* topology of (t1, h1), (t2, h2), (t3, h3), (t4, h4) means:
      // "h1 until t1, h2 until t2, h3 until t3, h4 until t4, then h1 again until t1".
      // It should become:
      // (t1, h2), (t2, h3), (t3, h4), (t4, h1) in our topology, meaning
      // "h2 from t1, h3 from t2, h4 from t3, h1 from t4, then h2 again from t1".
      // Rotating the host list to the right by one does it.
      lb.result() zip (hosts.drop(1) :+ hosts.head) map { case (l, h) => SegmentOwnership(l, h) }
    }
  }

  def createInitialTopologies(membership: Membership): Map[String, Vector[SegmentOwnership]] = {
    val tm = StorageService.instance.getTokenMetadata.cloneOnlyTokenMap
    val tokenToEndpoint = tm.getNormalAndBootstrappingTokenToEndpointMap

    create(tm.sortedTokens.asScala, { t: Token =>
      Option(tokenToEndpoint.get(t)) match {
        case Some(endpoint) =>
          Option(tm.getHostId(endpoint)) map { HostID(_) } match {
            case Some(hostID) =>
              membership.getReplica(hostID) match {
                case Some(replicaName) =>
                  (t.asInstanceOf[LongToken].getTokenValue, hostID, replicaName)
                case None =>
                  throw new IllegalArgumentException(s"No membership information for $hostID (for endpoint $endpoint for token $t)")
              }
            case None =>
              throw new IllegalStateException(s"No Host ID for endpoint $endpoint (for token $t)")
          }
        case None =>
          throw new IllegalStateException(s"No endpoint for token $t")
      }
    })
  }

  // This method is mostly public for testing; normally use createInitialTopologies
  def create[T](tokens: Iterable[T], dataFn: T => (Long, HostID, String)): Map[String, Vector[SegmentOwnership]] = {
    val ringBuilders = MMap.empty[String, RingBuilder]

    tokens foreach { t =>
      val (longToken, hostID, replicaName) = dataFn(t)
      ringBuilders
        .getOrElseUpdate(replicaName, new RingBuilder)
        .update(hostID, Location(longToken))
    }

    val result = ringBuilders.toMap.map { case (r, rb) => (r, rb.result) }
    require(result.values.flatten.size == tokens.size)
    result
  }
}
