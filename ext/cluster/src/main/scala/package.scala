package fauna

import fauna.atoms.Location

package cluster {
  /**
    * Thrown when no replicas exist for `Location`, indicating a
    * catastrophic configuration error.
    */
  case class ClusterTopologyException(loc: Location)
      extends Exception(s"No configured replica for location: $loc")
}
