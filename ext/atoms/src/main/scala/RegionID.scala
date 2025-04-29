package fauna.atoms

import fauna.codex.cbor.CBOR

object RegionID {
  /**
    * ID of the default region. Default region always exists and is the region
    * in which the root database, one with {@link ScopeID#RootID} is found.
    * When a cluster is initialized, its first member must be in the default
    * region.
    */
  val DefaultID = RegionID(0)

  implicit val CBORCodec = CBOR.TupleCodec[RegionID]
}

/**
  * Represents the ID of a region group. For simplified term usage, within
  * the code we use the term "region" to denote what externally we communicate
  * as "region group". Externally we use the term "region group" to avoid
  * confusion with cloud provider regions. While each region has a unique name
  * within the cluster (see RegionName), they also have a numeric ID
  * automatically assigned to them by Membership. This ID is mainly used to
  * identify regions in the transaction pipeline.
  * @param toInt the numeric ID of the region.
  */
case class RegionID(toInt: Int) extends AnyVal with Ordered[RegionID] {
  def compare(that: RegionID): Int =
    Integer.compare(toInt, that.toInt)

  def max(o: RegionID) = if (toInt >= o.toInt) this else o

  def next = RegionID(toInt + 1)
}

object RegionName {
  private val Separator = '/'
  private val DefaultNameString = ""

  /**
    * Name of the default region. The default region's name is "".
    */
  val DefaultName = RegionName(DefaultNameString)

  /**
    * Extracts a region name from a replica name. If the replica has a forward
    * slash character in its name, the region name is the prefix of the replica
    * name before the last forward slash. If the replica has no slash character
    * in its name, its region name is {@link #DefaultName}.
    * @param replicaName the name of the replica
    * @return the name of the region for the replica.
    */
  def fromReplicaName(replicaName: String): RegionName = {
    val i = replicaName.lastIndexOf(Separator)
    if (i == -1) {
      DefaultName
    } else {
      RegionName(replicaName.substring(0, i))
    }
  }

  implicit val CBORCodec = CBOR.TupleCodec[RegionName]
}

/**
  * Represents the name of a region. Region names are unique within a cluster.
  * Replica names encode the names of their regions, see
  * {@link RegionName#fromReplicaName(String)}.
  * @param name the string representing the region name.
  */
case class RegionName(name: String) extends AnyVal {
  import RegionName._

  /**
    * @return true if this name describes the default region.
    */
  def isDefault: Boolean =
    name == DefaultNameString

  /**
    * Returns true if the region named by this RegionName object contains the
    * specified replica, that is, if {@link RegionName#fromReplicaName(String)}
    * would return this region name for the replica.
    * @param replicaName the replica
    * @return true if the replica is in the region
    */
  def containsReplica(replicaName: String): Boolean =
    this == fromReplicaName(replicaName)

  /**
    * Returns a string useful for displaying the name of this region in
    * English language, mostly as part of log messages and such.
    * @return the display name for this region.
    */
  def displayName: String =
    if (isDefault) {
      "default region"
    } else {
      s"region $name"
    }
}
