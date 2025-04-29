package fauna.flags

import fauna.atoms.{ AccountID, HostID }
import fauna.codex.cbor._
import fauna.lang.syntax._

sealed abstract class PropertiesCompanion[ID: CBOR.Encoder, P <: Properties[ID]] {

  private val PropertiesKey = "properties".toUTF8Buf.asReadOnly

  /** Properties are encoded in CBOR as pairs of id -> values, with no
    * container. This allows them to be used as key/value pairs in a
    * Map. See PropertiesVecCodec.
    */
  private final class PropertiesEncoder(
    implicit idCodec: CBOR.Encoder[ID],
    valCodec: CBOR.Encoder[Map[String, Value]])
      extends CBOR.Encoder[P] {

    def encode(stream: CBOR.Out, prop: P): CBOR.Out = {
      idCodec.encode(stream, prop.id)
      stream.writeMapStart(1)
      stream.writeString(PropertiesKey)
      valCodec.encode(stream, prop.values)
      stream
    }
  }

  implicit def PropertiesVecEncoder =
    new PropertiesVecEncoder(new PropertiesEncoder)

  /** This codec encodes a Vector of Properties as a Map, wherein each
    * Property's id and values are pairs.
    *
    * This somewhat unusual encoding allows for batches in the client
    * to look like:
    *
    *     [ prop1, prop2, ... ]
    *
    * And the wire protocol to look like:
    *
    *     {
    *       prop1.id: {
    *         "properties": prop1.values
    *       },
    *       prop2.id: {
    *         "properties": prop2.values
    *       },
    *       ...
    *     }
    */
  final class PropertiesVecEncoder(propCodec: CBOR.Encoder[P])
      extends CBOR.Encoder[Vector[P]] {

    def encode(stream: CBOR.Out, props: Vector[P]): CBOR.Out = {
      stream.writeMapStart(props.size)
      val iter = props.iterator
      while (iter.hasNext) {
        val p = iter.next()
        propCodec.encode(stream, p)
      }
      stream
    }
  }

  def apply(id: ID, values: Map[String, Value]): P
}

object Properties {

  /** This prefix is appended to all base properties passed to account flags rule evaluation.
    * For example, the base prop "id" will be exposed to account rules as "account.id"
    */
  val AccountPrefix = "account"

  /** The key within an AccountProperties object which should contain
    * a customer's account ID as a LongValue.
    */
  val AccountID = "account_id"

  /** The key within a HostProperties object which should contain
    * a HostID as a StringValue.
    */
  val HostID = "host_id"

  /** The name of the cluster a host belongs to, as configured with
    * `cluster_name` in CoreConfig.
    */
  val ClusterName = "cluster_name"

  /** The name of the replica a host belongs to, as configured when a
    * host joins a cluster.
    *
    * See Membership.
    */
  val ReplicaName = "replica_name"

  /** The name of the region group a host's cluster belongs to, as
    * configured with `cluster_region_group` in CoreConfig.
    */
  val RegionGroup = "region_group"

  /** The name of the environment a host's cluster belongs to, as
    * configured with `cluster_environment` in CoreConfig.
    */
  val Environment = "environment"
}

/** A bag of properties, represented as key/value pairs.
  *
  * Each bag is associated with a unique identifier, such as an
  * account ID or host ID, which can be used to correlate requests
  * (responses) to the feature flags service to their responses
  * (requests).
  */
sealed abstract class Properties[ID] {

  def id: ID
  def values: Map[String, Value]
}

object AccountProperties extends PropertiesCompanion[AccountID, AccountProperties]

case class AccountProperties(id: AccountID, values: Map[String, Value])
    extends Properties[AccountID]

object HostProperties extends PropertiesCompanion[HostID, HostProperties]

case class HostProperties(id: HostID, values: Map[String, Value])
    extends Properties[HostID]
