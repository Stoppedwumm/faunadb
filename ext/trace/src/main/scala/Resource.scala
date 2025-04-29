package fauna.trace

import io.netty.util.AsciiString

object Resource {
  val Empty = new Resource(Map.empty[AsciiString, AsciiString])

  def apply(attributes: (AsciiString, CharSequence)*): Resource =
    // null values are defaults in CoreConfig; filter them out.
    new Resource(Map(attributes collect {
      case (k, v) if v != null => k -> new AsciiString(v)
    }: _*))

  /**
    * Pre-defined labels according to the OpenTelemetry semantic specification:
    * https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/data-resource-semantic-conventions.md
    */
  object Labels {
    /**
      * Name of the cloud provider.
      * Example values are aws, azure, gcp.
      */
    val CloudProvider = AsciiString.cached("cloud.provider")

    /**
      * The cloud account id used to identify different entities.
      */
    val CloudAccountID = AsciiString.cached("cloud.account.id")

    /**
      * A specific geographical location where different entities can
      * run.
      */
    val CloudRegion = AsciiString.cached("cloud.region")

    /**
      * Zones are a sub set of the region connected through
      * low-latency links.
      */
    val CloudZone = AsciiString.cached("cloud.zone")

    /**
      * Hostname of the host.
      *
      * It contains what the hostname(1) command returns on the host
      * machine.
      */
    val HostHostname = AsciiString.cached("host.hostname")

    /**
      * Unique host id.
      *
      * For Cloud this must be the instance_id assigned by the cloud
      * provider.
      */
    val HostID = AsciiString.cached("host.id")

    /**
      * Name of the host.
      *
      * It may contain what hostname(1) returns, the fully qualified
      * domain name, or a name specified by the user.
      */
    val HostName = AsciiString.cached("host.name")

    /**
      * Type of host.
      *
      * For Cloud this must be the machine type.
      */
    val HostType = AsciiString.cached("host.type")

    /**
      * Name of the VM image or OS install the host was instantiated from.
      */
    val HostImageName = AsciiString.cached("host.image.name")

    /**
      * VM image id. For Cloud, this value is from the provider.
      */
    val HostImageID = AsciiString.cached("host.image.id")

    /**
      * The version string of the VM image.
      */
    val HostImageVersion = AsciiString.cached("host.image.version")

    /**
      * Logical name of the service. Always "faunadb".
      */
    val ServiceName = AsciiString.cached("service.name")

    /**
      * A namespace for service.name. Always the value of
      * "network_cluster_name" or "cluster_name" from CoreConfig.
      */
    val ServiceNamespace = AsciiString.cached("service.namespace")

    /**
      * The string ID of the service instance. Always the assigned
      * HostID of this host.
      */
    val ServiceInstanceID = AsciiString.cached("service.instance.id")

    /**
      * The version string of the service API or implementation.
      * Always the BuildVersion of this host.
      */
    val ServiceVersion = AsciiString.cached("service.version")
  }
}

final class Resource private
  (private val attributes: Map[AsciiString, AsciiString])
    extends Iterable[(AsciiString, AsciiString)] {

  /**
    * Merges `secondary` with this Resource, according to the rules
    * defined here:
    * https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/sdk-resource.md#merge
    */
  def merge(secondary: Resource): Resource =
    new Resource(secondary.attributes ++ attributes)

  /**
    * Alias for merge().
    */
  def +(other: Resource): Resource = this merge other

  def iterator: Iterator[(AsciiString, AsciiString)] =
    attributes.iterator
}
