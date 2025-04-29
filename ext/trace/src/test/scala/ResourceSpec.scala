package fauna.trace.test

import fauna.trace.Resource
import io.netty.util.AsciiString

class ResourceSpec extends Spec {
  "Resource" - {
    "merge prefers primary" in {
      val ip = new AsciiString("127.0.0.1")
      val hostname = new AsciiString("localhost")

      val primary = Resource(Resource.Labels.HostName -> ip)
      val secondary = Resource(
        Resource.Labels.HostName -> hostname,
        Resource.Labels.HostHostname -> hostname)

      val attrs = primary.merge(secondary).toSeq

      attrs should contain ((Resource.Labels.HostName, ip))
      attrs shouldNot contain ((Resource.Labels.HostName, hostname))
      attrs should contain ((Resource.Labels.HostHostname, hostname))
    }
  }
}
