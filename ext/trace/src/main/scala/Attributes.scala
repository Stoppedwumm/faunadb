package fauna.trace

import io.netty.util.AsciiString

object Attributes {

  val Component = AsciiString.cached("component")

  /** Pre-allocated attributes for customer meta-data. */
  object Customer {
    val AccountID = AsciiString.cached("customer.account.id")
  }

  /**
    * Pre-allocated attributes for HTTP communication.
    */
  object HTTP {
    // The IP address of the original client behind all proxies, if
    // known (e.g. from X-Forwarded-For). Note that this is not
    // necessarily the same as net.peer.ip, which would identify the
    // network-level peer, which may be a proxy.
    val ClientIP = AsciiString.cached("http.client_ip")

    // Kind of HTTP protocol used: "1.0", "1.1", "2", "SPDY" or
    // "QUIC".
    val Flavor = AsciiString.cached("http.flavor")

    // The value of the HTTP host header. When the header is empty or
    // not present, this attribute should be the same.
    val Host = AsciiString.cached("http.host")

    // HTTP request method. E.g. "GET".
    val Method = AsciiString.cached("http.method")

    // The URI scheme identifying the used protocol: "http" or "https"
    val Scheme = AsciiString.cached("http.scheme")

    // The primary server name of the matched virtual host. This
    // should be obtained via configuration. If no such configuration
    // can be obtained, this attribute MUST NOT be set ( net.host.name
    // should be used instead).
    val ServerName = AsciiString.cached("http.server_name")

    // HTTP response status code. E.g. 200 (integer)
    val StatusCode = AsciiString.cached("http.status_code")

    // The full request target as passed in a HTTP request line or
    // equivalent, e.g. /path/12314/?q=ddds#123".
    val Target = AsciiString.cached("http.target")

    // Value of the HTTP User-Agent header sent by the client.
    val UserAgent = AsciiString.cached("http.user_agent")

    val V10 = AsciiString.cached("1.0")
    val V11 = AsciiString.cached("1.1")
    val V2 = AsciiString.cached("2")
    val VUnknown = AsciiString.cached("Unknown")
  }

  /**
    * Pre-allocated attributes for network communication.
    */
  object Net {
    val Transport = AsciiString.cached("transport")
    val TCP = AsciiString.cached("IP.TCP")

    // Signals that there is only in-process communication not using a
    // "real" network protocol in cases where network attributes would
    // normally be expected. Usually all other network attributes can
    // be left out in that case.
    val InProcess = AsciiString.cached("inproc")

    val HostIP = AsciiString.cached("net.host.ip")
    val HostPort = AsciiString.cached("net.host.port")
    val HostName = AsciiString.cached("net.host.name")

    val PeerIP = AsciiString.cached("net.peer.ip")
    val PeerPort = AsciiString.cached("net.peer.port")
    val PeerName = AsciiString.cached("net.peer.name")

    val SignalID = AsciiString.cached("signal_id")
  }

  /**
    * Pre-allocated attributes for thread information.
    */
  object Thread {
    val ID = AsciiString.cached("thread.id")
    val Name = AsciiString.cached("thread.name")
  }

  object HealthCheck {
    val BackOff = AsciiString.cached("healthcheck.backoff")
  }
}
