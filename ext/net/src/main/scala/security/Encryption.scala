package fauna.net.security

/**
  * Encryption configuration for the network stack.
  */
object Encryption {
  val Default = Cleartext

  sealed abstract class Level {
    def sslConfig: SSLConfig
  }

  // no traffic is encrypted
  case object Cleartext extends Level {
    def sslConfig = NoSSL
  }

  sealed abstract class Secure(
    key: Option[KeySource],
    trust: Option[TrustSource],
    ciphers: List[String])
      extends Level {

    def sslConfig =
      SSLConfig(key map { ServerSSL(_, trust, true, ciphers) },
                ClientSSL(key, trust, true, ciphers))
  }

  // traffic within a replica is in the clear, across replicas is
  // encrypted.
  case class Replica(k: Option[KeySource], t: Option[TrustSource], c: List[String])
      extends Secure(k, t, c)

  // all connections are encrypted
  case class All(k: Option[KeySource], t: Option[TrustSource], c: List[String])
      extends Secure(k, t, c)
}
