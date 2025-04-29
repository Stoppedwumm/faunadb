package fauna.net

import fauna.atoms._
import fauna.codex.cbor._
import java.net.{ InetAddress, InetSocketAddress }
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

object HostAddress {
  def apply(hostPort: String): HostAddress = hostPort.split(":") match {
    case Array(host, portStr) => HostAddress(host, portStr.toInt)
    case _ => throw new IllegalArgumentException("Host string must be of the format host:port")
  }

  private val LegacyCodec = {
    def to(id: HostAddress) = (id.address.getAddress, id.port)
    def from(t: (Array[Byte], Int)) = HostAddress(InetAddress.getByAddress(t._1).getHostAddress, t._2)
    CBOR.AliasCodec(from, to)
  }

  private val CBORCodec = {
    def to(id: HostAddress) = (id.hostName, id.port)
    def from(t: (String, Int)) = HostAddress(t._1, t._2)
    CBOR.AliasCodec(from, to)
  }

  // TODO: remove when it's safe to
  implicit val BridgeCodec = new CBOR.Codec[HostAddress] {
    def decode(stream: CBOR.In): HostAddress =
      Try(CBORCodec.decode(stream.copy(stream.buf.duplicate))) match {
        case Success(ha) =>
          stream.skip()
          ha

        case Failure(CBORUnexpectedTypeException("Byte String", "UTF8 String")) =>
          LegacyCodec.decode(stream)

        case Failure(err) =>
          throw err
      }

    def encode(stream: CBOR.Out, ha: HostAddress): CBOR.Out =
      CBORCodec.encode(stream, ha)
  }
}

case class HostAddress(hostName: String, port: Int) extends Ordered[HostAddress] {
  val sockaddr = new InetSocketAddress(InetAddress.getByName(hostName), port)
  def address = sockaddr.getAddress

  def compare(that: HostAddress) = {
    val hostNameCmp = hostName.compareTo(that.hostName)
    if (hostNameCmp != 0) hostNameCmp else port - that.port
  }

  override def equals(o: Any): Boolean = o match {
    case ha: HostAddress =>
      (ha.sockaddr == sockaddr || ha.hostName == hostName) && ha.port == port
    case _ => false
  }
}

case class HostDest(toEither: Either[HostAddress, HostID]) extends AnyVal {

  override def toString =
    toEither match {
      case Right(a) => a.toString
      case Left(id) => id.toString
    }
}

object HostDest {
  implicit def asDest(id: HostID) = HostDest(Right(id))
  implicit def asDest(addr: HostAddress) = HostDest(Left(addr))

  implicit val CBORCodec =
    CBOR.AliasCodec[HostDest, Either[HostAddress, HostID]](HostDest(_), _.toEither)
}

case class HostInfo(id: HostID, address: HostAddress)

object HostInfo {
  implicit val CBORCodec = CBOR.TupleCodec[HostInfo]
}
