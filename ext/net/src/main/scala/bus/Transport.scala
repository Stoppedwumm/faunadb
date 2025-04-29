package fauna.net.bus.transport

import io.netty.buffer.Unpooled
import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.net.HostAddress
import fauna.net.util.EncodedTimeBound
import fauna.trace.TraceContext

/**
  * TransportClient and TransportServer are responsible for managing
  * underlying node-to-node connections. The core of the internode
  * protocol is a version negotiation hand-shake, which allows
  * seamless transport protocol version upgrades in the future. The
  * connection handshake is as follows:
  *
  * - Client initiates a connection to a host, using TLS if
  *   configured.
  *
  * - Client sends its min and max supported transport versions as two
  *   ints in network byte order.
  *
  * - If the version range is supported by the server, the server
  *   responds with the version to be used for the connection. If the
  *   version range is not supported, the server responds with the
  *   version -1 and closes the connection. Version is encoded as an
  *   int in network byte order.
  *
  * - If the client receives version -1, it closes the connection and
  *   reports an error upstream.
  *
  * - Messages in V0 of the protocol are CBOR encoded bytes, prepended
  *   with a network order int length.
  *
  * - Client responds with a Client Session Info message, which in V0
  *   is its cluster ID, virtual host ID, and host address.
  *
  * - Server responds with a Server Session Info message, which in V0
  *   is its cluster ID, virtual host ID, and a list of aliases.
  *
  * - If the cluster IDs of the client and server do not match, the
  *   client closes the connection and reports an error upstream.
  *
  * - Client selects the first compatible alias from the list
  *   received (the preferred alias). If it is not connected to the
  *   preferred alias, it closes the connection to the server and
  *   attempts to connect to the alias, and renegotiate the connection.
  */

object Transport {
  val VersionError = -1

  private[this] val buf = Unpooled.unreleasableBuffer("start3".toUTF8Buf)
  def TCPStreamStart = buf.duplicate()
}

object Message {
  implicit val codec = CBOR.SumCodec[Message](
    CBOR.TupleCodec[MessageHeader],
    CBOR.TupleCodec[MessageFrame])
}

sealed abstract class Message

/**
  * All V2 streams begin with a MessageHeaderV2, which establishes the
  * parameters of the stream between Client and Server. Any further
  * data transmitted for the same signalID/streamID combination MUST
  * be sent prefixed with MessageFrameV2.
  *
  * A stream is framed into fixed-length chunks, each of which is
  * assigned a 1-based sequence number. Frames must be sent and
  * received in order.
  *
  * @param signalID well-known address of the service sending/receiving this stream. globally unique.
  * @param streamID temporary address identifying this specific stream. unique per signalID.
  * @param msgCount total number of frames composing this stream, including the header frame
  * @param trace optional identifying distributed tracing information
  * @param deadline duration after which this stream must either complete or close
  */
case class MessageHeader(
  signalID: SignalID,
  streamID: Int,
  msgCount: Int,
  trace: Option[TraceContext],
  deadline: EncodedTimeBound) extends Message

/**
  * A continuation of a stream established by
  * MessageHeaderV2. `msgNum` is the 1-based sequence number of the
  * frame following this header.
  */
case class MessageFrame(
  signalID: SignalID,
  streamID: Int,
  msgNum: Int) extends Message

object ClientSessionInfoV2 {
  implicit val Codec = CBOR.TupleCodec[ClientSessionInfoV2]
}
case class ClientSessionInfoV2(cluster: String, hostID: HostID, hostAddress: HostAddress)

object ClientSessionInfoV3 {
  implicit val Codec = CBOR.TupleCodec[ClientSessionInfoV3]
}
case class ClientSessionInfoV3(
  cluster: String,
  hostID: HostID,
  hostAddress: HostAddress,
  tcpStreamID: Option[SignalID])

object Alias {
  implicit val Codec = CBOR.TupleCodec[Alias]
}
case class Alias(hostAddress: HostAddress, variant: String, zone: Option[String])

object ServerSessionInfo {
  implicit val Codec = CBOR.TupleCodec[ServerSessionInfo]
}
case class ServerSessionInfo(cluster: String, hostID: HostID, aliases: Vector[Alias])

case class UnexpectedTransportException(msg: String) extends RuntimeException(msg)

case class InvalidClusterIDException(localCluster: String, remoteCluster: String, remoteAddress: HostAddress) extends Exception(
  s"Remote $remoteAddress belongs to cluster $remoteCluster. (Local cluster is $localCluster.)")

case class UnsupportedTransportVersionRange(hostAddress: HostAddress, min: Int, max: Int) extends Exception(
  s"$hostAddress not compatible with MessageBus Transport version range ${min}-${max}.")

case class UnsupportedTCPStream(hostAddress: HostAddress) extends Exception(
  s"$hostAddress does not support direct TCP stream connections.")

case class TCPStreamOpenFailure(host: Either[HostAddress, HostID]) extends Exception(
  s"Failed to open TCP stream to ${host.fold(_.toString, _.toString)}.")
