package fauna.net.test

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec._
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.net.bus.transport._
import fauna.net.security.{ KeySource, NoSSL, SSL, TrustSource }
import fauna.net.HostAddress
import fauna.trace._
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, Unpooled }
import java.nio.channels.FileChannel
import java.nio.file._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.LongAdder
import java.util.UUID
import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Random

class MessageBusSpec extends Spec {

  val exporter = new MemoryExporter
  val tracer = new SamplingTracer(Sampler.Default)

  tracer.addExporter(exporter)

  GlobalTracer.registerIfAbsent { (None, tracer) }

  implicit val ec = NamedThreadPoolExecutionContext("bus")

  before {
    // uncomment to enable console log trace messages
    // fauna.logging.test.setLogTrace()
    exporter.reset()
  }

  val pem = testPEMFile("private_key1.pem")

  private def startAndRegister(buses: MessageBus*) = {
    buses foreach { _.start() }
    buses foreach { b1 =>
      buses foreach { b2 => b1.registerHostID(b2.hostID, b2.hostAddress) }
    }
  }

  def newMessageBus(
    zone: String = null,
    useSSL: Boolean = true,
    keepaliveInterval: FiniteDuration = 1.second,
    keepaliveTimeout: FiniteDuration = 5.seconds) = {
    val ssl = if (useSSL) SSL(KeySource(pem), TrustSource(pem), true) else NoSSL
    val sslPort = findFreePort()
    val clearPort = findFreePort()
    log.debug(s"SSL Port: $sslPort")
    log.debug(s"Clear port: $clearPort")

    MessageBus(
      "test cluster",
      HostID.randomID,
      "localhost",
      None,
      Option(zone),
      ssl,
      clearPort,
      sslPort,
      keepaliveInterval = keepaliveInterval,
      keepaliveTimeout = keepaliveTimeout
    )
  }

  "MessageBus" - {
    "slices messages that are too large" in {
      val bus1 = MessageBus(
        "test cluster",
        HostID.randomID,
        "localhost",
        maxMessageSize = 4,
        port = findFreePort(),
        sslPort = findFreePort())

      val bus2 = newMessageBus(useSSL = false)

      startAndRegister(bus1, bus2)

      val count = new LongAdder

      val string = "abcdefghijklmnopqrstuvwxyz"
      @volatile var result: String = ""

      val protocol = Protocol[String]("test")

      val handler = bus1.tempHandler(protocol) { (_, msg, _) =>
        count.increment
        result = msg
        Future.unit
      }

      val sink = bus2.sink(protocol, handler.id)
      sink.send(string)

      eventually { count.sum should equal(1) }
      result should equal(string)

      bus1.stop()
      bus2.stop()
    }

    "serialization" - {
      def buf(bytes: Byte*) = Unpooled.wrappedBuffer(bytes.toArray)

      val id = HostID(new UUID(123, 456))
      val addr = HostAddress("127.0.0.2", 123)
      val alias1 = Alias(addr, "foo", Some("dc1"))
      val alias2 = Alias(addr, "bar", None)

      "ClientSessionInfoV2" in {
        val info = ClientSessionInfoV2("foo", id, addr)
        val bytes = buf(-125, 99, 102, 111, 111, -126, 24, 123, 25, 1, -56, -126,
          105, 49, 50, 55, 46, 48, 46, 48, 46, 50, 24, 123)
        CBOR.encode(info) should equal(bytes)
        CBOR.decode[ClientSessionInfoV2](bytes) should equal(info)

        val legacy = buf(-125, 99, 102, 111, 111, -126, 24, 123, 25, 1, -56, -126,
          68, 127, 0, 0, 2, 24, 123)
        CBOR.decode[ClientSessionInfoV2](legacy) should equal(info)
      }

      "ClientSessionInfoV3" in {
        val info = ClientSessionInfoV3("foo", id, addr, None)
        val bytes = buf(-124, 99, 102, 111, 111, -126, 24, 123, 25, 1, -56, -126,
          105, 49, 50, 55, 46, 48, 46, 48, 46, 50, 24, 123, -10)
        CBOR.encode(info) should equal(bytes.duplicate)
        CBOR.decode[ClientSessionInfoV3](bytes) should equal(info)

        val withStream = ClientSessionInfoV3("foo", id, addr, Some(SignalID(123)))
        val withStreamBytes = buf(-124, 99, 102, 111, 111, -126, 24, 123, 25, 1, -56,
          -126, 105, 49, 50, 55, 46, 48, 46, 48, 46, 50, 24, 123, 24, 123)
        CBOR.decode[ClientSessionInfoV3](withStreamBytes) should equal(withStream)
      }

      "Alias" in {
        val bytes1 = buf(-125, -126, 105, 49, 50, 55, 46, 48, 46, 48, 46, 50, 24,
          123, 99, 102, 111, 111, 99, 100, 99, 49)
        CBOR.encode(alias1) should equal(bytes1.duplicate)
        CBOR.decode[Alias](bytes1) should equal(alias1)

        val legacy1 = buf(-125, -126, 68, 127, 0, 0, 2, 24, 123, 99, 102, 111, 111,
          99, 100, 99, 49)
        CBOR.decode[Alias](legacy1) should equal(alias1)

        val bytes2 = buf(-125, -126, 105, 49, 50, 55, 46, 48, 46, 48, 46, 50, 24,
          123, 99, 98, 97, 114, -10)
        CBOR.encode(alias2) should equal(bytes2.duplicate)
        CBOR.decode[Alias](bytes2) should equal(alias2)

        val legacy2 =
          buf(-125, -126, 68, 127, 0, 0, 2, 24, 123, 99, 98, 97, 114, -10)
        CBOR.decode[Alias](legacy2) should equal(alias2)
      }

      "ServerSessionInfo" in {
        val info = ServerSessionInfo("foo", id, Vector(alias1, alias2))
        val bytes = buf(-125, 99, 102, 111, 111, -126, 24, 123, 25, 1, -56, -126,
          -125, -126, 105, 49, 50, 55, 46, 48, 46, 48, 46, 50, 24, 123, 99, 102, 111,
          111, 99, 100, 99, 49, -125, -126, 105, 49, 50, 55, 46, 48, 46, 48, 46, 50,
          24, 123, 99, 98, 97, 114, -10)

        CBOR.encode(info) should equal(bytes.duplicate)
        CBOR.decode[ServerSessionInfo](bytes) should equal(info)

        val legacy = buf(-125, 99, 102, 111, 111, -126, 24, 123, 25, 1, -56, -126,
          -125, -126, 68, 127, 0, 0, 2, 24, 123, 99, 102, 111, 111, 99, 100, 99, 49,
          -125, -126, 68, 127, 0, 0, 2, 24, 123, 99, 98, 97, 114, -10)

        CBOR.decode[ServerSessionInfo](legacy) should equal(info)
      }
    }

    "basics" - {
      val bus1 = newMessageBus()
      val bus2 = newMessageBus()

      startAndRegister(bus1, bus2)

      "sends and receives a local message" in {
        @volatile var result: String = "none yet"

        val protocol = Protocol[String]("test")

        val handler = bus1.tempHandler(protocol) { (_, msg, _) =>
          tracer.withSpan("result") {
            Future {
              result = msg
            }
          }
        }

        val sink = bus1.sink(protocol, handler.id)

        val span = tracer
          .buildSpan("sink")
          .enableSampling()
          .start()

        val scope = tracer.activate(span)

        try {
          sink.send("received")

          eventually { result should equal("received") }
        } finally {
          scope foreach { _.close() }
        }

        exporter.finishedSpans map { _.name.toString } should contain("result")
      }

      "local sends release bytebuf refcounts correctly" in {
        @volatile var result: Option[ByteBuf] = None

        val expected = ByteBufAllocator.DEFAULT.directBuffer(4)
        expected.setByte(0, 42)

        expected.refCnt() should equal(1)

        val protocol = Protocol[ByteBuf]("test")
        val handler = bus1.tempHandler(protocol) { (_, msg, _) =>
          result = Some(msg)
          Future.unit
        }
        val sink = bus1.sink(protocol, handler.id)

        sink.send(expected)

        eventually {
          /* The refcount contract with messagebus handlers is: when a handler
           * receives a message, the message is released once the handler’s future
           * completes. Messages the handler wishes to preserve beyond the lifecycle
           * of the handler call should be retained. */
          expected.refCnt() should equal(0)
        }
      }

      "sends and receives a message" in {
        @volatile var result: String = "none yet"

        val protocol = Protocol[String]("test")

        val handler = bus1.tempHandler(protocol) { (_, msg, _) =>
          result = msg
          Future.unit
        }

        val sink = bus2.sink(protocol, handler.id)

        sink.send("received")

        eventually { result should equal("received") }
      }

      "drops messages to mismatched hostIDs" in {
        @volatile var result: String = "none yet"

        val protocol = Protocol[String]("test")

        val handler = bus1.tempHandler(protocol) { (_, msg, _) =>
          result = msg
          Future.unit
        }

        val sink = bus2.sink(protocol, handler.id.copy(host = HostID.randomID))

        sink.send("received")

        Thread.sleep(100)
        result should not equal ("received")
      }

      "requests" - {
        val protocol = Protocol[(String, SignalID)]("request")
        implicit val replyProtocol =
          Protocol.Reply[(String, SignalID), String]("reply")

        val handler = bus1.tempHandler(protocol) { case (from, (msg, replyTo), _) =>
          Thread.sleep(300)
          bus1.sink(replyProtocol, replyTo.at(from)).send(msg.toUpperCase).unit
        }

        val sink = bus2.sink(protocol, handler.id)

        "makes requests" in {
          val res = sink.request(("hello", _), 10.seconds.bound)
          Await.result(res, 10.seconds) should equal(Some("HELLO"))
        }

        "requests time out" in {
          val res = sink.request(("hello", _), 10.milliseconds.bound)
          Await.result(res, 10.seconds) should equal(None)
        }
      }

      "requests twice" in {
        case class Syn(msg: String, replyTo: SignalID)
        case class Ack(msg: String, replyTo: SignalID)
        case class SynAck(msg: String)

        implicit val SynCodec = CBOR.TupleCodec[Syn]
        implicit val AckCodec = CBOR.TupleCodec[Ack]
        implicit val SynAckCodec = CBOR.TupleCodec[SynAck]

        val synProtocol = Protocol[Syn]("syn")
        implicit val ackProtocol = Protocol.Reply[Syn, Ack]("ack")
        implicit val synackProtocol = Protocol.Reply[Ack, SynAck]("synack")

        @volatile var result = SynAck("none yet")

        val handler = bus1.tempHandler(synProtocol) {
          case (from, Syn(msg, replyTo), _) =>
            bus1
              .sink(ackProtocol, replyTo.at(from))
              .request(Ack(msg.toUpperCase, _), 10.seconds.bound) map { r2 =>
              r2 foreach { result = _ }
            }
        }

        val sink = bus2.sink(synProtocol, handler.id)

        sink.request(Syn("hello", _), 10.seconds.bound) foreach { r1 =>
          val Ack(str, replyTo) = r1.get
          val reversed = new StringBuilder(str).reverse.toString
          bus2
            .sink(synackProtocol, replyTo.at(sink.dest.host))
            .send(SynAck(reversed))
        }

        Thread.sleep(500)
        result.msg should equal("OLLEH")
      }

      "connects with tcp streams" in {
        val p1 = Promise[String]()
        val handler1 = bus1.tempTCPStreamHandler { (_, ch) =>
          ch.writeAndFlush(("hello, client!").toUTF8Buf)
          ch.close()
          Future.unit
        }

        val ch1 = Await.result(bus2.openTCPStream(handler1.id, "test"), 1.second)

        ch1.recv() foreach { buf =>
          p1.success(buf.toUTF8String)
          buf.release()
          ch1.close()
        }

        Await.result(p1.future, 1.seconds) should equal("hello, client!")

        val p2 = Promise[String]()
        val handler2 = bus1.tempTCPStreamHandler { (_, ch) =>
          ch.recv() map { buf =>
            p2.success(buf.toUTF8String)
            buf.release()
            ch.close()
          }
          Future.unit
        }

        val ch2 = Await.result(bus2.openTCPStream(handler2.id, "test"), 1.second)
        ch2.writeAndFlush(("hello, server!").toUTF8Buf)
        ch2.close()

        Await.result(p2.future, 1.seconds) should equal("hello, server!")
      }

      "tcp backpressure" in {
        val gate = Promise[Unit]()
        val done = Promise[Unit]()
        val handler = bus1.tempTCPStreamHandler { (_, ch) =>
          ch.enableChannelSyntax()

          def loop(): Future[Unit] =
            ch.recv() flatMap { buf =>
              buf.release()
              if (!done.isCompleted) {
                loop()
              } else {
                ch.close()
                Future.unit
              }
            }

          gate.future flatMap { _ => loop() }
        }

        val ch = Await.result(bus2.openTCPStream(handler.id, "test"), 1.second)
        val msg = Unpooled.wrappedBuffer(new Array[Byte](1000))

        while (ch.isWritable) {
          Await.result(ch.sendAndFlush(msg.retainedSlice()), 10.millis)
        }

        // blocked on writing
        val blockedWrite = ch.sendAndFlush(msg)
        assert(blockedWrite.value.isEmpty)

        gate.success(())

        // channel is drained, should be able to write again.
        Await.result(blockedWrite, 10.seconds)

        done.success(())
      }
    }

    "handles failed connects" in {
      val bus1 = newMessageBus(useSSL = false)
      val bus2 = newMessageBus(useSSL = false)

      val count = new LongAdder
      val handler = bus1.tempHandler(Protocol[String]("tmp")) { (_, _, _) =>
        count.increment
        Future.unit
      }

      Await.result(
        bus2.send(handler.id.signalID, bus1.hostAddress, "", "test"),
        10.seconds)
      bus1.start()

      Await.result(
        bus2.send(handler.id.signalID, bus1.hostAddress, "", "test"),
        10.seconds)
      eventually { count.sum should equal(1) }

      bus1.stop()
    }

    "negotiates alternative transports" in {
      val bus1 = newMessageBus(zone = "A")
      val bus2 = newMessageBus(zone = "A")
      val bus3 = newMessageBus(zone = "B")

      startAndRegister(bus1, bus2, bus3)

      val count = new LongAdder

      val handler = bus1.tempHandler(Protocol[String]("tmp")) { (_, _, _) =>
        count.increment
        Future.unit
      }

      bus2.send(handler.id.signalID, bus1.hostAddress, "hi", "test")
      bus3.send(handler.id.signalID, bus1.hostAddress, "hi", "test")

      eventually { count.sum should equal(2) }

      Seq(bus1, bus2, bus3) foreach { _.stop() }
    }

    "handles contention on write" in {
      val bus1 = newMessageBus(useSSL = false)
      val bus2 = newMessageBus(useSSL = false)

      startAndRegister(bus1, bus2)

      val msgCount = 10000
      val msgSize = 1000
      val totalSize = msgSize * msgCount
      val threads = 100
      val msgPerThread = msgCount / threads

      val count = new LongAdder

      val alloc = ByteBufAllocator.DEFAULT
      val handler = bus1.tempHandler(Protocol[ByteBuf]("tmp")) { (_, _, _) =>
        count.increment
        Future.unit
      }

      val start = System.currentTimeMillis

      val futures = (1 to threads) map { _ =>
        Future.delegate {
          def loop(rem: Int): Future[Unit] =
            if (rem == 0) {
              Future.unit
            } else {
              val buf = alloc.buffer(msgSize)

              buf.writerIndex(buf.writerIndex + msgSize)

              bus2.send(handler.id.signalID, bus1.hostAddress, buf, "test") flatMap {
                case true  => loop(rem - 1)
                case false => loop(rem)
              }
            }

          loop(msgPerThread)
        }
      }

      futures foreach {
        Await.result(_, 30.seconds)
      }

      eventually { count.sum should equal(msgCount.toLong) }

      val time = System.currentTimeMillis - start
      val bytesPerSec = totalSize.toDouble / (time.toDouble / 1000) / 1024 / 1024

      info("%d ms, %.3f MB/sec".format(time, bytesPerSec))

      bus1.stop()
      bus2.stop()
    }

    "sinks many messages" in {
      val bus1 = newMessageBus(useSSL = false)
      val bus2 = newMessageBus(useSSL = false)

      startAndRegister(bus1, bus2)

      val totalSize = 1L * 1024 * 1024 * 1024
      val msgSize = 10 * 1024
      val iters = totalSize / msgSize

      assert(iters > 0)

      val count = new LongAdder
      val alloc = ByteBufAllocator.DEFAULT
      val handler = bus1.tempHandler(Protocol[ByteBuf]("tmp")) { (_, _, _) =>
        count.increment
        Future.unit
      }

      val start = System.currentTimeMillis

      val buf = alloc.buffer(msgSize)

      implicit val ec = ExecutionContext.parasitic
      def loop(remaining: Long): Future[Unit] =
        if (remaining == 0) {
          Future.unit
        } else {
          buf.retain
          buf.writerIndex(buf.writerIndex + msgSize)
          bus2.send(handler.id.signalID, bus1.hostAddress, buf, "test") flatMap {
            _ =>
              buf.clear
              loop(remaining - 1)
          }
        }
      Await.result(loop(iters), 30 seconds)

      eventually { count.sum should equal(iters) }

      val time = System.currentTimeMillis - start
      val bytesPerSec = totalSize.toDouble / (time.toDouble / 1000) / 1024 / 1024

      info("%d ms, %.3f MB/sec".format(time, bytesPerSec))

      bus1.stop()
      bus2.stop()

      buf.release
    }

    "many-to-many messages" in {
      val busCount = 5
      val threads = 5

      val buses = (1 to busCount) map { _ => newMessageBus(useSSL = false) }
      startAndRegister(buses: _*)

      val totalSize = 3 * 1024L * 1024 * 1024
      val msgSize = 10 * 1024
      val msgs = totalSize / msgSize / threads * threads
      val msgPerThread = msgs / threads

      val count = new LongAdder
      val realTotalSize = new LongAdder
      val alloc = ByteBufAllocator.DEFAULT
      val handlers = buses map {
        _.tempHandler(Protocol[ByteBuf]("tmp")) { (_, _, _) =>
          count.increment
          Future.unit
        }
      }

      val protocols = handlers map { _.id.signalID }

      val start = System.currentTimeMillis

      val futures = (1 to threads) map { _ =>
        Future.delegate {
          val random = new Random()

          def loop(remaining: Long): Future[Unit] =
            if (remaining == 0) {
              Future.unit
            } else {
              val size = msgSize / 2 + random.nextInt(msgSize)
              realTotalSize.add(size)
              val buf = alloc.buffer(size)
              buf.writerIndex(buf.writerIndex + size)
              val fromIdx = random.nextInt(busCount)
              val toIdx = random.nextInt(busCount)
              buses(fromIdx).send(
                protocols(toIdx),
                buses(toIdx).hostAddress,
                buf,
                "test") flatMap { _ =>
                loop(remaining - 1)
              }
            }

          loop(msgPerThread)
        }
      }

      futures foreach {
        Await.result(_, 30.seconds)
      }

      eventually { count.sum should equal(msgs) }

      val time = System.currentTimeMillis - start
      val bytesPerSec =
        realTotalSize.doubleValue() / (time.toDouble / 1000) / 1024 / 1024

      info("%d ms, %.3f MB/sec".format(time, bytesPerSec))

      buses foreach { _.stop() }
    }

    "keepalive" - {
      "client receives keepalive pings" in {
        val logContents = fauna.logging.test.withFileLogging {
          fauna.logging.test.setLogTrace()

          val bus1 = newMessageBus(keepaliveTimeout = 200.millis)
          val bus2 = newMessageBus(keepaliveInterval = 50.millis)

          startAndRegister(bus1, bus2)

          val protocol = Protocol[String]("test")
          val handler = bus2.tempHandler(protocol) { (_, _, _) => Future.unit }

          // send one message to initiate the connection
          val sink = bus1.sink(protocol, handler.id)
          sink.send("x")

          Thread.sleep(1000)
        }

        // check out the logs
        logContents should include regex "Received keepalive ping from .+"
        logContents should include regex "Sending keepalive ping to .+"
        logContents shouldNot include regex "Closing connection with .+ due to missed keepalive pings"
      }

      "client closes/reconnects when no pings are sent" in {
        val logContents = fauna.logging.test.withFileLogging {

          val bus1 = newMessageBus(keepaliveTimeout = 200.millis)
          val bus2 = newMessageBus(keepaliveInterval = 1.day)

          startAndRegister(bus1, bus2)

          val protocol = Protocol[String]("test")
          val handler = bus2.tempHandler(protocol) { (_, _, _) => Future.unit }

          // send one message to initiate the connection
          val sink = bus1.sink(protocol, handler.id)
          sink.send("x")

          Thread.sleep(1000)
        }

        // check out the logs
        logContents should include regex "Closing connection with .+ due to missed keepalive pings"
      }
    }
  }

  "FileTransferContext" - {
    import StandardOpenOption._

    val tmp = aTestDir()
    val bus1 = newMessageBus()
    val bus2 = newMessageBus()

    startAndRegister(bus1, bus2)

    val tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
    val ft = FileTransferContext(tmpDirectory, transferExpiry = 5.seconds)

    "small, single files" in {
      for (i <- 0 to 1024) {
        val file = tmp / "file"
        val buf = new Array[Byte](i)
        Random.nextBytes(buf)
        Files.write(file, buf, WRITE, APPEND, CREATE)

        val ch = FileChannel.open(file, READ)

        val handle = ft.prepareChannels(bus1, Map("file" -> ch))

        val fut = ft.receive(bus2, handle.manifest) { dir =>
          dir.entries.size should equal(1)
          (s"cmp -s ${dir / "file"} $file" !) should equal(0)
          Future.unit
        }

        Await.result(fut, 10.seconds)
      }
    }

    "sends some files" in {

      val file1 = tmp / "file1"
      val file2 = tmp / "file2"
      val file3 = tmp / "file3"

      val buf = new Array[Byte](1000)

      Seq(file1, file2, file3) foreach { path =>
        1 to 1000 foreach { _ =>
          Random.nextBytes(buf)
          Files.write(path, buf, WRITE, APPEND, CREATE)
        }
      }

      val ch1 = FileChannel.open(file1, READ)
      val ch2 = FileChannel.open(file2, READ)
      val ch3 = FileChannel.open(file3, READ)

      val handle =
        ft.prepareChannels(bus1, Map("file1" -> ch1, "file2" -> ch2, "file3" -> ch3))

      Await.result(
        ft.receive(bus2, handle.manifest) { dir =>
          dir.entries.size should equal(3)
          (s"cmp -s ${dir / "file1"} $file1" !) should equal(0)
          (s"cmp -s ${dir / "file2"} $file2" !) should equal(0)
          (s"cmp -s ${dir / "file3"} $file3" !) should equal(0)
          Future.unit
        },
        30.seconds
      )

      a[FileTransferException] should be thrownBy (Await.result(
        ft.receive(bus2, handle.manifest) { _ =>
          Future.unit
        },
        30.seconds))

      tmp.deleteRecursively()
    }

    "a large file" in {
      val file = tmpDirectory / "large_file"
      val bufSize = 128 * 1024

      try {
        // generate file
        {
          val buf = ByteBuffer.allocateDirect(bufSize)
          buf.limit(bufSize)

          val ch = FileChannel.open(file, WRITE, APPEND, CREATE)

          var written = 0L
          while (written < Int.MaxValue) {
            written += ch.write(buf.duplicate())
          }

          ch.force(true)
          ch.close()
        }

        val ch = FileChannel.open(file, READ)

        val handle = ft.prepareChannels(bus1, Map("file" -> ch))

        Await.result(
          ft.receive(bus2, handle.manifest) { dir =>
            dir.entries.size should equal(1)
            (s"cmp -s ${dir / "file"} $file" !) should equal(0)
            Future.unit
          },
          60.seconds
        )

      } finally {
        file.delete()
      }
    }
  }
}
