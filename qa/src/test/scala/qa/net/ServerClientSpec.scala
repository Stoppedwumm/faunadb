package fauna.qa.test.net

import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.net.Network
import fauna.qa.Host
import fauna.qa.net._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

class ServerClientSpec extends AnyFreeSpec with Matchers {
  val host = Host("NoDC", "localhost")

  case class TestRequest(i: Int)
  implicit val ReqCodec = CBOR.RecordCodec[TestRequest]
  case class TestResponse(i: Int)
  implicit val RepCodec = CBOR.RecordCodec[TestResponse]

  "Server / Client" - {
    "will process CBOR messages" in {
      val port = Network.findFreePort()

      val server = new Server[TestRequest, TestResponse](port, { req =>
        Future.successful(TestResponse(req.i))
      })
      server.start()

      val client = new Client[TestRequest, TestResponse](host, port)

      val rep = client.send(TestRequest(0))
      Await.result(rep, 5.seconds) should equal(TestResponse(0))
      server.stop()
    }

    "can handle multiple messages" in {
      val port = Network.findFreePort()

      val server = new Server[TestRequest, TestResponse](port, { req =>
        Timer.Global.delay(1.second)(Future.successful(TestResponse(req.i)))
      })
      server.start()

      val client = new Client[TestRequest, TestResponse](host, port)

      implicit val ec = ExecutionContext.global
      val reps = (0 until 5).map { i =>
        client.send(TestRequest(i))
      }.sequence

      Await.result(reps, 5.seconds) should equal((0 until 5).map(TestResponse(_)))
      server.stop()
    }
  }
}
