package fauna.qa.test.net

import fauna.api.test.APISpecMatchers
import fauna.atoms.APIVersion
import fauna.exec.ImmediateExecutionContext
import fauna.lang.Timing
import fauna.net.http.HttpServer
import fauna.net.Network
import fauna.prop.api._
import fauna.qa._
import fauna.qa.net._
import java.util.concurrent.TimeoutException
import java.net.InetSocketAddress
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

class FaunaClientSpec
    extends AnyFreeSpec
    with Matchers
    with DefaultQueryHelpers
    with BeforeAndAfterAll
    with Eventually
    with APISpecMatchers
    with APIResponseHelpers {

  private var host: String = _

  override protected def beforeAll() = {
    val CoreLauncher.IDAndClient(_, client) = CoreLauncher.launchOneNodeCluster(APIVersion.Default.toString)
    host = client.cfg.host

    eventually(timeout(15.seconds), interval(1.second)) {
      val res = client.api.get("/ping?scope=write", FaunaDB.rootKey)
      res should respond(OK)
    }
  }

  override protected def afterAll() = CoreLauncher.terminateAll()

  "FaunaClient" - {
    "can query Core" in {
      val coreNode = CoreNode(
        Host("NoDC", host),
        8443,
        "NoReplica",
        true,
        CoreNode.Role.DataLog)

      val clientFactory = new FaunaClientFactory(1)
      val client = Await.result(clientFactory.connect(coreNode), 5.seconds)
      val iters = 1000

      implicit val ec = ExecutionContext.parasitic
      def loop(remaining: Int): Future[Unit] =
        if (remaining == 0) {
          Future.unit
        } else {
          client.query("secret", AddF(1, 1)) flatMap { res =>
            res.code should equal(200)
            (res.body / "resource").as[Int] should equal(2)
            loop(remaining - 1)
          }
        }

      val start = Timing.start
      Await.result(loop(iters), 30.seconds)
      val time = start.elapsedMillis

      info(
        "%d reqs in %d ms, %.3f req/s".format(iters, time, iters.toDouble / (time.toDouble / 1000))
      )

      Await.ready(client.close(), 5.seconds)
    }

    "can timeout requests" in {
      val port = Network.findFreePort()
      val node = CoreNode(
        Host("NoDC", host),
        port,
        "NoReplica",
        true,
        CoreNode.Role.DataLog)
      val factory = new FaunaClientFactory(1)

      val server = HttpServer(new InetSocketAddress(host, port)) { (_, req) =>
        implicit val ec = ImmediateExecutionContext
        HttpServer.discard(req) flatMap { _ => Future.never }
      }
      server.start()

      val client = Await.result(factory.connect(node), 5.seconds)
      val timing = Timing.start
      val req = client.query("secret", AddF(1, 1), 1.second)
      Await.ready(req, 10.seconds)

      timing.elapsed.toSeconds.toInt should equal(1 +- 1)

      a[TimeoutException] should be thrownBy {
        req.value.get.get
      }

      server.stop()
      Await.ready(client.close(), 5.seconds)
    }
  }
}
