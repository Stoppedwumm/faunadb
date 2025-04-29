package fauna.qa.test.main

import com.typesafe.config.ConfigFactory
import fauna.config.Loader
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.net.Network
import fauna.qa._
import fauna.qa.main._
import fauna.qa.net._
import fauna.qa.operator.Cmd
import java.io.File
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import scala.concurrent._
import scala.concurrent.duration._

class OperatorSpec extends AnyFreeSpec with Matchers with Eventually {
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = new QAConfig(ConfigFactory.load()) {

    override val coreConfPath = {
      val f = File.createTempFile("core-", ".conf")
      f.deleteOnExit()
      f.toPath
    }
  }

  val host = Host("NoDC", "localhost")

  "Operator" - {
    "starts and stops" in {
      val port = Network.findFreePort()

      val operator = new Operator(port, config)
      Timer.Global.scheduleTimeout(2.seconds) { operator.close() }
      Await.result(operator.run(), 5.seconds)
    }

    "can create a valid core config" in {
      val port = Network.findFreePort()

      val oClient = new OperatorClient(host, port)
      val operator = new Operator(port, config)
      val runner = operator.run()

      val res = oClient.send(OperatorReq.CreateConfig)
      Await.result(res, 5.seconds).isSuccess should be(true)

      Loader.deserialize(config.coreConfPath).isRight shouldBe (true)

      operator.close()
      Await.result(runner, 5.seconds)
    }

    "run commands" in {
      val port = Network.findFreePort()

      val oClient = new OperatorClient(host, port)
      val operator = new Operator(port, config)
      val runner = operator.run()

      val resS = oClient.send(OperatorReq.RunCmd(Cmd.Etc.Echo("testing")))
      Await.result(resS, 5.seconds).isSuccess should be(true)

      val resF = oClient.send(OperatorReq.RunCmd(Cmd.Etc.ExitCode(1)))
      Await.result(resF, 5.seconds).isSuccess should be(false)

      operator.close()
      Await.result(runner, 5.seconds)
    }

    "run commands simultaneously" in {
      val port = Network.findFreePort()

      val oClient = new OperatorClient(host, port)
      val operator = new Operator(port, config)
      val runner = operator.run()

      // If these run serially, they'd take 10 seconds to
      // complete. They should take 5. We'll wait 8 to give
      // it a little buffer.
      val res1 = oClient.send(OperatorReq.RunCmd(Cmd.Etc.Sleep(5)))
      val res2 = oClient.send(OperatorReq.RunCmd(Cmd.Etc.Sleep(5)))
      implicit val ec = ExecutionContext.parasitic
      Await.result(Seq(res1, res2).sequence, 8.seconds).map(_.isSuccess) should be(Seq(true, true))

      operator.close()
      Await.result(runner, 5.seconds)
    }

    "can stop running commands" in {
      val port = Network.findFreePort()

      val oClient = new OperatorClient(host, port)
      val operator = new Operator(port, config)
      val runner = operator.run()

      val res = oClient.send(OperatorReq.RunCmd(Cmd.Etc.Sleep(20)))

      Timer.Global.scheduleTimeout(2.seconds) {
        val res = oClient.send(OperatorReq.Reset)
        Await.result(res, 5.seconds) should equal(OperatorRep.Ready)
      }

      // The original 20 second sleep should stop within
      // a few seconds after the reset is sent.
      Await.result(res, 5.seconds).isSuccess should equal(false)

      operator.close()
      Await.result(runner, 5.seconds)
    }
  }
}
