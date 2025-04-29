package fauna.qa.test.main

import com.typesafe.config.ConfigFactory
import fauna.codex.json.JSObject
import fauna.exec.{ ImmediateExecutionContext, Timer }
import fauna.lang.NamedPoolThreadFactory
import fauna.lang.Timestamp
import fauna.net.http._
import fauna.net.netty.UseEpoll
import fauna.net.Network
import fauna.net.security.NoSSL
import fauna.prop.api.{ CoreLauncher, DefaultQueryHelpers }
import fauna.qa._
import fauna.qa.recorders.StatsSnapshot
import fauna.qa.main._
import fauna.qa.net._
import fauna.stats.StatsRecorder
import io.netty.channel.epoll._
import io.netty.channel.nio._
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{ AtomicInteger, LongAdder }
import org.HdrHistogram._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.hashing.MurmurHash3
import fauna.lang.clocks.Clock

class TestLoadGen(name: String, config: QAConfig)
    extends TestGenerator(name, config)
    with ValidatingTestGenerator {
  def this(config: QAConfig) = this("test", config)

  val query = FaunaQuery("secret", AddF(1, 1))
  val streamCount = new AtomicInteger()
  val initCount = new AtomicInteger()
  val validateCount = new AtomicInteger()

  val schema = Schema.DB.Root("secret")

  def stream(schema: Schema.DB) =
    RequestStream("test", { () =>
      streamCount.incrementAndGet
      query
    })

  override def initializer(schema: Schema.DB, clientID: Int) =
    new RequestIterator {
      val name = "test"
      def hasNext = initCount.get < 5

      def next() = {
        initCount.incrementAndGet
        query
      }
    }
  
  override def getValidateStateQuery(schema: Schema.DB, ts: Timestamp): FaunaQuery = {
    val count = validateCount.incrementAndGet
    FaunaQuery("secret", AddF(count, 1))
  }
}

class WorkerSpec
    extends AnyFreeSpec
    with Matchers
    with Eventually
    with DefaultQueryHelpers
    with BeforeAndAfterAll {

  override protected def beforeAll() = CoreLauncher.launchOneNodeCluster("2.0") // Version doesn't matter.

  override protected def afterAll() = CoreLauncher.terminateAll()

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = QAConfig(ConfigFactory.load())
  val host = Host("NoDC", "localhost")
  val coreNode = CoreNode(host, 8443, "NoReplica", true, CoreNode.Role.DataLog)
  val clientFactory = new FaunaClientFactory(10)

  private def testGenMap(testGen: TestGenerator, factory: FaunaClientFactory) =
    Map("test" -> TestGeneratorWithClientFactory(testGen, factory))

  "Worker" - {
    "starts and stops" in {
      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)

      val worker = new Worker(port, testGenMap(new TestLoadGen(config), cf))
      Timer.Global.scheduleTimeout(2.seconds) { worker.close() }
      Await.result(worker.run(), 5.seconds)
    }

    "runs initializers" in {
      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)
      val lg = new TestLoadGen(config)

      val wClient = new WorkerClient(host, port)
      val worker = new Worker(port, testGenMap(lg, cf))
      val runner = worker.run()

      val msg = WorkerReq.InitData(Vector(coreNode), lg.schema, lg, 1 to 1)

      // Test message can be sent
      val res = wClient.send(msg)
      Await.result(res, 5.seconds) should equal(WorkerRep.DataPrepared)

      // Test the work gets done
      Await.result(worker.initData(msg), 5.seconds)

      // Serializing means the worker will create a new instance of the
      // TestGenerator so the counter of `lg` won't be incremented until
      // we test the method directly.
      lg.initCount.get should equal(5)
      lg.streamCount.get should equal(0)

      worker.close()
      Await.result(runner, 5.seconds)
    }

    "runs data streams" in {
      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)
      val lg = new TestLoadGen(config)

      val wClient = new WorkerClient(host, port)
      val worker = new Worker(port, testGenMap(lg, cf))
      val runner = worker.run()

      val msg = WorkerReq.Run(Vector(coreNode), lg.schema, lg, 1)

      // This also happens to test that we can reset the worker. It's
      // the only way to stop the stream when we test sending the message.
      Timer.Global.scheduleTimeout(2.seconds) { worker.reset() }

      // Test message can be sent
      val res = wClient.send(msg)
      Await.result(res, 5.seconds) should equal(WorkerRep.RunComplete(false))

      Timer.Global.scheduleTimeout(2.seconds) { worker.reset() }

      // Test the work gets done
      Await.result(worker.runWork(msg), 5.seconds)

      // Serializing means the worker will create a new instance of the
      // TestGenerator so the counter of `lg` won't be incremented until
      // we test the method directly.
      lg.streamCount.get should be > 0
      lg.initCount.get should equal(0)

      worker.close()
      Await.result(runner, 5.seconds)
    }

    "data streams can be squelched and unsquelched" in {
      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)
      val lg = new TestLoadGen(config)

      val wClient = new WorkerClient(host, port)
      val worker = new Worker(port, testGenMap(lg, cf))
      val runner = worker.run()

      val work = worker.runWork(WorkerReq.Run(Vector(coreNode), lg.schema, lg, 1))

      var cur = lg.streamCount.get

      val res1 = wClient.send(WorkerReq.Squelch(true, Vector(coreNode.host)))
      Await.result(res1, 5.seconds) should equal(
        WorkerRep.Squelched(true, Vector(coreNode.host))
      )
      eventually {
        cur = lg.streamCount.get
        Thread.sleep(1000)
        lg.streamCount.get should equal(cur)
      }

      val res2 = wClient.send(WorkerReq.Squelch(false, Vector(coreNode.host)))
      Await.result(res2, 5.seconds) should equal(
        WorkerRep.Squelched(false, Vector(coreNode.host))
      )
      eventually {
        cur = lg.streamCount.get
        Thread.sleep(1000)
        lg.streamCount.get should be > cur
      }

      worker.close()
      Await.result(work, 5.seconds)
      Await.result(runner, 5.seconds)
    }

    "can provide stats" in {
      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)
      val lg = new TestLoadGen(config)

      val wClient = new WorkerClient(host, port)
      val worker = new Worker(port, testGenMap(lg, cf))
      val runner = worker.run()

      val work = worker.runWork(WorkerReq.Run(Vector(coreNode), lg.schema, lg, 1))

      var cur = lg.streamCount.get

      // Run work for a bit and ensure it's going
      eventually {
        cur = lg.streamCount.get
        Thread.sleep(1000)
        lg.streamCount.get should be > cur
      }

      // Stop the run so we can grab a consistent snapshot
      Await.result(
        wClient.send(WorkerReq.Squelch(true, Vector(coreNode.host))),
        5.seconds
      )
      eventually {
        cur = lg.streamCount.get
        Thread.sleep(1000)
        lg.streamCount.get should equal(cur)
      }

      Await.result(wClient.send(WorkerReq.GetStats), 5.seconds) match {
        case WorkerRep.Stats(StatsSnapshot(_, latencies, _)) =>
          // We might miss one request somewhere. That should be okay
          latencies.getTotalCount.toInt should equal(cur +- 1)
        case msg => fail(s"GetStats returned the wrong msg: $msg")
      }

      worker.close()
      Await.result(work, 5.seconds)
      Await.result(runner, 5.seconds)
    }

    "can validate state" in {
      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)
      val lg = new TestLoadGen(config)

      val wClient = new WorkerClient(host, port)
      val worker = new Worker(port, testGenMap(lg, cf))
      val runner = worker.run()

      val msg = WorkerReq.ValidateState(Vector(coreNode), lg.schema, lg, Clock.time)

      // Test message can be sent
      val res = wClient.send(msg)
      val jresult = MurmurHash3.stringHash(JSObject(("resource", 2)).toString()).toString()
      Await.result(res, 5.seconds) should equal(WorkerRep.ValidateStatePassed(jresult))

      // Test the work gets done
      Await.result(worker.validateState(msg), 5.seconds)

      // Serializing means the worker will create a new instance of the
      // TestGenerator so the counter of `lg` won't be incremented until
      // we test the method directly.
      lg.validateCount.get should be > 0

      worker.close()
      Await.result(runner, 5.seconds)
    }

    "will fail for invalid state" in {
      val host2 = Host("NoDC", "localhost")
      val coreNode2 = CoreNode(host2, 8443, "NoReplica", true, CoreNode.Role.DataLog)

      val port = Network.findFreePort()
      val cf = new FaunaClientFactory(5)
      val lg = new TestLoadGen(config)

      val wClient = new WorkerClient(host, port)
      val worker = new Worker(port, testGenMap(lg, cf))
      val runner = worker.run()

      val msg = WorkerReq.ValidateState(Vector(coreNode, coreNode2), lg.schema, lg, Clock.time)

      // Test message can be sent
      val res = wClient.send(msg)
      val vc = Vector(JSObject(("resource", 2)).toString(), JSObject(("resource", 3)).toString())
      val result = Await.result(res, 5.seconds).asInstanceOf[WorkerRep.ValidateStateFailed]
      result.result should contain theSameElementsAs vc

      // Test the work gets done
      Await.result(worker.validateState(msg), 5.seconds)

      // Serializing means the worker will create a new instance of the
      // TestGenerator so the counter of `lg` won't be incremented until
      // we test the method directly.
      lg.validateCount.get should be > 0

      worker.close()
      Await.result(runner, 5.seconds)
    }
  }

  "Customer" - {
    "will run a set of requests until complete" in {
      val recorder = new Recorder(5)
      val errors = new LongAdder
      val customer = new LegacyCustomer(coreNode, clientFactory, recorder, errors)

      val lg = new TestLoadGen(config)
      val reqs = lg.initializer(lg.schema, 0)

      Await.result(customer.runRequests(reqs), 10.seconds)
      recorder.getIntervalHistogram.getTotalCount should equal(5)
      errors.sum should equal(0)
      customer.close()
    }

    "can be paused and continued" in {
      val recorder = new Recorder(5)
      val errors = new LongAdder
      val customer = new LegacyCustomer(coreNode, clientFactory, recorder, errors)

      val lg = new TestLoadGen(config)

      val runner = customer.runRequests(lg.stream(lg.schema))

      eventually { lg.streamCount.get should be > 0 }

      customer.pause()

      var cur = 0
      eventually {
        cur = lg.streamCount.get
        Thread.sleep(1000)
        lg.streamCount.get should equal(cur)
      }

      customer.continue()
      eventually { lg.streamCount.get should be > cur }

      customer.close()
      Await.result(runner, 5.seconds)
    }

    "can handled dropped connections" in {
      val port = Network.findFreePort()
      val node = CoreNode(
        Host("NoDC", "127.0.0.1"),
        port,
        "noReplica",
        true,
        CoreNode.Role.DataLog)

      val serverCount = new AtomicInteger()
      var cur = 0
      val reqHandler: HttpHandlerF = {
        case (_, req) =>
          implicit val ec = ImmediateExecutionContext
          serverCount.incrementAndGet()
          HttpServer.discard(req) map { _ =>
            val hs = Iterable(
              HTTPHeaders.TxnTime -> "0",
              HTTPHeaders.QueryTime -> "0"
            )
            HttpResponse(200, Body("{}", ContentType.JSON), hs)
          }
      }

      def mkServer =
        new HttpServer(
          new InetSocketAddress("127.0.0.1", port),
          HttpServer.DefaultClientReadTimeout,
          HttpServer.DefaultKeepAliveTimeout,
          NoSSL,
          StatsRecorder.Null,
          () =>
            HttpRequestHandler(
              reqHandler,
              HttpRequestHandler.DefaultExceptionHandlerF
            ),
          HttpServer.MaxInitialLineLength,
          HttpServer.MaxHeaderSize,
          HttpServer.MaxChunkSize,
          Int.MaxValue
        ) {

          override protected val eventLoopGroup = {
            val tf = new NamedPoolThreadFactory("TestServer", true)
            if (UseEpoll) {
              new EpollEventLoopGroup(0, tf)
            } else {
              new NioEventLoopGroup(0, tf)
            }
          }

          override def stop(graceful: Boolean): Unit = {
            eventLoopGroup.shutdownGracefully().await
            super.stop(graceful)
          }
        }

      var server = mkServer
      server.start()

      val recorder = new Recorder(5)
      val errors = new LongAdder
      val customer = new LegacyCustomer(node, clientFactory, recorder, errors)

      val lg = new TestLoadGen(config)

      customer.runRequests(lg.stream(lg.schema))

      eventually {
        cur = serverCount.get
        cur should be > 0
      }

      server.stop()
      server.isRunning should equal(false)

      eventually {
        cur = serverCount.get
        Thread.sleep(100)
        serverCount.get should equal(cur)
      }

      server = mkServer
      server.start()

      eventually(timeout(10.seconds)) {
        cur = serverCount.get
        Thread.sleep(100)
        serverCount.get should be > cur
      }

      customer.close()
      server.stop()
    }
  }
}
