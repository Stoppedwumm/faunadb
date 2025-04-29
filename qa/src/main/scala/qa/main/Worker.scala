package fauna.qa.main

import com.typesafe.config.ConfigFactory
import fauna.exec.AsyncSemaphore
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.qa._
import fauna.qa.net._
import fauna.qa.recorders.StatsSnapshot
import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.{ AtomicReference, LongAdder }
import org.HdrHistogram._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent._
import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * The following network helper classes provide a nice class name for
  * logging as well as simpler creation due to not needing to provide
  * types for Req and Rep.
  */
class WorkerServer(port: Int, handler: WorkerReq => Future[WorkerRep])
    extends Server[WorkerReq, WorkerRep](port, handler)

class WorkerClient(host: Host, port: Int)
    extends Client[WorkerReq, WorkerRep](host, port)

case class WorkerClientGroup(clients: Vector[Client[WorkerReq, WorkerRep]])
    extends ClientGroup[WorkerReq, WorkerRep] {

  def limited(clients: Vector[Client[WorkerReq, WorkerRep]]) =
    copy(clients = clients)
}

object WorkerClientGroup {

  def apply(hosts: Vector[Host], port: Int): WorkerClientGroup =
    apply(hosts map { new WorkerClient(_, port) })
}

case class TestGeneratorWithClientFactory(
  generator: TestGenerator,
  factory: FaunaClientFactory)

/**
  * The main Worker object started by a script on a dedicated machine
  */
object Worker {

  @annotation.nowarn("cat=unused-params")
  def main(argv: Array[String]): Unit = {
    val config = QAConfig(ConfigFactory.load())
    val log = getLogger()

    val testGenNames = config.testGenNames

    val testGens: Seq[TestGenerator] = testGenNames flatMap {
      case "None"      => None
      case testGenName => {
        try {
          val cls = Class.forName(s"fauna.qa.generators.test.$testGenName")
          val ctor = cls.getDeclaredConstructor(classOf[QAConfig])
          ctor.newInstance(config).asInstanceOf[QATest].generators
        } catch {
          case e: ClassNotFoundException =>
            log.error(s"Invalid test generator: $testGenName")
            throw e
        }
      }
    }

    val testGenFactoryMap: Map[String, TestGeneratorWithClientFactory] =
      testGens map { gen => {
        // Create FaunaClientFactory with 10% buffer for parallel test gens;
        // e.g. 100 clients / 3 test gens gets 34 each, so adding a little
        // buffer for those scenarios
        val parallelism = ((config.clients * 1.1) toInt) / testGens.length
        gen.name ->
          TestGeneratorWithClientFactory(
            gen,
            new FaunaClientFactory(parallelism, gen.streamWithHttp2))
        }
      } toMap

    val port = config.remotePort
    val worker = new Worker(
      port,
      testGenFactoryMap,
      config.perCustomerRate
    )

    log.info(s"Starting Worker on $port")
    Await.result(worker.run(), Duration.Inf)
  }
}

/**
  * Workers simply send requests to specified Core nodes and record the results.
  * They are controlled remotely by the Commander via WorkerReq messages.
  */
class Worker(
  port: Int,
  testGenFactoryMap: Map[String, TestGeneratorWithClientFactory],
  rate: Rate = Rate.Max) {

  private[this] val histoRecorder = new Recorder(StatsSnapshot.SignificantDigits)
  private[this] var histoSnapshot: Histogram = _
  private[this] val errorCount = new LongAdder()

  private[this] val runningP = Promise[Unit]()
  private[this] val log = getLogger()

  private[this] val server = new WorkerServer(port, {
    case WorkerReq.Reset              => reset()
    case msg: WorkerReq.InitData      => initData(msg)
    case msg: WorkerReq.Run           => runWork(msg)
    case msg: WorkerReq.Squelch       => squelch(msg)
    case msg: WorkerReq.ValidateState => validateState(msg)
    case WorkerReq.GetStats           => statsSnapshot()
    case WorkerReq.Ping               => Future.successful(WorkerRep.Pong)
  })

  private[this] var customerMap = Map.empty[String, AtomicReference[Seq[Customer]]]

  // scalac is wrong, silence it.
  @annotation.nowarn("cat=unused")
  private[this] var squelchedHosts = ListBuffer.empty[Host]

  private def swapCustomers(newCustomers: Seq[Customer], key: String): Unit = synchronized {
    customerMap.get(key) match {
      case Some(customers) => customers.getAndSet(newCustomers) foreach { _.close() }
      case None => customerMap += key -> new AtomicReference[Seq[Customer]](newCustomers)
    }
  }

  private def withCustomers[T](
    cluster: Seq[CoreNode],
    parallelism: Int,
    customerKey: String,
    retryOnFailure: Boolean = false,
    customerRate: Rate = rate,
    lastSeenTxnTime: Timestamp = Timestamp.Epoch,
    useDriver: Boolean = false)(
    f: Seq[Customer] => Future[T]
  ): Future[T] = {
    implicit val ec = ExecutionContext.parasitic

    val config = QAConfig(ConfigFactory.load())
    val active = cluster filter { _.isActive }
    val clients = if (active.isEmpty) {
      Seq.empty[Customer]
    } else {
      (1 to parallelism) map { idx =>
        val node = active(idx % active.size)
        // We pass the lastSeenTxnTime from the schema in order to seed the
        // FaunaClient such that it will see all schema objects that were created.

        useDriver match {
          case true => new DriverCustomer(node, config, customerKey)
          case false =>
            new LegacyCustomer(
              node,
              testGenFactoryMap(customerKey).factory,
              histoRecorder,
              errorCount,
              customerRate,
              retryOnFailure,
              lastSeenTxnTime
            )
        }
      }
    }

    swapCustomers(clients, customerKey)
    f(Random.shuffle(clients)) ensure { swapCustomers(Seq.empty, customerKey) }
  }

  def reset(): Future[WorkerRep] = {
    val current = synchronized {
      customerMap.values flatMap { _.getAndSet(Seq.empty) }
    }
    log.info(s"Resetting. Closing ${current.size} customers.")
    current foreach { _.close() }
    histoRecorder.reset()
    Future.successful(WorkerRep.Ready)
  }

  def initData(msg: WorkerReq.InitData): Future[WorkerRep] = {
    log.info(
      s"[${msg.generator.name}] Initializing data with " +
        s"${msg.parallelism} customers")
    withCustomers(
      msg.cluster,
      msg.parallelism,
      msg.generator.name,
      retryOnFailure = true,
      lastSeenTxnTime = msg.schema.lastSeenTxnTimestamp) { customers =>
      // limit parallelism
      val semaphore = AsyncSemaphore(5)
      implicit val ec = ExecutionContext.global
      val runners = customers.zip(msg.clientIDs) map {
        case (customer, clientID) =>
          semaphore.acquire() flatMap { _ =>
            val reqs = msg.generator.initializer(msg.schema, clientID)
            val reqsList = reqs.iterator.toList
            if (!reqsList.isEmpty) {
              log.info(s"[${msg.generator.name}] inserting ${reqsList.size} records")
            }
            customer.runRequests(RequestIterator(reqs.name, reqsList))
          } ensure {
            semaphore.release()
          }
      }
      runners.join.map { _ =>
        WorkerRep.DataPrepared
      }
    }
  }

  def runWork(msg: WorkerReq.Run): Future[WorkerRep] = {
    log.info(
      s"[${msg.generator.name}] " +
        s"Starting load traffic with ${msg.parallelism} customers")
    withCustomers(
      msg.cluster,
      msg.parallelism,
      msg.generator.name,
      useDriver = msg.generator.useDriver) { customers =>
      val runners = customers map { customer =>
        customer.run(
          testGenFactoryMap(msg.generator.name).generator,
          msg.schema
        )
      }
      implicit val ec = ExecutionContext.global
      runners.join.map { _ =>
        WorkerRep.RunComplete(false)
      } recover {
        case _ => WorkerRep.RunComplete(true)
      }
    }
  }

  def validateState(msg: WorkerReq.ValidateState): Future[WorkerRep] = {
    log.info(
      s"[${msg.generator.name}] Validating database state at time ${msg.snapshotTS}")
    implicit val ec = ExecutionContext.global

    val active = msg.cluster filter { node =>
      node.isActive && !squelchedHosts.contains(node.host)
    }

    if (active.isEmpty) {
      Future.successful(WorkerRep.ValidateStateNoop)
    } else {
      val ts = msg.snapshotTS

      val allQueryResults = active map { host =>
        val factory = new FaunaClientFactory(1)
        val cust =
          new LegacyCustomer(
            host,
            factory,
            histoRecorder,
            errorCount,
            Rate.Max,
            false)

        val testGen = testGenFactoryMap(msg.generator.name).generator match {
          case gen: ValidatingTestGenerator => gen
          case g => throw new IllegalStateException(s"Expected ValidatingStateQuery type, got ${g.getClass}")
        }

        cust.runValidationQuery(
          testGen,
          msg.schema,
          minSnapTime = ts,
          timeout = 20.seconds) map { result =>
            log.info(s"Host[${host.addr}] ${testGen.name} result: ${result}")
            Some(result)
          } recoverWith {
            case ex: TimeoutException =>
              log.info(s"Host[${host.addr}] Ignoring timeout: ${ex.getMessage()}")
              Future.successful(None)
            case ex: ConcurrentModificationException =>
              log.info(s"Host[${host.addr}] Ignoring server-returned 409 code: ${ex.getMessage()}")
              Future.successful(None)
            case ex: IllegalStateException =>
              Future.successful(Option(s"Host[${host.addr}] ${testGen.name} ValidateFailure: ${ex.getMessage()}"))
            case ex =>
              log.error("Unhandled validation error", ex)
              Future.failed(ex)
        } ensure {
          cust.close()
        }
      } sequence

      allQueryResults map { results =>
        val someResults = results.flatten
        val hashedResults = someResults map { result =>
          MurmurHash3.stringHash(result).toString()
        }

        if (hashedResults.isEmpty) {
          WorkerRep.ValidateStateNoop
        } else {
          var passed =
            !(someResults exists { result => result.contains("ValidateFailure:") })

          for (val1 <- hashedResults; val2 <- hashedResults) {
            if (val1 != val2) {
              passed = false
            }
          }

          if (passed) {
            WorkerRep.ValidateStatePassed(hashedResults.last)
          } else {
            WorkerRep.ValidateStateFailed(someResults.toVector)
          }
        }
      }
    }
  }

  def squelch(msg: WorkerReq.Squelch): Future[WorkerRep] = {
    log.info(s"${if (msg.flag) "" else "Un-"}Squelching ${msg.hosts.mkString(", ")}")
    implicit val ec = ExecutionContext.global
    Future {
      val squelched = Vector.newBuilder[Host]

      synchronized {
        customerMap.values foreach {
          _.getAndUpdate({ current =>
            val hostSet = msg.hosts.toSet
            current foreach { c =>
              if (hostSet(c.host)) {
                if (msg.flag) {
                  c.pause()
                  squelchedHosts += c.host
                } else {
                  c.continue()
                  squelchedHosts -= c.host
                }
                squelched += c.host
              }
            }
            current
          })
        }
      }

      log.info(
        s"${if (msg.flag) "" else "Un-"}Squelched ${squelched.result().mkString(", ")}"
      )
      WorkerRep.Squelched(msg.flag, squelched.result())
    }
  }

  def statsSnapshot(): Future[WorkerRep] = {
    log.debug("Snapshotting stats")
    implicit val ec = ExecutionContext.global
    Future {
      histoSnapshot = histoRecorder.getIntervalHistogram(histoSnapshot)
      WorkerRep.Stats(
        StatsSnapshot(System.nanoTime, histoSnapshot, errorCount.sumThenReset())
      )
    }
  }

  def run(): Future[Unit] = {
    server.start()
    runningP.future
  }

  def close(): Unit = {
    server.stop()
    synchronized { customerMap.keys foreach { swapCustomers(Seq.empty, _) } }
    runningP.setDone()
  }
}
