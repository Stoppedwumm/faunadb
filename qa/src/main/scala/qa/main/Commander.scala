package fauna.qa.main

import com.typesafe.config.ConfigFactory
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.qa._
import fauna.qa.recorders.RunRecorder
import fauna.qa.net._
import fauna.qa.operator._
import java.util.concurrent.TimeoutException
import scala.collection.immutable.Map
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

// Used internally to signify completion status
private sealed trait Completed
private case object Succeeded extends Completed
private case class Failed(err: Throwable) extends Completed

/**
  * Commander's Main object.
  * Based on the configuration, it'll create the op and test runners and a Commander
  * for each generator. Then it will run each in serial.
  */
object Commander {
  @annotation.nowarn("cat=unused-params")
  def main(argv: Array[String]): Unit = {
    val log = getLogger()
    val config = QAConfig(ConfigFactory.load())

    val testGenNames = config.testGenNames
    val opGenName = config.opGenName

    log.info(
      s"Initializing. TestGens: '${testGenNames.mkString(", ")}'; OpGen: '$opGenName'; Seed: ${config.propConfig.seed}"
    )

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

    val qaOp =
      try {
        val cls = Class.forName(s"fauna.qa.generators.op.$opGenName")
        val ctor = cls.getDeclaredConstructor(classOf[QAConfig])
        ctor.newInstance(config).asInstanceOf[OpGenerator]
      } catch {
        case e: ClassNotFoundException =>
          log.error(s"Invalid op generator: $opGenName")
          throw e
      }
    
    val commander = new Commander(config, testGens, qaOp)

    implicit val ec = ExecutionContext.global
    Await.result(
      commander.run recoverWith {
        case err: IllegalStateException =>
          log.error(err.toString(), err)
          err.printStackTrace(System.err)
          sys.exit(2)
        case NonFatal(err) =>
          log.error(err.toString(), err)
          err.printStackTrace(System.err)
          sys.exit(1)
      },
      Duration.Inf
    )
  }
}

/**
  * Commander orchestrates QA runs. It's the brains of the outfit.
  * This will step through resetting the cluster, initializing the cluster, initializing
  * data for the test run, then starting the OpRunner. After initialization, it will
  * periodically request stats snapshots from the workers and record them. When complete
  * it will report findings and exit.
  */
class Commander(
  config: QAConfig,
  testGenerators: Seq[TestGenerator],
  opGenerator: OpGenerator
) {

  private[this] val log = getLogger()
  implicit val ec = ExecutionContext.global

  private[this] val workers =
    WorkerClientGroup(config.qaCluster, config.remotePort)

  private[this] val operators =
    OperatorClientGroup(config.coreCluster.map { _.host }, config.remotePort)

  def run: Future[Unit] = {
    val startTime = Clock.time

    val recorder =
      new RunRecorder(startTime, config)

    val testRunner =
      new TestRunner(recorder, config, testGenerators, workers)

    val opRunner =
      new OpRunner(recorder, config, testRunner, opGenerator, operators)

    val testGens = testGenerators map { gen => gen.getClass.getSimpleName }
    val opGen = opGenerator.getClass.getSimpleName
    val clientCount = config.clients.toString

    val title = "Starting QA Run"
    val fields = Seq(
      "TestGens" -> (if (testGens.isEmpty) "None" else testGens.mkString(",")),
      "OpGen" -> opGen,
      "Max Duration" -> config.testDuration.toString,
      "Clients" -> clientCount
    )
    val tags = Seq("test" -> testGens.mkString(","), "op" -> opGen, "clients" -> clientCount)

    val slackF = recorder.slackMsg(title, fields)
    val annotateF = recorder.annotate(title, fields, tags)

    val completeF = Seq(slackF, annotateF).join flatMap { _ =>
      initialize(testRunner, opRunner, recorder) flatMap {
        case (dbMap, cluster) =>
          run(dbMap, cluster, testRunner, opRunner, recorder) flatMap { reason =>
            notify(startTime, reason, recorder)
          } transformWith { reason =>
            finalize(dbMap, cluster, testRunner, opRunner, recorder) flatMap { _ =>
              reason.fold(Future.failed, Future.successful)
            }
          }
      }
    }

    def heartbeatWorkers(): Unit =
      if (!completeF.isCompleted) {
        val hbPromise = Promise[Unit]()
        hbPromise.completeWith(testRunner.heartbeatWorkers())

        config.timer.scheduleTimeout(30.seconds) {
          val msg = "Worker heartbeat timed out; trying again."

          if (hbPromise.tryFailure(new TimeoutException(msg))) {
            log.warn(msg)
          }
        }

        hbPromise.future ensure {
          config.timer.scheduleTimeout(30.seconds) {
            heartbeatWorkers()
          }
        }
      }
    heartbeatWorkers()

    completeF transformWith { result =>
      val close = Seq(
        testRunner.close(),
        opRunner.close(),
        workers.close(),
        operators.close(),
        recorder.close()).join

      // ignore errors during any close(), unwrap Failed
      close recover { case _ => () } flatMap { _ =>
        result match {
          case Failure(err)         => Future.failed(err)
          case Success(Failed(err)) => Future.failed(err)
          case Success(_)           => Future.unit
        }
      }
    }
  }

  /**
    * Initializes a database and cluster for a test run.
    *
    * If the initialization process exceeds fauna.ready-timeout, fails
    * the test run with a timeout exception.
    */
  private def initialize(
    testRunner: TestRunner,
    opRunner: OpRunner,
    recorder: RunRecorder): Future[(Map[TestGenerator, Schema.DB], Vector[CoreNode])] = {

    val init = Promise[(Map[TestGenerator, Schema.DB], Vector[CoreNode])]()

    log.info("Resetting TestRunner and OpRunner")

    val initCompleteF = for {
      _ <- Seq(testRunner.reset(), opRunner.reset()).join

      _ = log.info("Initializing cluster")
      cluster <- opRunner.initCluster(config.coreCluster)

      _ = log.info("Initializing schema")
      db <- testRunner.initSchema(cluster)
      _ <- recorder.annotate(
        "Schema Created",
        Seq.empty,
        Seq("phase" -> "schema-create")
      )

      _ = log.info("Initializing data")
      _ <- recorder.annotate(
        "Initializing Data",
        Seq.empty,
        Seq("phase" -> "data-init")
      )
      _ <- testRunner.initData(cluster, db)
      _ <- recorder.annotate("Data Ready", Seq.empty, Seq("phase" -> "data-ready"))

    } yield (db, cluster)

    init.completeWith(initCompleteF)

    config.timer.scheduleTimeout(config.readyTimeout) {
      val msg =
        s"Max initialization time (${config.readyTimeout.toCoarsest}) elapsed."

      if (init.tryFailure(new TimeoutException(msg))) {
        log.warn(msg)
      }
    }

    init.future
  }

  @annotation.nowarn("cat=unused-params")
  private def finalize(
    dbMap: Map[TestGenerator, Schema.DB],
    cluster: Vector[CoreNode],
    testRunner: TestRunner,
    opRunner: OpRunner,
    recorder: RunRecorder): Future[Unit] = {
    testRunner.tearDownSchema(dbMap, cluster)
  }

  /**
    * Executes the test run against an initialized database and
    * cluster.
    *
    * This method must not fail with an exception. It must capture
    * exceptions in Failed for notify().
    *
    * If the test run exceeds fauna.test.duration, fails with a
    * timeout.
    */
  private def run(
    dbMap: Map[TestGenerator, Schema.DB],
    cluster: Vector[CoreNode],
    testRunner: TestRunner,
    opRunner: OpRunner,
    recorder: RunRecorder): Future[Completed] = {

    val result = Promise[Completed]()

    log.info("Starting run")

    def periodicallyRecordStats(): Unit =
      if (!result.isCompleted) {
        testRunner.recordStats() ensure {
          config.timer.scheduleTimeout(10.seconds) {
            periodicallyRecordStats()
          }
        }
      }
    periodicallyRecordStats()

    def periodicallyValidateClusterState(): Unit =
      if (!result.isCompleted) {
        testRunner.validateClusterState(cluster, dbMap) recover {
          case NonFatal(ex) =>
            result.tryFailure(ex)
            ()
        } ensure {
          config.timer.scheduleTimeout(10.seconds) {
            periodicallyValidateClusterState()
          }
        }
      }

    periodicallyValidateClusterState()

    val runCompleteF = for {
      _ <- recorder.annotate(
        "Starting Test",
        Seq.empty,
        Seq("phase" -> "start-test")
      )
      _ <- opRunner.run(cluster, dbMap)
    } yield Succeeded

    result.completeWith(runCompleteF)

    // Timeout if the test don't finish after 10 minutes past its configured duration
    val maxDuration = config.testDuration + 10.minutes

    config.timer.scheduleTimeout(maxDuration) {
      val msg = s"Max test duration (${maxDuration.toCoarsest}) elapsed."

      if (result.tryFailure(new TimeoutException(msg))) {
        log.warn(msg)
      }
    }

    result.future recover {
      case err =>
        log.error(s"Run Failed: $err")
        Failed(err)
    }
  }

  /**
    * Sends notifications to DataDog and Slack as to the result of the
    * run.
    *
    * Supresses failures to send either notification, and returns the
    * result of the run.
    */
  private def notify(
    startTime: Timestamp,
    reason: Completed,
    recorder: RunRecorder): Future[Completed] = {
    val dashboards = config.dashboardURLs(startTime, Clock.time) sortBy {
      _._1
    } map {
      case (name, url) => s"  * <$url|$name>"
    } mkString "\n"

    val notice = reason match {
      case Succeeded =>
        val fields = Seq.newBuilder[(String, String)]
        fields += ("Result" -> "Success")
        if (dashboards.nonEmpty) {
          fields += ("Dashboards" -> dashboards)
        }

        log.info(s"Run Succeeded.")
        val annF = recorder.annotate(
          "Completed Successfully",
          Seq.empty,
          Seq("phase" -> "complete", "result" -> "succeeded")
        )
        val slackF = recorder.slackMsg("Completed QA Run", fields.result())
        Seq(annF, slackF).join

      case Failed(err) =>
        ("Failed", err.toString)
        val fields = Seq.newBuilder[(String, String)]
        fields += ("Result" -> "Failure")
        if (dashboards.nonEmpty) {
          fields += ("Dashboards" -> dashboards)
        }
        fields += ("Failure" -> err.toString)

        log.info(s"Run Failed: $err")
        val annF = recorder.annotate(
          "Failed",
          Seq("Reason" -> err.toString),
          Seq("phase" -> "complete", "result" -> "failed")
        )
        val slackF = recorder.slackMsg("Completed QA Run", fields.result())
        Seq(annF, slackF).join
    }

    // return the completion value, regardless of Slack's success
    notice transform { case _ => Success(reason) }
  }

}

/**
  * TestRunner orchestrates traffic to the cluster based on the TestGenerator.
  */
class TestRunner(
  recorder: RunRecorder,
  config: QAConfig,
  generators: Seq[TestGenerator],
  workers: WorkerClientGroup
)(implicit val ec: ExecutionContext) {

  private[this] val log = getLogger()

  @volatile private[this] var shouldValidate: Boolean = false
  private[this] val isValidatingTestGenerator: Boolean =
    generators.exists(gen => gen.isInstanceOf[ValidatingTestGenerator])
  private[this] var runStartTime: Timestamp = Clock.time

  def close(): Future[Unit] =
    reset()

  def recordStats(): Future[Unit] =
    recorder.recordStat {
      workers.all(WorkerReq.GetStats) map { ret =>
        ret collect {
          case (h, WorkerRep.Stats(snap)) => (h, snap)
        }
      }
    }

  def heartbeatWorkers(): Future[Unit] =
    workers.all(WorkerReq.Ping).unit

  def reset(): Future[Unit] = {
    shouldValidate = false
    workers.send(WorkerReq.Reset)
  }

  def initSchema(cluster: Vector[CoreNode]): Future[Map[TestGenerator, Schema.DB]] = {
    val genMap = generators.foldLeft(Future.successful(Seq.empty[(TestGenerator, Schema.DB)])) {
      (accFuture, generator) =>
        accFuture flatMap { acc =>
          generator.setup(cluster.head) map { db => acc :+ (generator -> db) }
        }
    }

    genMap map { _.toMap } recoverWith {
      case error if cluster.size > 1 => {
        log.error(s"Error on initSchema: $error")
        initSchema(cluster.tail)
      }
    } andThen {
      case r =>
        log.info(s"Schema initialization complete: $r")
    }
  }

  def tearDownSchema(dbMap: Map[TestGenerator, Schema.DB], cluster: Vector[CoreNode]): Future[Unit] = {
    val genMap = dbMap map { case (generator, db) =>
      generator.tearDown(db, cluster.head) 
    } join

    genMap recoverWith {
      case _ if cluster.size > 1 => tearDownSchema(dbMap, cluster.tail)
    } andThen {
      case r =>
        log.info(s"Schema tearing down complete: $r")
    }
  }

  def initData(cluster: Vector[CoreNode], dbMap: Map[TestGenerator, Schema.DB]): Future[Unit] = {
    dbMap map { case (generator, db) =>
      val reqs = config.clientIDsByHost map {
        case (host, ids) =>
          val hostsInDC = cluster filter { _.dc == host.dc }
          (host, WorkerReq.InitData(hostsInDC, db, generator, ids))
      }

      workers.all(reqs).unit andThen { r =>
        runStartTime = Clock.time
        log.info(s"Data initialization complete: $r")
      }
    } join
  }

  def run(cluster: Vector[CoreNode], dbMap: Map[TestGenerator, Schema.DB]): Future[Unit] = {
    shouldValidate = isValidatingTestGenerator

    dbMap map { case (generator, db) =>
      val reqs = config.clientIDsByHost map {
        case (host, ids) =>
          val hostsInDC = cluster filter { _.dc == host.dc }
          (host, WorkerReq.Run(hostsInDC, db, generator, ids.size))
      }

      workers.all(reqs) map { result =>
        result map {
          case (h, WorkerRep.RunComplete(true)) =>
            throw new IllegalStateException(
              f"Worker '${h.addr}' reported errors; check worker's qa.log")
          case (_, _) => ()
        }
      } andThen { r =>
        log.info(s"Data generation run complete: $r")
      }
    } join
  }

  def squelch(flag: Boolean, hosts: Vector[Host]): Future[Unit] = {
    workers.send(WorkerReq.Squelch(flag, hosts))
  }

  def validateClusterState(cluster: Vector[CoreNode], dbMap: Map[TestGenerator, Schema.DB]): Future[Unit] = {
    if (shouldValidate) {
      dbMap map {
        case (generator: ValidatingTestGenerator, db) =>
          // We found that the latency between "Clock.time" and the cluster receiving the
          // validate query was enough for the state to have changed and the validate
          // query to fail since it was sending a snapshot ts earlier than the writes
          // it expects to see; adding a 1-second buffer to cover that delay
          val now = Clock.time + 1.second
          val past = Timestamp.ofMillis(config.rand.between(runStartTime.millis, now.millis))

          if (generator.isTimeAware) {
            val states = Seq(now, past) map { time =>
              validateClusterStateAtTimestamp(cluster, db, time, generator)
            }

            states.join
          } else {
            validateClusterStateAtTimestamp(cluster, db, now, generator)
          }
        case (_, _) => Future.unit
      } join
    } else {
      Future.unit
    }
  }

  def validateClusterStateAtTimestamp(
    cluster: Vector[CoreNode],
    db: Schema.DB,
    ts: Timestamp,
    generator: TestGenerator): Future[Unit] = {
    val reqs = config.clientIDsByHost map {
      case (host, _) =>
        val hostsInDC = cluster filter { _.dc == host.dc }
        log.info(s"Worker[${host.addr}] ${generator.name} validating state on hosts: ${hostsInDC.map(x => x.addr).mkString(", ")}")
        (host, WorkerReq.ValidateState(hostsInDC, db, generator, ts))
    }

    workers.all(reqs) map { r =>
      val passedResults: Seq[(Host, String)] = r collect {
        case (h, WorkerRep.ValidateStatePassed(x)) => (h, x)
        case (h, WorkerRep.ValidateStateFailed(x)) =>
          throw new IllegalStateException(s"Worker[${h.addr}] ${generator.name} mismatch: ${x.mkString(", ")}")
      }

      for {
        (host1, val1) <- passedResults
        (host2, val2) <- passedResults
      } {
        if (!val1.equals(val2)) {
          throw new IllegalStateException(
            s"${generator.name}.ValidateState found inconsistent hashes between Workers ${host1} and ${host2}; ${val1} <> ${val2}")
        }
      }
    }
  }
}

/**
  * OpRunner orchstrates cluster operations based on the OpGenerator
  */
class OpRunner(
  recorder: RunRecorder,
  config: QAConfig,
  testRunner: TestRunner,
  generator: OpGenerator,
  operators: OperatorClientGroup
)(implicit val ec: ExecutionContext) {
  @volatile private[this] var closed: Boolean = false

  private[this] val log = getLogger()

  def close(): Future[Unit] = {
    closed = true
    reset()
  }

  def reset(): Future[Unit] =
    operators.send(OperatorReq.Reset)

  def initCluster(nodes: Vector[CoreNode]): Future[Vector[CoreNode]] = {
    val QASetup(ops, coreNodes) = generator.setup(nodes)
    log.info(s"InitCluster $ops")

    val ret = for {
      _ <- resetCore(coreNodes)
      _ <- runOps(ops, Vector.empty, Map.empty, config.timeout.bound)
    } yield coreNodes
    ret andThen {
      case r =>
        log.info(s"InitCluster complete: $r")
    }
  }

  def run(cluster: Vector[CoreNode], dbMap: Map[TestGenerator, Schema.DB]): Future[Unit] = {
    // All operations presume traffic is running. The sleep times
    // provide a little buffer for workers to ramp up/down.
    val stop = Vector(Pause(1.minute), StopTraffic)
    val ops = (Pause(1.minute) +: generator.ops(cluster)) ++ stop

    // Giving a little more buffer here for time-bound Ops like InitOnly
    val deadline = (config.testDuration + 5.seconds).bound

    // Run StartTraffic in parallel with the rest of the Ops for the OpGen
    Seq(
      runOp(StartTraffic, cluster, dbMap),
      runOps(ops, cluster, dbMap, deadline)
    ).join andThen {
      case r => log.info(s"Ops run complete: $r")
    }
  }

  private def resetCore(coreNodes: Vector[CoreNode]): Future[Unit] = {
    val cmds = Vector.newBuilder[Cmd]

    cmds += Cmd.Time.EnableNTP
    cmds += Cmd.Network.Reset

    config.coreVersion foreach { vers =>
      cmds += Cmd.RPM.Remove
      cmds += Cmd.RPM.Install(vers)
    }

    runOp(Remote(cmds.result(), Hosts(coreNodes))) flatMap { _ =>
      if (config.getBoolean("fauna.db.create-config")) {
        operators.send(OperatorReq.CreateConfig)
      } else {
        Future.unit
      }
    }
  }

  private def runOps(
    ops: Vector[Operation],
    cluster: Vector[CoreNode],
    dbMap: Map[TestGenerator, Schema.DB],
    deadline: TimeBound
  ): Future[Unit] =
    if (ops.nonEmpty && deadline.hasTimeLeft && !closed) {
      runOp(ops.head, cluster, dbMap) flatMap { _ =>
        runOps(ops.tail, cluster, dbMap, deadline)
      }
    } else {
      Future.unit
    }

  private def runOp(
    op: Operation,
    cluster: Vector[CoreNode] = Vector.empty,
    dbMap: Map[TestGenerator, Schema.DB] = Map.empty
  ): Future[Unit] = {
    log.info(s"RunOp: $op")
    op match {
      case StartTraffic =>
        testRunner.run(cluster, dbMap)

      case StopTraffic =>
        testRunner.reset()

      case SlackMsg(title, fields) =>
        recorder.slackMsg(title, fields)

      case Annotate(title, fields, extraTags) =>
        recorder.annotate(title, fields, extraTags)

      case Pause(duration) =>
        config.timer.delay(duration)(Future.unit)

      case Repeat(ops, deadline) =>
        runOps(ops, cluster, dbMap, deadline) flatMap { _ =>
          runOp(op, cluster, dbMap)
        }

      case Squelch(flag, hosts) =>
        testRunner.squelch(flag, hosts)

      case Remote(cmds, hosts) =>
        val nodes = operators.limit(hosts)
        cmds.foldLeft(Future.unit) { (f, cmd) =>
          f flatMap { _ =>
            nodes.all(OperatorReq.RunCmd(cmd)) flatMap { reps =>
              val failures = reps collect {
                case (host, OperatorRep.Failure(_, _)) => host
              }
              if (failures.nonEmpty) {
                Future.failed(RunCmdFailedException(cmd, failures))
              } else {
                Future.unit
              }
            }
          }
        }

      case Verify(cmd, hosts, reduce) =>
        val nodes = operators.limit(hosts)
        val responses = nodes.all(OperatorReq.RunCmd(cmd))
        responses map { reps => reduce(reps.toMap) } map {
          case Left(message) => throw new RuntimeException(message)
          case Right(_)      => ()
        }
    }
  }
}
