package fauna.tx.test

import fauna.atoms.HostID
import fauna.exec.NamedThreadPoolExecutionContext
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.tx.consensus._
import fauna.tx.consensus.log.Entry
import fauna.tx.consensus.messages.Message
import fauna.tx.log.TX
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.util.UUID
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.{ Buffer, PriorityQueue }
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random
import scala.util.control.NonFatal

import TestExchange._

object TestExchange {
  type SIG = Int
  type V = String

  val MaxTicks = 10000
  val testEC = NamedThreadPoolExecutionContext("test.exchange", 10)

  case class Address(id: HostID, sig: SIG)

  case class Event(ts: Long, address: Address, msg: Option[(HostID, Message)])

  case class RunResult(elapsedMillis: Long, ticks: Long, messageCount: Long, maxIdx: List[TX], maxTerms: List[Term], err: Option[Throwable], log: Path)

  def repeat(
    msgLossRate: Double = 0,
    msgRepeatRate: Double = 0,
    msgDelay: Int = 1,
    msgDelayJitter: Int = 0,
    pollInterval: Int = 10,
    pollJitter: Int = 0,
    maxTicks: Int = 10000,
    checkInternalState: Boolean = true,
    times: Int = 1000)(f: TestExchange => Any)(implicit info: Informer): Unit = {

    val allStart = System.currentTimeMillis

    val runs = (1 to times) map { _ =>
      implicit val ec = testEC
      Future {
        val start = System.currentTimeMillis
        val dir = Files.createTempDirectory("test-exchange")
        val log = dir / "run.out"
        val out = Files.newBufferedWriter(log, StandardCharsets.UTF_8)

        def printLog(x: Any): Unit = {
          out.write(x.toString)
          out.newLine()
        }

        val ex = new TestExchange(
          dir,
          msgLossRate,
          msgRepeatRate,
          msgDelay,
          msgDelayJitter,
          pollInterval,
          pollJitter,
          maxTicks,
          checkInternalState,
          printLog)

        val err = try {
          f(ex)
          None
        } catch {
          case NonFatal(e) => Some(e)
        }

        val res = RunResult(System.currentTimeMillis - start, ex.ticks, ex.messageCount, ex.maxIdx, ex.maxTerms, err, log)

        ex.close()
        out.close()

        if (err.isEmpty) {
          dir.deleteRecursively()
        }

        res
      }
    } map {
      Await.result(_, Duration.Inf)
    }

    try {
      runs foreach { r =>
        if (r.err.isDefined) {
          println(new String(Files.readAllBytes(r.log), "UTF-8"))
          throw r.err.get
        }
      }
    } finally {
      info(s"$times repeats:")
      info("total time: %.3f ms" format (System.currentTimeMillis - allStart).toDouble)
      info("time avg: %.3f ms" format ((runs map { _.elapsedMillis } sum).toDouble / times))
      info("max ticks: %d" format (runs map { _.ticks } max))
      info("ticks avg: %.1f" format ((runs map { _.ticks } sum).toDouble / times))
      info("messages avg: %.1f" format ((runs map { _.messageCount } sum).toDouble / times))
      val maxIdx = ((runs map { _.maxIdx }).transpose map { _ map { _.toLong} max }).mkString("[", ",", "]")
      info(s"max log sizes: $maxIdx")
      val maxTerms = ((runs map { _.maxTerms }).transpose map { _ map { _.toLong} max }).mkString("[", ",", "]")
      info(s"max terms: $maxTerms")
    }
  }
}

class TestExchange(
  dir: Path,
  var msgLossRate: Double,
  var msgRepeatRate: Double,
  var msgDelay: Int,
  var msgDelayJitter: Int,
  var pollInterval: Int,
  var pollJitter: Int,
  var maxTicks: Int,
  val checkInternalState: Boolean,
  val log: Any => Unit) extends Matchers {

  case class Replica(dir: Path, log: ReplicatedLog[V], transport: TestTransport, ringReplica: Option[Replica])

  private[this] val states = Buffer.empty[Buffer[Replica]]

  private def statesForSig(sig: SIG) = states.lift(sig).getOrElse(List.empty[Replica])

  implicit private def hostIDToInt(id: HostID) = id.uuid.getLeastSignificantBits.toInt

  def replicas = replicasForSig(0)
  def replicasForSig(sig: SIG) = statesForSig(sig) map { _.log }

  var inTick = false
  private val sendbuf = List.newBuilder[(HostID, Address, Message, FiniteDuration)]

  val events = PriorityQueue.empty[Event](Ordering.by { _.ts * -1 })

  var committed = states flatMap { _ map { r => (r.transport.addr, r.log.entries(TX.MinValue) releaseAfter { _.toList }) } } toMap
  var messageCount = 0
  var currentTime = 0L
  var ticks = 0

  def nonMemberIDs = nonMemberIDsForSig(0)
  def nonMemberIDsForSig(sig: SIG) = statesForSig(sig).zipWithIndex collect { case (r, i) if !r.log.isRingMember => i } toSet

  def randomReplicaID = randomReplicaIDForSig(0)
  def randomReplicaIDForSig(sig: SIG) = Random.shuffle(statesForSig(sig).indices.toSet).head

  def randomMemberID = randomMemberIDForSig(0)
  def randomMemberIDForSig(sig: SIG) = Random.shuffle(statesForSig(sig).zipWithIndex collect { case (r, i) if r.log.isRingMember => i }).head

  def maxIdx: List[TX] = states.toList map { insts =>
    (insts foldLeft TX.MinValue) { case (t, i) => if (t > i.log.lastIdx) t else i.log.lastIdx }
  }

  def maxTerms: List[Term] = states.toList map { insts =>
    (insts map { _.log.term } foldLeft Term.MinValue) { _ max _ }
  }

  def close() = states foreach { _ foreach { _.log.close() } }

  def addReplica(): Unit = addReplicaForSig(0)
  def closeReplica(id: Int) = closeReplicaForSig(0, id)

  def addReplicaForSig(sig: SIG): Unit = addReplicaForSigInternal(sig, None)
  def addReplicaForSig(sig: SIG, ringSig: SIG): Unit = addReplicaForSigInternal(sig, Some(ringSig))

  def closeReplicaForSig(sig: SIG, id: Int): Unit = {
    states(sig)(id).log.close()
    states(sig).remove(id)
  }

  // Add a replica that optionally delegates its membership ring to another replica
  private def addReplicaForSigInternal(sig: SIG, ringSig: Option[SIG]): Unit = {
    while (states.size <= sig) states += Buffer.empty
    val id = HostID(new UUID(0, states(sig).size))
    val ringReplica = ringSig map { sig => states(sig)(id) }
    val inst = testReplica(id, sig, ringReplica)
    states(sig) += inst
  }

  def resetLeft(): Unit =
    states foreach { state =>
      state.zipWithIndex foreach {
        case (inst, id) =>
          if (!inst.log.isRingMember) {
            inst.log.close()
            inst.dir.deleteRecursively()
            state(id) = testReplica(inst.transport.addr.id, inst.transport.addr.sig, inst.ringReplica)
          }
      }
    }

  def restart(id: Int): Unit = restart(0, id)

  def restart(sig: SIG, id: Int): Unit =
    states(sig)(id) = restartedReplica(states(sig)(id))

  def tick(): Unit = synchronized {
    if (events.nonEmpty) {
      val Event(ts, Address(id, sig), msg) = events.dequeue()

      ticks += 1
      currentTime = ts

      inTick = true

      for(replicas <- states.lift(sig); inst <- replicas.lift(id)) {
        msg match {
          case Some((from, msg)) =>
            log(s"@$currentTime $from -> $id ($sig) : $msg")
            inst.transport.recvMessage(from, Future.successful(msg))

          case None =>
            log(s"@$currentTime $id ($sig) : tick")
            inst.transport.recvTick()
        }
      }

      inTick = false

      states foreach { _ foreach { r => log(s"    ${r.log}") } }
      sendbuf.result() foreach { case (f, Address(t, s), m, _) => log(s"    $f -> $t ($s): $m") }
      log("")

      sendbuf.result() foreach { case (f, t, m, a) => enqueueMessage(f, t, m, a) }
      sendbuf.clear()


      // send

      // Checks

      if (checkInternalState) {
        val leaders = replicas filter { _.isLeader } groupBy { _.term }

        leaders foreach { case (_, rs) => rs.size <= 1 should equal (true) }

        val prevTokens = committed.values.flatten map { _.token } toSet

        for (sigreplicas <- states; replica <- sigreplicas) {
          val addr = replica.transport.addr
          val newEntries = replica.log.entries(TX.MinValue) releaseAfter { _.toList }
          val prevEntries = committed.lift(addr) getOrElse Nil

          val tokens = newEntries map { _.token }
          val dupes = tokens groupBy identity filter { _._2.size > 1 }

          dupes.keys.isEmpty should be (true)

          prevEntries.to(LazyList) zip newEntries.to(LazyList) foreach { case (a, b) => a should equal (b) }
          committed = committed.updated(addr, newEntries)
        }

        val newTokens = committed.values.flatten map { _.token } toSet

        (prevTokens -- newTokens) should equal (Set.empty)

        if (committed.nonEmpty) {
          (committed groupBy { case (a, _) => a.sig }).values.map { _ map { case (_, e) => e } } foreach {
            _.reduceLeft[Seq[ReplicatedLogEntry[String]]] {
              case (as, bs) =>
                as.to(LazyList) zip bs.to(LazyList) foreach { case (a, b) => a should equal (b) }
                bs
            }
          }
        }
      }
    }
  }

  def resolveAll(): Unit = {
    // let things quiesce
    while (ticks < maxTicks && events.nonEmpty) tick()

    if (ticks == maxTicks) fail("Unresolved writes")

    log(s"RESOLVED ACTIVE STATES")
  }

  private def sendMessage(from: HostID, to: Address, msg: Message, after: FiniteDuration) = synchronized {
    if (inTick) {
      sendbuf += ((from, to, msg, after))
    } else {
      log(s"@$currentTime $from : add proposal")
      replicas foreach { r => log(s"    $r") }
      log(s"    $from -> ${to.id} (${to.sig}): $msg")
      log("")
      enqueueMessage(from, to, msg, after)
    }
  }

  private def enqueueMessage(from: HostID, to: Address, msg: Message, after: FiniteDuration) = {
    messageCount += 1
    if (Random.nextDouble() >= msgLossRate) {
      events.enqueue(Event(currentTime + after.toMillis.toInt + randomInterval(msgDelay, msgDelayJitter), to, Some(from -> msg)))
      if (Random.nextDouble() < msgRepeatRate) {
        events.enqueue(Event(currentTime + after.toMillis.toInt + randomInterval(msgDelay, msgDelayJitter), to, Some(from -> msg)))
      }
    } else {
      //log(s"$from -> $to : $msg *dropped*")
    }
  }

  private def randomInterval(base: Int, jitter: Int): Int =
    if (jitter <= 0) base else {
      base - (jitter / 2) + Random.nextInt(jitter)
    }

  class TestTransport(val addr: Address) extends ReplicatedLogTransport {

    val clock = Clock

    private[this] var handler: Option[Handler] = None

    def handle(h: Handler) = handler = Some(h)

    def close(): Unit = handler = None

    def isClosed: Boolean = handler.isEmpty

    def addEntry(entry: Entry, idx: TX, ts: Timestamp) =
      handler foreach { _.entries(Vector(entry), Vector(idx -> ts)) }

    def send(to: HostID, message: Message, after: FiniteDuration) =
      sendMessage(addr.id, Address(to, addr.sig), message, after)

    def schedule(from: HostID, message: Future[Message]) =
      handler foreach { _.message(from, message) }

    def scheduleTick(after: FiniteDuration) = {
      val (timeouts, evs) = events.dequeueAll partition { e => (e.address == addr) && e.msg.isEmpty }

      val timeout = if (timeouts.isEmpty) {
        List(Event(currentTime + randomInterval(after.toMillis.toInt, pollJitter), addr, None))
      } else {
        timeouts
      }

      events.enqueue(evs: _*)
      events.enqueue(timeout: _*)
    }

    def scheduleFlush(notify: Set[HostID]) =
      handler foreach { _.flush(notify) }

    def recvTick() =
      handler foreach { _.tick() }

    def recvMessage(sender: HostID, msg: Future[Message]) =
      handler foreach { h =>
        val reenqueue = h.message(sender, msg)
        if (reenqueue) sys.error("should not reenqueue in TestExchange")
      }
  }

  private def testReplica(id: HostID, sig: SIG, ringReplica: Option[Replica]) = {
    val transport = new TestTransport(Address(id, sig))
    val replicaDir = dir / s"$sig-$id"

    val log = ReplicatedLog[V].open(transport, id, replicaDir, "replog", pollInterval.millis,
      ringLog = ringReplica map { r => ReplicatedLog.handle(r.log) },
      txnLogBackupPath = None
      )

    Replica(replicaDir, log.asInstanceOf[ReplicatedLog[String]], transport, ringReplica)
  }

  private def restartedReplica(replica: Replica) = {
    val transport = new TestTransport(replica.transport.addr)
    val stats = replica.log.stats
    val log = new ReplicatedLog[V](
      transport,
      pollInterval.millis,
      replica.log.store,
      stats,
      "test",
      ringLog = replica.ringReplica map { _.log },
      leaderStateChangeListener = { _ => () },
      alwaysActive = false)

    Replica(replica.dir, log, transport, replica.ringReplica)
  }
}
