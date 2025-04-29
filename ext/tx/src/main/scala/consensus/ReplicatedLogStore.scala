package fauna.tx.consensus.log

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.tx.consensus.{ Term, Pos, Ring }
import fauna.tx.log._
import io.netty.buffer.ByteBufAllocator
import java.util.Arrays
import scala.collection.{ Set => ISet }
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Entry {
  implicit val entryCodec = CBOR.SumCodec[Entry](
    CBOR.TupleCodec[Null],
    CBOR.TupleCodec[Value],
    CBOR.TupleCodec[AddMember],
    CBOR.TupleCodec[RemoveMember]
  )

  def unapply(e: Entry) = Some((e.term, e.token))

  final case class Null(term: Term, token: Long) extends Entry {
    def isValue = false
    def withTerm(t: Term) = copy(term = t)
    def bytesSize = 16
  }

  // FIXME: use ByteBuf and wrangle refcounting
  final case class Value(term: Term, token: Long, value: Array[Byte]) extends Entry {
    def isValue = true
    def withTerm(t: Term) = copy(term = t)
    def bytesSize = 16 + value.length

    override def equals(o: Any) =
      o match {
        case o: Value => term == o.term && token == o.token && Arrays.equals(value, o.value)
        case _        => false
      }

    override def hashCode = term.hashCode * token.hashCode
  }

  sealed abstract class MemberChange extends Entry {
    def isValue = false
    def withTerm(t: Term): MemberChange
    def bytesSize = 16 + 16 // Assume HostID UUID
    val member: HostID
  }

  final case class AddMember(term: Term, token: Long, member: HostID) extends MemberChange {
    def withTerm(t: Term) = copy(term = t)
  }

  final case class RemoveMember(term: Term, token: Long, member: HostID) extends MemberChange {
    def withTerm(t: Term) = copy(term = t)
  }
}

sealed abstract class Entry {
  val term: Term
  val token: Long
  def isValue: Boolean
  def withTerm(t: Term): Entry

  def bytesSize: Int

  def isMeta = !isValue
}

object ReplicatedLogMeta {
  implicit val CBORCodec = CBOR.TupleCodec[ReplicatedLogMeta]
}

case class ReplicatedLogMeta(self: HostID, term: Term, ring: Ring, ringIdx: TX, prevTerm: Term)

// FIXME: extend LogStore.
case class ReplicatedLogStore(self: HostID, log: LogStore[TX, Entry]) extends LogLike[TX, Entry] {

  @inline
  private def encode[T: CBOR.Encoder](t: T) =
    CBOR.encode(ByteBufAllocator.DEFAULT.ioBuffer, t)

  type E = LogEntry[TX, Entry]

  @volatile private[this] var meta =
    log.metadata map { _ releaseAfter { CBOR.parse[ReplicatedLogMeta](_) } } getOrElse {
      val empty = ReplicatedLogMeta(self, Term.MinValue, Ring(Set.empty), TX.MinValue, Term.MinValue)
      log.metadata(encode(empty))
      empty
    }

  require(meta.self == self, s"Configured replica id $self does not equal configured id ${meta.self}")

  @volatile private[this] var currRing = ringFromLog()

  def prevPos = getPos(prevIdx)

  def lastPos = getPos(lastIdx)

  @volatile private [this] var _flushedLastPos = getPos(flushedLastIdx)
  def flushedLastPos = _flushedLastPos

  def uncommittedLastPos = getPos(uncommittedLastIdx)

  // LogLike

  def prevIdx = log.prevIdx

  def lastIdx = log.lastIdx

  def entries(after: TX) = log.entries(after max prevIdx)

  def poll(after: TX, within: Duration) = log.poll(after, within)

  def subscribe(after: TX, idle: Duration)(f: Log.Sink[TX, E])(implicit ec: ExecutionContext) =
    log.subscribe(after, idle)(f)

  // LogStore

  def uncommittedLastIdx = log.uncommittedLastIdx

  def flushedLastIdx = log.flushedLastIdx

  def uncommittedEntries(after: TX) =
    log.uncommittedEntries(after max prevIdx)

  // def add(t: Entry): TX

  def setTerm(t: Term) = {
    meta = meta.copy(term = t)
    log.metadata(encode(meta))
  }

  def updateCommittedIdx(idx: TX) = {
    val metaOpt = if (meta.ring != currRing) {
      meta = meta.copy(ring = ringFromLog(idx), ringIdx = idx)
      Some(encode(meta))
    } else {
      None
    }

    log.updateCommittedIdx(idx, metaOpt)
  }

  def truncate(toIdx: TX): Unit = {
    if (toIdx > lastIdx) {
      throw new IllegalArgumentException(s"$toIdx is greater than log's lastIdx $lastIdx")
    }

    if (toIdx > prevIdx) {
      meta = meta.copy(ringIdx = toIdx max meta.ringIdx, prevTerm = getTerm(toIdx).get)
      log.truncate(toIdx, Some(encode(meta)))
    }
  }

  // def discard(after: TX): Unit

  def reinit(prev: Pos, ring: Set[HostID], ringIdx: TX) =
    if (prev.idx > uncommittedLastIdx) {
      currRing = Ring(ring)
      meta = meta.copy(ring = currRing, ringIdx = ringIdx, prevTerm = prev.term)
      log.reinit(prev.idx, Some(encode(meta)))
      _flushedLastPos = getPos(flushedLastIdx)
    }

  def close() = log.close()

  def isClosed = log.isClosed

  // ReplicatedLogStore

  /**
    * The current ring is the ring state as of the last log entry:
    * committedIdx is not taken into account. Because the leader only
    * allows one uncommitted member change entry into the log at a
    * time, this means that any uncommitted ring will differ from the
    * committed ring by at most 1 node, and therefore all of their
    * possible quorum sets will share at least one node. See
    * single-server member change algorithm for details.
    */
  def ring = currRing

  def committedRing = (meta.ring, meta.ringIdx)

  def term = meta.term

  private def getEntry(idx: TX): Option[E] =
    if (idx <= prevIdx || idx > uncommittedLastIdx) {
      None
    } else {
      uncommittedEntries(idx - 1) releaseAfter { es =>
        if (es.hasNext) {
          val e = es.next()
          assert(e.idx == idx)
          Some(e)
        } else {
          None
        }
      }
    }
  //FIXME: this and getPos are the only use of getEntry. We could
  //avoid IO by maintaining a TX -> Term index.
  def getTerm(idx: TX): Option[Term] =
    if (idx == prevIdx) Some(meta.prevTerm) else getEntry(idx) map { _.get.term }

  private def getPos(idx: TX): Pos =
    Pos(
      idx,
      if (idx == prevIdx) {
        meta.prevTerm
      } else {
        val entry = getEntry(idx) getOrElse {
          throw new IllegalStateException(
            s"No entry found at $idx (prev:$prevIdx, uncommittedLast:$uncommittedLastIdx, flushedLast:$flushedLastIdx, last:$lastIdx)")
        }
        entry.get.term
      }
    )

  def containsPos(pos: Pos) =
    getTerm(pos.idx).fold(false) { _ == pos.term }

  def getTokenIdx(tokens: ISet[Long], after: TX, within: TX): Map[Long, TX] =
    uncommittedEntries(after) takeWhile { _.idx <= within } releaseAfter { es =>
      val found = Map.newBuilder[Long, TX]

      es foreach { e =>
        if (tokens contains e.get.token) {
          found += (e.get.token -> e.idx)
        }
      }

      found.result()
    }

  /**
    * Appends entries to the log after the specified index. It is
    * important to note that if the provided entries already exist in
    * the log, it is left unmodified. This prevents delayed Append
    * messages from incorrectly causing later entries that were
    * accepted by the node from being discarded, violating the Leader
    * Completeness invariant.
    */
  def appendAfter(after: TX, newEntries: Seq[Entry]): Pos = {
    require(after <= uncommittedLastIdx, s"after $after greater than uncommittedLastIdx $uncommittedLastIdx")
    require(after >= prevIdx, s"after $after less than prevIdx $prevIdx")

    @annotation.tailrec
    def append0(idx: TX, as: Seq[LogEntry[TX, Entry]], bs: Seq[Entry]): (Pos, Boolean) =
      (as.headOption, bs.headOption) match {
        case (Some(a), Some(b)) if a.get.term == b.term => append0(idx + 1, as.tail, bs.tail)
        case (_, None)                                  => (getPos(idx), false)
        case _ =>
          val updateRing = idx < uncommittedLastIdx || (bs exists { _.isMeta })
          log.discard(idx)
          log.add(bs)
          (uncommittedLastPos, updateRing)
      }

    if (newEntries.isEmpty) {
      getPos(after)
    } else {
      val (acc, updateRing) = uncommittedEntries(after) releaseAfter { es =>
        append0(after, es.to(LazyList), newEntries)
      }

      // if the ring changed (or may have because entries were
      // discarded) update it.
      if (updateRing) {
        currRing = ringFromLog()
      }

      acc
    }
  }

  def flush() = {
    log.flush()
    _flushedLastPos = getPos(flushedLastIdx)
    _flushedLastPos
  }

  /**
    * Reset the log's ring to Set(self) by applying necessary
    * membership changes to the log.
    */
  def resetRing(): Unit = {
    val writes = Seq.newBuilder[Entry]

    val toRemove = currRing.all - self
    val toAdd = Set(self) -- currRing.all

    toRemove foreach { id => writes += Entry.RemoveMember(term, 0, id) }
    toAdd foreach { id => writes += Entry.AddMember(term, 0, id) }

    appendAfter(uncommittedLastIdx, writes.result())
    log.flush()
    updateCommittedIdx(flushedLastIdx)
  }

  private def ringFromLog(to: TX = uncommittedLastIdx) =
    uncommittedEntries(lastIdx max meta.ringIdx) releaseAfter { es =>
      Ring((es.lastIdx(to) foldLeft meta.ring.all) { (members, e) =>
        e.get match {
          case Entry.AddMember(_, _, id)    => members + id
          case Entry.RemoveMember(_, _, id) => members - id
          case _ => members
        }
      })
    }
}
