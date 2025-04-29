package fauna.tx.consensus

import fauna.atoms.HostID
import fauna.tx.log.{ LogEntry, TX }

sealed trait ReplicatedLogEntry[+V] extends LogEntry[TX, V] {
  val term: Term
  val token: Long
  def isMeta: Boolean
}

object ReplicatedLogEntry {
  final case class Value[+V](idx: TX, term: Term, token: Long, get: V) extends ReplicatedLogEntry[V] {
    def isEmpty = false
    def isMeta = false
    def toOption = Some(get)
  }

  final case class Null(idx: TX, term: Term, token: Long) extends ReplicatedLogEntry[Nothing] {
    def isEmpty = true
    def isMeta = false
    def get = throw new NoSuchElementException("no value in null log entry.")
    def toOption = None
  }

  sealed trait MemberChange extends ReplicatedLogEntry[Nothing] {
    def isEmpty = true
    def isMeta = true
    def toOption = None
    def get = throw new NoSuchElementException("no value in meta log entry.")
    val member: HostID
  }

  final case class AddMember(idx: TX, term: Term, token: Long, member: HostID) extends MemberChange

  final case class RemoveMember(idx: TX, term: Term, token: Long, member: HostID) extends MemberChange
}
