package fauna.tx

import fauna.atoms.HostID
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.tx.consensus.log.Entry
import fauna.tx.log._
import java.lang.{ Long => JLong }

/**
  * The replicated log algorithm in this package is based on RAFT, by
  * Diego Ongaro. His dissertation describing RAFT in detail can be
  * found at https://github.com/ongardie/dissertation
  *
  * In short, RAFT guarantees correctness by maintaining the following
  * invariants at all times:
  *
  * - Election Safety: At most one leader can be elected in a given
  *   term.
  *
  * - Leader Append-Only: A leader never overwrites or deletes entries
  *   in its log; it only appends new entries.
  *
  * - Log Matching: If two logs contain an entry with the same index
  *   and term, then the logs are identical in all entries up through
  *   the given index.
  *
  * - Leader Completeness: If a log entry is committed in a given
  *   term, then that entry will be present in the logs of the leaders
  *   for all higher-numbered terms.
  *
  * - State Machine Safety: If a server has applied a log entry at a
  *   given index to its state machine, no other server will ever
  *   apply a different log entry for the same index.
  *
  *
  * This implementation differs from the basic RAFT algorithm as
  * described in the paper in the following ways:
  *
  *
  * ## Election
  *
  * In basic RAFT, a node begins an election for a new leader once it
  * has not received a message from the leader after a timeout. As
  * starting an election requires incrementing the current term, the
  * election will continue and a new leader will be chosen, even if
  * the existing leader was correctly functioning.
  *
  * This happens, for example, if a node is partitioned off from its
  * peers. It will start a new election after being cut off. Because
  * it has increased its term, once the partition is healed it will
  * disrupt the functioning leader. By waiting for a quorum of nodes
  * to broadcast their inability to contact the leader, a node in the
  * ring will not disrupt an existing leader by starting an
  * unnecessary election.
  *
  * This implementation avoids RAFT's deadlocks and timeout
  * sensitivity by electing nodes slightly differently: When a node
  * receives a vote request, like RAFT, it will give its vote if:
  *
  * 1. It has not voted for another node in the same term.
  *
  * 2. The requester's last log tx/term is higher.
  *
  * However, in the case where 2 is not satisfied because the node
  * knows that its log is more advanced, it immediately starts a new
  * election for itself in the next term.
  *
  * This may happen multiple times in succession, for example node 'a'
  * may start an election which is seen by a further ahead node 'b'.
  * Node 'b' will immediately start an election for itself at the next
  * term. An even further ahead node 'c', upon receiving 'b's vote
  * request, would start a 3rd election at the next higher term.
  *
  * Like RAFT, if a node receives votes from a quorum of nodes, it
  * begins issuing Appends. Since only one node can be elected leader
  * for any given term, if a node receives an Append, it accepts the
  * sender as leader for the term.
  *
  *
  * ## Replication, Entry Acceptance
  *
  * Rather than relying on the leader's Append messages to propagate
  * the committed TX to followers, nodes broadcast their acceptance of
  * appends to all of their peers, and can derive the latest committed
  * position from those accept messages. This does not violate the
  * Leader Completeness invariant, because while nodes learn of
  * commitment via Append messages, entries are *in fact* committed
  * when they are received by a majority of nodes.
  *
  *
  * ## Membership Change
  *
  * This implementation uses the simpler single-server membership
  * change algorithm that was introduced in Diego's RAFT dissertation,
  * along with the fix to prevent the bug described in
  * https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E from
  * happening.
  *
  */

package consensus {
  object Term {
    val MinValue = Term(0)
    val MaxValue = Term(Long.MaxValue)

    implicit val codec = CBOR.TupleCodec[Term]
  }

  final case class Term(toLong: Long) extends AnyVal {
    def incr = Term(toLong + 1)

    def compare(other: Term): Int = JLong.compare(toLong, other.toLong)
    def max(o: Term): Term = if (toLong > o.toLong) this else o
    def min(o: Term): Term = if (toLong < o.toLong) this else o
  }

  object Pos {
    implicit val codec = CBOR.TupleCodec[Pos]

    val MinValue = Pos(TX.MinValue, Term.MinValue)
    val MaxValue = Pos(TX.MaxValue, Term.MaxValue)
  }

  final case class Pos(idx: TX, term: Term) extends Ordered[Pos] {

    override def toString = s"Pos(${idx.toLong}, ${term.toLong})"

    def compare(other: Pos) =
      term compare other.term match {
        case 0   => idx compare other.idx
        case cmp => cmp
      }

    def max(o: Pos): Pos = if (this > o) this else o
    def min(o: Pos): Pos = if (this < o) this else o
  }

  final class RejectedEntryException(token: Long, within: TX) extends Exception(
    s"Failed to commit entry for token $token by log index $within.")

  package object messages {
    type Token = Long
    type Entries = Vector[Entry]
    type ProposalMeta = Vector[(TX, Timestamp)]
  }

  package messages {
    object Message {
      implicit val CBORCodec = CBOR.SumCodec[Message](
        CBOR.TupleCodec[SyncProposals],
        CBOR.TupleCodec[RequestProposals],
        CBOR.TupleCodec[Proposals],
        CBOR.DefunctCodec(Unused1),
        CBOR.TupleCodec[Reinit],
        CBOR.DefunctCodec(Unused2),
        CBOR.TupleCodec[AcceptAppend],
        CBOR.TupleCodec[Sync],
        CBOR.TupleCodec[Append],
        CBOR.TupleCodec[CallElection],
        CBOR.TupleCodec[RequestVote],
        CBOR.TupleCodec[Vote],
        CBOR.TupleCodec[Reset],
        CBOR.TupleCodec[RequestAppend])
    }

    sealed abstract class Message { val term: Term; val leader: Option[HostID] = None }

    // every poll interval send one page of tokens to sync (max 512 per page?)
    case class SyncProposals(term: Term, tokens: Vector[Token]) extends Message

    // leader adds missing proposals to a queue. every poll interval ask
    // for one page of tokens (max 16 per page? with StreamContexts
    // ask for 512 and pull bytes incrementally)
    case class RequestProposals(term: Term, tokens: Vector[Token]) extends Message
    case class Proposals(term: Term, after: Pos, entries: Entries, meta: ProposalMeta) extends Message

    // request another node to append entries
    case class Append(term: Term, committed: TX, accepted: Pos, prev: Pos, entries: Entries, token: Long = -1) extends Message {
      def last = if (entries.isEmpty) prev else Pos(prev.idx + entries.size, entries.last.term)
      override def toString = s"Append($term, c $committed, acc $accepted, p $prev, l $last, ${entries.size}, tok $token)"
    }

    // message to notify others a node has appended (and fsynced/flushed) entries
    // up to a certain position to its log
    case class AcceptAppend(term: Term, acc: Pos, token: Option[Token]) extends Message
    case class RequestAppend(term: Term, after: Pos, count: Int, token: Long = -1) extends Message
    case class Reinit(term: Term, prev: Pos, ring: Vector[HostID], ringIdx: TX) extends Message

    // request a node to send back an up to date `AcceptAppend` message
    case class Sync(term: Term, token: Token) extends Message

    case class CallElection(term: Term, immediate: Boolean) extends Message
    case class RequestVote(term: Term, lastPos: Pos) extends Message
    case class Vote(term: Term, vote: HostID, votePos: Pos) extends Message

    case class Reset(term: Term, override val leader: Option[HostID]) extends Message

    // Placeholder for no longer used command in SumCodec
    sealed trait Unused extends Message { val term = Term.MinValue }
    case object Unused1 extends Unused
    case object Unused2 extends Unused
  }
}
