package fauna.storage

import scala.collection.immutable.ArraySeq

/**
  * Two mutations conflict if they have the same key and valid time,
  * but different transaction times. The mutation with a greater
  * transaction time wins (last write wins).
  */
trait Mutation extends Any {
  def ts: BiTimestamp
  def action: Action

  // previously, the transaction time component of version cell names
  // was a snowflake id - this flag indicates that condition. Changes
  // always lose conflicts with non-changes.
  def isChange: Boolean
}

/**
  * Conflicts should be removed as they are discovered, in favor of
  * the canonical T.
  *
  * Reads should always resolve conflicts which may arise, and only
  * return the canonical T.
  *
  * Conflict removal happens asynchronously as document TTLs and
  * history retention policies are applied to the corpus. See
  * `DocGarbageCollection`.
  */
final case class Conflict[+T <: Mutation](canonical: T, conflicts: List[T]) {
  def ts = canonical.ts
}

object Conflict {
  /**
    * Resolves conflicts between mutations with the same key by
    * choosing the winner based on the highest id.
    *
    * Expects an iterable of mutations sorted by key, whether ascending
    * or descending. Preserves the order of mutations within the
    * Iterable.
    */
  def resolve[T <: Mutation](muts: Iterable[T])(implicit
    eq: Equiv[T]): Seq[Conflict[T]] = {
    if (muts.isEmpty) {
      Nil
    } else {
      val rv = ArraySeq.newBuilder[Conflict[T]]
      val last = muts.iterator.drop(1).foldLeft(Conflict[T](muts.head, Nil)) {
        case (Conflict(scrutinee, losers), mut) =>
          if (!eq.equiv(mut, scrutinee)) {
            // next row, add the batch and reset
            rv += Conflict(scrutinee, losers)
            Conflict(mut, Nil)
          } else if (mut.isChange && !scrutinee.isChange) {
            // scrutinee won
            Conflict(scrutinee, mut :: losers)
          } else if (scrutinee.isChange && !mut.isChange) {
            // scrutinee lost
            Conflict(mut, scrutinee :: losers)
          } else if (mut.ts.transactionTS > scrutinee.ts.transactionTS) {
            // scrutinee lost
            Conflict(mut, scrutinee :: losers)
          } else if (mut.ts == scrutinee.ts && mut.action > scrutinee.action) {
            // this is an unusual situation: two changes have the same
            // valid time and transaction time; this shouldn't
            // happen. however, the schema orders actions, so resolve
            // that way.

            // scrutinee lost
            Conflict(mut, scrutinee :: losers)
          } else {
            // scrutinee won
            Conflict(scrutinee, mut :: losers)
          }
      }

      // add in the last batch
      rv += last

      rv.result()
    }
  }
}
