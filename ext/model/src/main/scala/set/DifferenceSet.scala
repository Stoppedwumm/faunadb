package fauna.model

import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model.Difference.zeroSet
import fauna.repo.query.Query

case class Difference(sets: List[EventSet]) extends AbstractAlgebraicSet {

  def isPresent(memberships: Set[Int]) = memberships == zeroSet

  def isFiltered: Boolean = sets.exists { _.isFiltered }

  def filteredForRead(auth: Auth) =
    sets match {
      case Nil => Query.some(this)
      case min :: subs =>
        val minQ = min.filteredForRead(auth)
        val subsQ = (subs map { _.filteredForRead(auth) } sequence)

        (minQ, subsQ) par {
          case (None, _)       => Query.none
          case (Some(m), subs) => Query.some(Difference(m :: subs.flatten.toList))
        }
    }
}

object Difference {
  val zeroSet = Set(0)
}
