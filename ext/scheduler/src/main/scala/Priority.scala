package fauna.scheduler

import fauna.atoms._
import fauna.lang.PriorityTreeGroup
import scala.concurrent.Future

case class Priority(toInt: Int) extends AnyVal

object Priority {
  val MinValue = 1
  val MaxValue = 500
  val DefaultValue = MinValue
  val Default = Priority(MinValue)
}

case class PriorityGroup(id: GlobalID, treeGroup: PriorityTreeGroup)

object PriorityGroup {

  object TreeGroup {
    // this wont be necessary when PriorityGroups are FaunaClasses
    sealed abstract class StaticGroup(weight: Int)
        extends PriorityTreeGroup(new Object, Nil, weight)

    object Root extends StaticGroup(Priority.MaxValue)

    // Not a case object so that different priority groups with
    // default priority tree compare differently from each other.
    class Default extends StaticGroup(Priority.DefaultValue)
  }

  val Root = PriorityGroup(ScopeID.RootID, TreeGroup.Root)
  val Background = PriorityGroup(ScopeID.RootID, new TreeGroup.Default)
  val Default = PriorityGroup(ScopeID.RootID, new TreeGroup.Default)
}

trait PriorityProvider {
  def lookup(id: GlobalID): Future[PriorityGroup]
}

// to be used for testing purposes or when QoS is disabled
object ConstantPriorityProvider extends PriorityProvider {
  private val Default = Future.successful(PriorityGroup.Default)
  def lookup(id: GlobalID) = Default
}
