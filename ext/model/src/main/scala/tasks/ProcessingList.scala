package fauna.model.tasks

import fauna.atoms.TaskID
import java.util.concurrent.ConcurrentHashMap

object ProcessingList {
  private sealed trait State
  private case object Processing extends State
  private case object Stealing extends State
}

/** A stateful list used to atomically mark tasks as processing or stealing in order
  * to coordinate access to the host's run queue between task executor and task
  * stealer.
  *
  * == Semantics ==
  *
  * 1. Tasks are marked by calling `tryMarkProcessing` or `tryMarkStealing` with a
  *    list of task IDs called "candidates".
  *
  * 2. Marking candidates returns a list of task IDs that were successfully marked.
  *    Tasks in the candidates list that are not in the result of the mark methods
  *    lost the race to mark.
  *
  * 3. The tasks _returned_ by the mark methods MUST be given to the `unmark` method
  *    after the computation is done.
  */
final class ProcessingList {
  import ProcessingList._

  private[this] val stateByTask = new ConcurrentHashMap[TaskID, State]

  def tryMarkProcessing(candidates: Iterable[TaskID]): Iterable[TaskID] =
    tryToPut(candidates, Processing)

  def tryMarkStealing(candidates: Iterable[TaskID]): Iterable[TaskID] =
    tryToPut(candidates, Stealing)

  def unmark(tasks: Iterable[TaskID]): Unit =
    tasks foreach { stateByTask.remove(_) }

  @inline
  private def tryToPut(tasks: Iterable[TaskID], state: State): Iterable[TaskID] =
    tasks filter { stateByTask.putIfAbsent(_, state) eq null }
}
