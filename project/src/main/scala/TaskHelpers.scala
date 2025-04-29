package fauna.sbt

import sbt._

object TaskHelpers {
  def serializedAggregate(tasks: Seq[TaskKey[Unit]]) = {
    Def.taskDyn {
      @volatile var error: Throwable = null

      Def.sequential(
        tasks map { t =>
          Def.task {
            t.result.value match {
              case Inc(ex)  => error = ex
              case Value(_) => ()
            }
          }
        },
        Def.task {
          Option(error) foreach { throw _ }
        })
    }
  }
}
