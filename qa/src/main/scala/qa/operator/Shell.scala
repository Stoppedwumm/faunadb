package fauna.qa.operator

import java.io.File
import scala.sys.process._

sealed trait Shell {
  def mkProcess(dir: Option[File]): ProcessBuilder
  def cmdString = mkProcess(None).toString
}

object Shell {

  case class Pipe(cmds: Seq[Shell]) extends Shell {

    def mkProcess(dir: Option[File]): ProcessBuilder =
      cmds map { _.mkProcess(dir) } reduceLeft { _ #| _ }
  }

  object Pipe {
    def apply(cmd: Shell, cmds: Shell*): Pipe = Pipe(cmd +: cmds)
  }

  case class And(cmds: Seq[Shell]) extends Shell {

    def mkProcess(dir: Option[File]): ProcessBuilder =
      cmds map { _.mkProcess(dir) } reduceLeft { _ #&& _ }
  }

  object And {
    def apply(cmd: Shell, cmds: Shell*): And = And(cmd +: cmds)
  }

  case class Or(cmds: Seq[Shell]) extends Shell {

    def mkProcess(dir: Option[File]): ProcessBuilder =
      cmds map { _.mkProcess(dir) } reduceLeft { _ #|| _ }
  }

  object Or {
    def apply(cmd: Shell, cmds: Shell*): Or = Or(cmd +: cmds)
  }

  case class Bash(cmd: String) extends Shell {

    def mkProcess(dir: Option[File]): ProcessBuilder =
      Process(Seq("bash", "-c", cmd), dir)
  }

  case class ChDir(dir: File, cmd: Shell) extends Shell {

    def mkProcess(d: Option[File]): ProcessBuilder =
      cmd.mkProcess(Some(dir))
  }

  case class Xargs(bash: Bash) extends Shell {

    def mkProcess(dir: Option[File]): ProcessBuilder =
      Bash(s"xargs ${bash.cmd}").mkProcess(dir)
  }
}
