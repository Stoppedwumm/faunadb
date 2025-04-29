package fauna.qa.operator

import fauna.codex.cbor.CBOR
import fauna.qa.Host
import java.nio.file.Path
import scala.sys.process._

case class RunCmdFailedException(cmd: Cmd, hosts: Seq[Host])
    extends Exception(s"Command Failed. $cmd on $hosts")

/** A shell command DSL providing |, &, and chdir */
sealed class Cmd(val shell: Shell) {
  def apply(logger: ProcessLogger): Int = run(logger).exitValue()

  def run(logger: ProcessLogger): Process =
    shell.mkProcess(None).run(logger)

  def shellString = shell.cmdString
}

object Cmd {
  import fauna.qa.operator.Shell._

  implicit val Codec: CBOR.Codec[Cmd] =
    CBOR.SumCodec[Cmd](
      CBOR.RecordCodec[Raw],
      CBOR.SingletonCodec(CreateDirs),
      RPM.Codec,
      Admin.Codec,
      Proc.Codec,
      Time.Codec,
      Network.Codec,
      Etc.Codec
    )

  case class Raw(cmd: String) extends Cmd(Bash(cmd))

  case object CreateDirs
      extends Cmd(
        And(
          Bash("mkdir -p /media/ephemeral/fauna/log"),
          Bash("mkdir -p /media/ephemeral/fauna/tmp"),
          Bash("chown -R faunadb.faunadb /media/ephemeral/fauna")
        )
      )

  sealed class RPM(s: Shell) extends Cmd(s)

  object RPM {

    val Codec: CBOR.Codec[RPM] = CBOR.SumCodec[RPM](
      CBOR.SingletonCodec(Remove),
      CBOR.RecordCodec[Install],
      CBOR.RecordCodec[Downgrade]
    )

    case object Remove
        extends RPM(
          And(
            Proc.StopNode.shell,
            Bash("yum remove -y faunadb"),
            Bash("rm -rf /usr/share/fauna")
          )
        )

    private def InstallCmd(action: String, vers: String) =
      vers match {
        case "stable" | "unstable" =>
          Bash(
            s"yum --disablerepo=* --enablerepo=fauna-core-$vers $action -y faunadb"
          )
        case _ =>
          Bash(s"yum --disablerepo=* --enablerepo=fauna-* $action -y faunadb-$vers")
      }

    case class Install(vers: String)
        extends RPM(
          And(
            Bash("yum clean metadata --disablerepo=* --enablerepo=fauna-* -q"),
            Bash(
              "yum check-update --disablerepo=* --enablerepo=fauna-* -q; /usr/bin/test $? -ne 1"
            ), //yum check-update returns 100 on "success but updates available"
            InstallCmd("install", vers),
            CreateDirs.shell
          )
        )

    case class Downgrade(vers: String) extends RPM(InstallCmd("downgrade", vers))
  }

  sealed class Admin(s: Shell) extends Cmd(s)

  object Admin {

    val Codec: CBOR.Codec[Admin] = CBOR.SumCodec[Admin](
      CBOR.RecordCodec[InitCluster],
      CBOR.RecordCodec[JoinToCluster],
      CBOR.RecordCodec[MoveNode],
      CBOR.RecordCodec[UpdateReplica],
      CBOR.SingletonCodec(Repair),
      CBOR.RecordCodec[ForceRemoveNode],
      CBOR.RecordCodec[RemoveNode],
      CBOR.SingletonCodec(UpdateStorage),
      CBOR.SingletonCodec(GetStatus),
      CBOR.SingletonCodec(CleanStorage)
    )

    private def AdminCmd(cmd: String, args: String*) =
      Bash((Seq("faunadb-admin", cmd) ++ args).mkString(" "))

    private def mkAdminCmd(cmd: String, args: String*) =
      And(Proc.WaitForAdmin.shell, AdminCmd(cmd, args: _*))

    case class InitCluster(replica: String)
        extends Admin(mkAdminCmd("init", s"-r $replica"))

    case class JoinToCluster(seed: String, replica: String)
        extends Admin(mkAdminCmd("join", seed, s"-r $replica"))

    case class MoveNode(seed: String) extends Admin(mkAdminCmd("move", seed))

    case class UpdateReplica(rtype: String, replicas: Vector[String])
        extends Admin(mkAdminCmd("update-replica", rtype +: replicas: _*))

    case object CleanStorage extends Admin(mkAdminCmd("debug", "clean-storage"))

    case object Repair extends Admin(mkAdminCmd("repair"))

    case class ForceRemoveNode(target: String, seed: String)
        extends Admin(
          And(
            Proc.ClearNode.shell,
            Pipe(
              AdminCmd(s"-H $seed:8444", "host-id", target),
              Bash("tail -n 1"),
              Xargs(
                AdminCmd(s"-H $seed:8444", "--force", "--allow-unclean", "remove"))
            )
          )
        )

    case class RemoveNode(target: String, seed: String)
        extends Admin(
          Pipe(
            AdminCmd(s"-H $seed:8444", "host-id", target),
            Bash("tail -n 1"),
            Xargs(AdminCmd(s"-H $seed:8444", "remove"))
          )
        )

    case object UpdateStorage extends Admin(mkAdminCmd("update-storage-version"))

    case object GetStatus extends Admin(mkAdminCmd("status"))
  }

  sealed class Proc(s: Shell) extends Cmd(s)

  object Proc {

    val Codec: CBOR.Codec[Proc] = CBOR.SumCodec[Proc](
      CBOR.RecordCodec[SignalFauna],
      CBOR.RecordCodec[WaitForPing],
      CBOR.RecordCodec[RestartNode],
      CBOR.SingletonCodec(WaitForAdmin),
      CBOR.SingletonCodec(WaitForTopology),
      CBOR.SingletonCodec(WaitForRepair),
      CBOR.SingletonCodec(EraseData),
      CBOR.SingletonCodec(StartNode),
      CBOR.SingletonCodec(StopNode),
      CBOR.SingletonCodec(ClearNode),
      CBOR.SingletonCodec(WaitForCleanup)
    )

    case class SignalFauna(signal: Int)
        extends Proc(Bash(s"pkill -f faunadb.jar -$signal"))

    case class WaitForPing(isSecure: Boolean = false)
        extends Proc(
          Bash(
            s"while ! $$(curl --max-time 3 --output /dev/null --silent --fail -k http${if (isSecure) "s" else ""}://localhost:8443/ping); do sleep 0.1; done"
          )
        )

    // TODO: should we be code golfing in some sort of timeout / max retry limit
    // here?
    case object WaitForAdmin
        extends Proc(
          Bash(
            "while ! netstat -tna | grep 'LISTEN\\>' | grep -q ':8444\\>'; do sleep 0.1; done"
          )
        )

    case object WaitForTopology
        extends Proc(
          Bash(
            "sleep 20; while ! faunadb-admin movement-status | grep -q 'No data movement'; do sleep 10; done"
          )
        )

    case object WaitForRepair
        extends Proc(
          Bash(
            "sleep 20; while ! faunadb-admin repair-status | grep -q 'Repair idle.'; do sleep 10; done"
          )
        )

    case object WaitForCleanup
        extends Proc(Bash(
          "sleep 20; while ! faunadb-admin debug needs-cleanup 2>/dev/null | grep -v WARN | jq .resources.clean | grep false; do sleep 10; done"))
    case object EraseData extends Proc(Bash(s"rm -rf /media/ephemeral/fauna/*"))

    private def SystemCtlFauna(cmd: String) =
      Bash(s"systemctl $cmd faunadb")

    case object StartNode extends Proc(Or(SystemCtlFauna("start"), Bash("true")))

    case object StopNode extends Proc(Or(SystemCtlFauna("stop"), Bash("true")))

    case class RestartNode(isSecure: Boolean = false)
        extends Proc(And(SystemCtlFauna("restart"), WaitForPing(isSecure).shell))

    case object ClearNode extends Proc(And(StopNode.shell, EraseData.shell))
  }

  sealed class Time(s: Shell) extends Cmd(s)

  object Time {

    val Codec: CBOR.Codec[Time] = CBOR.SumCodec[Time](
      CBOR.SingletonCodec(DisableNTP),
      CBOR.SingletonCodec(EnableNTP),
      CBOR.RecordCodec[AdjustTime]
    )

    case object DisableNTP extends Time(Bash("systemctl stop chronyd"))

    case object EnableNTP extends Time(Bash("systemctl start chronyd"))

    case class AdjustTime(millis: Long)
        extends Time(
          Pipe(
            Bash("date +%s%N"),
            Bash("awk {print $1 + " + millis.toString + " }"),
            Bash("date -s +%s%N")
          )
        )
  }

  sealed class Network(s: Shell) extends Cmd(s)

  object Network {

    val Codec: CBOR.Codec[Network] = CBOR.SumCodec[Network](
      CBOR.SingletonCodec(Heal),
      CBOR.RecordCodec[DropByIP],
      CBOR.RecordCodec[PacketDelay],
      CBOR.RecordCodec[PacketLoss],
      CBOR.SingletonCodec(Fast),
      CBOR.SingletonCodec(Reset)
    )

    case object Heal
        extends Network(And(Bash("iptables -F -w"), Bash("iptables -X -w")))

    case class DropByIP(ip: String)
        extends Network(Bash(s"iptables -A INPUT -s $ip -j DROP -w"))

    case class PacketDelay(delayMillis: Long, jitterMillis: Long)
        extends Network(
          Bash(
            s"tc qdisc add dev eth0 root netem delay ${delayMillis}ms ${jitterMillis}ms"
          )
        )

    case class PacketLoss(percent: Int)
        extends Network(Bash(s"tc qdisc add dev eth0 root netem loss ${percent}ms"))

    case object Fast extends Network(Bash(s"tc qdisc del dev eth0 root"))

    // the "; true" is to handle non-zero exit codes from tc if no tc rules have
    // been added to eth0.
    case object Reset
        extends Network(And(Heal.shell, Bash(s"tc qdisc del dev eth0 root; true")))
  }

  sealed class Etc(s: Shell) extends Cmd(s)

  object Etc {

    implicit val PathCodec =
      CBOR.AliasCodec[Path, String](Path.of(_), _.toString)

    val Codec: CBOR.Codec[Etc] = CBOR.SumCodec[Etc](
      CBOR.RecordCodec[Echo],
      CBOR.RecordCodec[TouchFile],
      CBOR.RecordCodec[DeleteFile],
      CBOR.RecordCodec[ExitCode],
      CBOR.SingletonCodec(NoOp),
      CBOR.RecordCodec[Sleep]
    )

    // Mostly-useless messages for testing
    case class Echo(str: String) extends Etc(Bash(s"echo $str"))

    case class TouchFile(file: Path) extends Etc(Bash(s"touch ${file.toString}"))

    case class DeleteFile(file: Path) extends Etc(Bash(s"rm ${file.toString}"))

    case class ExitCode(code: Int)
        extends Etc(Bash(s""">&2 echo "exiting with $code"; exit $code"""))

    case object NoOp extends Etc(ExitCode(0).shell)

    case class Sleep(seconds: Int) extends Etc(Bash(s"sleep $seconds"))
  }
}
