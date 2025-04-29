package fauna.qa.net

import fauna.codex.cbor.CBOR
import fauna.qa.operator._

/**
  * Request/Response messages meant for Operator agents.
  */
sealed trait OperatorReq

object OperatorReq {
  case object Reset extends OperatorReq
  case class RunCmd(cmd: Cmd) extends OperatorReq
  case object CreateConfig extends OperatorReq

  implicit val Codec =
    CBOR.SumCodec[OperatorReq](
      CBOR.SingletonCodec(Reset),
      CBOR.RecordCodec[RunCmd],
      CBOR.SingletonCodec(CreateConfig)
    )
}

sealed trait OperatorRep {
  def isSuccess: Boolean
  def isFailure: Boolean = !isSuccess
}

object OperatorRep {

  case object Ready extends OperatorRep {
    val isSuccess: Boolean = true
  }

  sealed trait OutputData {
    def out: Vector[String]
    def err: Vector[String]
    lazy val standardOutput = out mkString "\n"
    lazy val errorOutput = err mkString "\n"
  }

  case class Success(out: Vector[String], err: Vector[String]) extends OperatorRep with OutputData {
    val isSuccess: Boolean = true
  }

  case class Failure(out: Vector[String], err: Vector[String]) extends OperatorRep with OutputData {
    val isSuccess: Boolean = false
  }

  implicit val Codec =
    CBOR.SumCodec[OperatorRep](
      CBOR.SingletonCodec(Ready),
      CBOR.RecordCodec[Success],
      CBOR.RecordCodec[Failure]
    )
}
