package fauna.qa.net

import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp
import fauna.qa._
import fauna.qa.recorders.StatsSnapshot

/**
  * Request/Response messages meant for Worker agents.
  */
sealed trait WorkerReq

object WorkerReq {
  case object Reset extends WorkerReq

  case class InitData(
    cluster: Vector[CoreNode],
    schema: Schema.DB,
    generator: TestGenerator,
    clientIDs: Range
  ) extends WorkerReq {
    def parallelism: Int = clientIDs.size
  }

  case class Run(
    cluster: Vector[CoreNode],
    schema: Schema.DB,
    generator: TestGenerator,
    parallelism: Int
  ) extends WorkerReq

  case class ValidateState(
    cluster: Vector[CoreNode],
    schema: Schema.DB,
    generator: TestGenerator,
    snapshotTS: Timestamp
  ) extends WorkerReq

  case class Squelch(flag: Boolean, hosts: Vector[Host]) extends WorkerReq
  case object GetStats extends WorkerReq
  case object Ping extends WorkerReq

  implicit val Codec =
    CBOR.SumCodec[WorkerReq](
      CBOR.SingletonCodec(Reset),
      CBOR.RecordCodec[InitData],
      CBOR.RecordCodec[Run],
      CBOR.RecordCodec[Squelch],
      CBOR.SingletonCodec(GetStats),
      CBOR.SingletonCodec(Ping),
      CBOR.RecordCodec[ValidateState],
    )
}

sealed trait WorkerRep

object WorkerRep {
  case object Ready extends WorkerRep
  case object DataPrepared extends WorkerRep
  case class RunComplete(withErrors: Boolean) extends WorkerRep
  case class Squelched(flag: Boolean, hosts: Vector[Host]) extends WorkerRep
  case class Stats(snapshot: StatsSnapshot) extends WorkerRep
  case object Pong extends WorkerRep
  case class ValidateStatePassed(result: String) extends WorkerRep
  case class ValidateStateFailed(result: Vector[String]) extends WorkerRep
  case object ValidateStateNoop extends WorkerRep

  implicit val Codec =
    CBOR.SumCodec[WorkerRep](
      CBOR.SingletonCodec(Ready),
      CBOR.SingletonCodec(DataPrepared),
      CBOR.RecordCodec[RunComplete],
      CBOR.RecordCodec[Squelched],
      CBOR.RecordCodec[Stats],
      CBOR.SingletonCodec(Pong),
      CBOR.RecordCodec[ValidateStatePassed],
      CBOR.RecordCodec[ValidateStateFailed],
      CBOR.SingletonCodec(ValidateStateNoop),
    )
}
