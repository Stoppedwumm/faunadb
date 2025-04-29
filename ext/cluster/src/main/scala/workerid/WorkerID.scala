package fauna.cluster.workerid

import fauna.codex.cbor.CBOR

object AssignedWorkerID {
  implicit val codec = CBOR.AliasCodec[AssignedWorkerID, Int](AssignedWorkerID.apply, _.id)

  def apply(id: Int): AssignedWorkerID = {
    require(id >= WorkerID.MinValue)
    require(id <= WorkerID.MaxValue)
    new AssignedWorkerID(id)
  }
}

object WorkerID {
  val ValueBits = 10 // 0 to 1023, 1024 workers max.
  val MinValue = 0
  val MaxValue = -1 ^ (-1 << ValueBits)

  implicit val codec = CBOR.SumCodec[WorkerID](
    AssignedWorkerID.codec,
    CBOR.SingletonCodec(UnassignedWorkerID),
    CBOR.SingletonCodec(UnavailableWorkerID)
  )
}

sealed abstract class WorkerID {
  def idOpt: Option[Int]
}

case class AssignedWorkerID(id: Int) extends WorkerID {
  def idOpt = Some(id)
}

case object UnassignedWorkerID extends WorkerID {
  def idOpt = None
}

case object UnavailableWorkerID extends WorkerID {
  def idOpt = None
}
