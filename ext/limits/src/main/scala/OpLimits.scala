package fauna.limits

object OpLimit {
  def MaxValue = OpLimit(Double.MaxValue, Double.MaxValue)
}

final case class OpLimit(permitsPerSecond: Double, burstSeconds: Double)

final case class OpsLimits(read: OpLimit, write: OpLimit, compute: OpLimit) {
  override def toString = s"OpsLimits(read:$read, write:$write, compute:$compute)"
}
