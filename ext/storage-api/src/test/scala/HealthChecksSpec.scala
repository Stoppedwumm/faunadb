package fauna.storage.api.test

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.lang._
import fauna.lang.syntax._
import fauna.prop.Prop
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.api.HealthChecks
import fauna.storage.ops._

class HealthChecksSpec extends Spec("healthchecks") {
  once("works") {
    for {
      ts0 <- Prop.timestamp()
      ts1 <- Prop.timestampAfter(ts0)
      ts2 <- Prop.timestampAfter(ts1)
      ts3 <- Prop.timestampAfter(ts2)
    } yield {
      withStorageEngine(StatsRecorder.Null) { engine =>
        def insertHealthCheck(hc: (HostID, Timestamp)) = {
          val (id, ts) = hc
          val key = (HealthChecks.Root, CBOR.encode(id).toByteArray)
          val value = new Value[Tables.HealthChecks.Key](key, CBOR.encode(ts))
          val k = value.keyPredicate.uncheckedRowKeyBytes
          val c = value.keyPredicate.uncheckedColumnNameBytes
          val write = InsertDataValueWrite(
            Tables.HealthChecks.Schema.name,
            k,
            c,
            value.data,
            None)
          applyWrites(engine, ts, write)
        }

        val id0 = HostID.NullID // So id0 < id1.
        val id1 = HostID.randomID
        insertHealthCheck((id0, ts0))
        insertHealthCheck((id1, ts1))

        runHealthChecks(engine, HealthChecks(ts1)) should be(
          Vector((id0, ts0), (id1, ts1)))

        insertHealthCheck((id0, ts2))
        insertHealthCheck((id1, ts3))

        runHealthChecks(engine, HealthChecks(ts3)) should be(
          Vector((id0, ts2), (id1, ts3)))
      }
    }
  }
}
