package fauna.repo.store

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.repo.query.Query
import fauna.storage.{ Tables, Value }
import fauna.storage.api.HealthChecks
import fauna.storage.ops.InsertDataValueWrite
import scala.concurrent.duration._

object HealthCheckStore extends ExceptionLogging {
  type Key = Tables.HealthChecks.Key

  val schema = Tables.HealthChecks.Schema

  val TTL = Some(30.seconds)

  val PollPageSize = 1024

  private val Root = "HealthCheckRoot"

  def getAll: Query[Map[HostID, Timestamp]] = Query.snapshotTime flatMap {
    snapshotTS => Query.read(HealthChecks(snapshotTS)) map { _.values toMap }
  }

  def insert(id: HostID, ts: Timestamp): Query[Unit] = {
    val localKey = (Root, CBOR.encode(id).toByteArray)
    val data = CBOR.encode(ts)
    val value = new Value[Key](localKey, data)
    val k = value.keyPredicate.uncheckedRowKeyBytes
    val c = value.keyPredicate.uncheckedColumnNameBytes
    val write = InsertDataValueWrite(schema.name, k, c, value.data, TTL)
    Query.write(write)
  }
}
