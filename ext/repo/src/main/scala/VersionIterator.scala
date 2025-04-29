package fauna.repo

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.doc._
import fauna.storage._
import fauna.storage.api.MVTMap
import fauna.storage.cassandra.CassandraIterator
import io.netty.buffer._

/** Composes a CassandraIterator with an MVTMap to produce document
  * histories which respect both a snapshot time and min. valid time
  * from an offline snapshot of the Versions column family.
  */
final class VersionIterator(iter: CassandraIterator, mvt: Map[ScopeID, MVTMap])
    extends Iterator[Version] {

  require(iter.cfs.name == Tables.Versions.CFName)

  // C* Iterator will hand rows in CF order. Keep the key bytes around
  // to detect when moving to a new row.
  private[this] var currentKey: ByteBuf = Unpooled.EMPTY_BUFFER

  // Save off the MVT for the current row's Collection to avoid
  // needless Map lookups.
  private[this] var currentMVT: Option[Timestamp] = None

  // Once a row's GC edge has been observed, the remainder of the row
  // should be consumed from the C* Iterator without yielding any new
  // Versions - they're all dead, Jim.
  private[this] var dropRemainder: Boolean = false

  // Stash the next Version to return in the typical case where
  // something like this happens:
  // while (iter.hasNext) { val v = iter.next(); ... }
  private[this] var actual: Version = _

  def hasNext: Boolean = {
    if (actual ne null) {
      return true
    }

    actual = computeNext()
    actual ne null
  }

  def next(): Version = {
    val toReturn = if (actual eq null) {
      computeNext()
    } else {
      actual
    }

    require(toReturn ne null)

    actual = null
    toReturn
  }

  @annotation.tailrec
  private def computeNext(): Version =
    if (iter.hasNext) {
      val (key, cell) = iter.next()

      // New row.
      if ((currentKey eq null) || key != currentKey) {
        currentKey = key
        dropRemainder = false

        val (scope, id) = Tables.Versions.decodeRowKey(key)
        currentMVT = mvt.get(scope) flatMap {
          _.apply(id.collID)
        }
      }

      if (dropRemainder) {
        computeNext()
      } else {
        val version = Version.decodeCell(key, cell)

        currentMVT match {
          // NB. Pass unknown collections through untouched.
          case None                                  => version
          case Some(mvt) if version.ts.validTS > mvt => version
          case Some(mvt)                             =>
            // Consume the remainder of this row and move on.
            dropRemainder = true

            // Rewrite this version, it's the GC edge. See
            // DocHistory.applyGCRules() for the online read path
            // equivalent.
            if (!version.isDeleted && version.ttl.forall { _ >= mvt }) {
              Version.Live(
                version.parentScopeID,
                version.id,
                version.ts,
                Create,
                version.schemaVersion,
                version.data)
            } else {
              computeNext()
            }
        }
      }
    } else {
      null
    }
}
