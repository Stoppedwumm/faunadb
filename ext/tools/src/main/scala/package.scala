package fauna

import fauna.codex.json2.JSONWriter
import fauna.stats._
import fauna.storage.cassandra.Transformer
import fauna.storage.cassandra.SSTableIterator

package tools {

  trait OutputStats {
    val prefix: String
    val stats: StatsRecorder

    protected lazy val outputRowsKey = s"$prefix.Rows.Output"
    def incrOutputRows(): Unit = stats.incr(outputRowsKey)

    protected lazy val outputCellsKey = s"$prefix.Cells.Output"
    def incrOutputCells(cells: Long): Unit =
      stats.count(outputCellsKey, cells.toInt)
  }

  abstract class SSTableIteratorStats(val prefix: String)
      extends SSTableIterator.Stats {

    def stats: StatsRecorder

    private[this] lazy val runtimeKey = s"$prefix.Time"
    def recordRuntime(elapsedMillis: Long): Unit =
      stats.timing(runtimeKey, elapsedMillis)

    private[this] lazy val sstableBytesKey = s"$prefix.SSTable.Bytes"
    def recordSSTableBytes(bytes: Long): Unit =
      stats.count(sstableBytesKey, bytes.toInt)

    private[this] lazy val cellsKey = s"$prefix.Cells"
    def incrCells(count: Int = 1) = stats.count(cellsKey, count)

    private[this] lazy val cellsUnbornKey = s"$prefix.Cells.Unborn"
    def incrCellsUnborn(count: Int = 1) = stats.count(cellsUnbornKey, count)

    private[this] lazy val rowsKey = s"$prefix.Rows"
    def incrRows() = stats.incr(rowsKey)

    private[this] lazy val rowBytesKey = s"$prefix.Rows.Bytes"
    def recordRowBytes(bytes: Long) = stats.count(rowBytesKey, bytes.toInt)
  }

  final class DataExporterStats(prefix: String, _stats: StatsRecorder)
      extends SSTableIteratorStats(prefix)
      with OutputStats {

    // This class only buffers relatively-scalable LongAdders, and
    // passes all other metrics through to the ctor's
    // StatsRecorder. If this proves insufficient to resolve high
    // contention, buffer in a thread local, and merge at the end of
    // the export.

    private[this] val buffer = new StatsRequestBuffer

    val stats = new StatsRecorder {
      def count(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit = {
        _stats.count(key, value, tags)
        buffer.count(key, value, tags)
      }

      def incr(key: String): Unit = {
        _stats.incr(key)
        buffer.incr(key)
      }

      def decr(key: String): Unit = {
        _stats.decr(key)
        buffer.incr(key)
      }

      def set(key: String, value: Double) = _stats.set(key, value)
      def set(key: String, value: String) = _stats.set(key, value)
      def timing(key: String, value: Long) = _stats.timing(key, value)
      def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
        _stats.distribution(key, value, tags)
      def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
        _stats.event(level, title, text, tags)
    }

    def toJson(out: JSONWriter): Unit = buffer.toJson(out)
  }

  final class RewriterStats(prefix: String, _stats: StatsRecorder)
      extends SSTableIteratorStats(prefix)
      with Transformer.Stats
      with OutputStats {

    private[this] val buffer = new StatsRequestBuffer(
      Set(outputRowsKey, outputCellsKey, discardedKey, rowsRewrittenKey, cellsRewrittenKey))

    val stats = StatsRecorder.Multi(Seq(buffer, _stats))

    private[this] lazy val discardedKey = s"$prefix.Rows.Discarded"
    def incrDiscardedRows() = stats.incr(discardedKey)

    private[this] lazy val rowsRewrittenKey = s"$prefix.Rows.Rewritten"
    def incrRowsRewritten() = stats.incr(rowsRewrittenKey)

    private[this] lazy val cellsRewrittenKey = s"$prefix.Cells.Rewritten"
    def incrCellsRewritten() = stats.incr(cellsRewrittenKey)

    private[this] lazy val databasesRewrittenKey = s"$prefix.Databases.Rewritten"
    def incrDatabasesRewritten() = stats.incr(databasesRewrittenKey)

    private[this] lazy val accessProvidersRewrittenKey =
      s"$prefix.AccessProviders.Rewritten"
    def incrAccessProvidersRewritten() =
      stats.incr(accessProvidersRewrittenKey)

    private[this] lazy val lookupsRewrittenKey = s"$prefix.Lookups.Rewritten"
    def incrLookupsRewritten() = stats.incr(lookupsRewrittenKey)

    def toJson(out: JSONWriter): Unit = buffer.toJson(out)

    def rewrittenRows = buffer.countOrZero(rowsRewrittenKey)
  }
}
