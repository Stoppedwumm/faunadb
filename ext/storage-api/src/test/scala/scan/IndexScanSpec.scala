package fauna.storage.api.scan.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.prop._
import fauna.scheduler.ConstantPriorityProvider
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.api.{ Storage => NewStorage }
import fauna.storage.api.scan.{ ElementScan, KeyScan }
import fauna.storage.api.test._
import fauna.storage.index.IndexTerm
import fauna.storage.ops.SetAdd
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._

class IndexScanSpec extends Spec("indexscan") with Eventually {

  private def mkwrite(
    scopeID: ScopeID,
    indexID: IndexID,
    id: DocID,
    term: String,
    values: Vector[String],
    ts: Timestamp) =
    SetAdd(
      scopeID,
      indexID,
      doc = id,
      terms = Vector(term),
      values = values map { IndexTerm(_) },
      writeTS = Resolved(ts),
      action = Add
    )

  private def elementScanAll(
    engine: StorageEngine,
    cf: String,
    ts: Timestamp): Vector[ElementScan.Entry] = {
    var cursor =
      Option(ElementScan.Cursor(ScanSlice(cf, ScanBounds.All), false))
    var entries = Vector.empty[ElementScan.Entry]
    while (!cursor.isEmpty) {
      val (res, next) =
        runElementScan(engine, ElementScan(ts, cursor.get, 4))
      entries = entries ++ res
      cursor = next
    }
    entries
  }

  // Don't use storageProp because it runs the test multiple times but doesn't
  // clean up what's in storage.
  once(s"works") {
    for {
      (scopeID0, docID00) <- idsP
      (_, docID01)        <- idsP
      (_, docID02)        <- idsP
      (_, docID03)        <- idsP
      (scopeID1, docID10) <- idsP
      oldTS               <- Prop.timestamp()
      latestTS            <- Prop.timestampAfter(oldTS)
    } yield {
      withStorageEngine(StatsRecorder.Null) { engine =>
        val fakeIndexID0 = IndexID(691)
        val fakeIndexID1 = IndexID(1009)

        // 2 scopes, 2 terms, 4 elements.
        // Vary the number of values to make sure the predicate works.
        val writes = Vector(
          // Two elements with the same key, ordered differently by sorted
          // index and historical index.
          mkwrite(scopeID0, fakeIndexID0, docID00, "a", Vector("x", "b"), latestTS),
          mkwrite(scopeID0, fakeIndexID0, docID01, "a", Vector("y"), oldTS),
          // An element with the same term as the first two, but in a different
          // scope.
          mkwrite(scopeID1, fakeIndexID0, docID10, "a", Vector(), latestTS),
          // An unborn cell. This should not stop iteration.
          mkwrite(
            scopeID1,
            fakeIndexID0,
            docID10,
            "a",
            Vector(),
            latestTS + 1.milli),

          // An element with the same scope as the first two, but a different term.
          mkwrite(scopeID0, fakeIndexID0, docID02, "b", Vector("x"), oldTS),
          // An element with the same scope and terms as the first two, but in a
          // different index.
          mkwrite(scopeID0, fakeIndexID1, docID03, "a", Vector("x", "y", "z"), oldTS)
        )

        applyWrites(engine, latestTS, writes: _*)

        Seq(Tables.HistoricalIndex.CFName, Tables.SortedIndex.CFName) foreach { cf =>
          // Key scan everything using varying page sizes.
          Seq(1, 2, 3, 4, 5) foreach { countPer =>
            var keyfixes = Set.empty[(ScopeID, IndexID)]
            withClue(s"key scan cf: $cf countPer: $countPer:") {
              // 4 different keys, so 4 results total.
              val total = 4
              var count = 0
              var slice = Option(ScanSlice(cf, ScanBounds.All))
              while (!slice.isEmpty) {
                val (res, next) =
                  runKeyScan(engine, KeyScan(latestTS, slice.get, countPer))
                res.size shouldBe (countPer min (total - count))
                keyfixes = keyfixes ++ (res map { r => (r.scope, r.index) })
                count += res.size
                slice = next
              }
              keyfixes shouldBe Set(
                (scopeID0, fakeIndexID0),
                (scopeID0, fakeIndexID1),
                (scopeID1, fakeIndexID0))
              count shouldBe total
            }
          }

          // Element scan everything. The doc with the unborn cell may
          // need more than a single pass to tombstone.
          eventually {
            withClue(s"element scan cf: $cf:") {
              val entries = elementScanAll(engine, cf, latestTS)
              val expected = Set(docID00, docID01, docID02, docID03, docID10)
              Set((entries map { _.doc }): _*) shouldBe expected

              // Zap one entry.
              val ctx = new NewStorage.Context(
                ConstantPriorityProvider,
                engine,
                HostFlags
              )
              val storage = NewStorage(ctx)
              val zapped = entries.head
              storage.zapEntry(cf, zapped)

              // Scan again-- zapped entry is no longer visible.
              withClue(s"element scan zapped ${zapped.doc}") {
                val entries = elementScanAll(engine, cf, latestTS)
                Set((entries map { _.doc }): _*) shouldBe (expected - zapped.doc)
              }
            }
          }
        }
      }
    }
  }
}
