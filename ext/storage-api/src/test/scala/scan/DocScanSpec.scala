package fauna.storage.api.scan.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.prop._
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.api.scan.DocScan
import fauna.storage.api.test._
import fauna.storage.cassandra.CassandraKeyLocator
import fauna.storage.doc._
import scala.concurrent.duration._

class DocScanSpec extends Spec("docscan") {

  private def keyToLocation(key: (ScopeID, DocID)): Location = {
    val (scopeID, id) = key
    CassandraKeyLocator.locate(Tables.Versions.rowKeyByteBuf(scopeID, id))
  }

  // Don't use storageProp because it runs the test multiple times but doesn't
  // clean up what's in storage.
  once(s"works") {
    for {
      (scopeID0, docID00) <- idsP
      (_, docID01)        <- idsP
      (scopeID1, docID10) <- idsP
      (_, docID11)        <- idsP
      (scopeID2, docID20) <- idsP
      (_, docID21)        <- idsP
      (scopeID3, docID30) <- idsP
      unborn              <- Prop.int(1024)
      tombstones          <- Prop.int(1024)
      oldTS               <- Prop.timestamp()
      latestTS            <- Prop.timestampAfter(oldTS)
    } yield {
      withStorageEngine(StatsRecorder.Null) { engine =>
        // 7 documents, 7 latest versions, with 2 old versions.
        // Latest includes both live and deleted versions.
        val write0 = writeFactory(engine, scopeID0, _)
        write0(docID00)(latestTS, latestTS, Create, Data.empty, None)
        write0(docID01)(oldTS, oldTS, Create, Data.empty, None)
        write0(docID01)(latestTS, latestTS, Create, Data.empty, None)
        // Insert an unborn cell. This should not halt iteration.
        write0(docID01)(
          latestTS + 1.milli,
          latestTS + 1.milli,
          Create,
          Data.empty,
          None)

        val write1 = writeFactory(engine, scopeID1, _)
        write1(docID10)(latestTS, latestTS, Create, Data.empty, None)
        write1(docID11)(oldTS, oldTS, Create, Data.empty, None)
        write1(docID11)(latestTS, latestTS, Delete, Data.empty, None)

        val write2 = writeFactory(engine, scopeID2, _)
        write2(docID20)(latestTS, latestTS, Create, Data.empty, None)
        write2(docID21)(latestTS, latestTS, Create, Data.empty, None)

        val write3 = writeFactory(engine, scopeID3, _)
        write3(docID30)(Timestamp.ofMicros(1), latestTS, Create, Data.empty, None)

        // Spread some landmines around the keyspace.
        for (_ <- 0 until unborn) {
          val (scope, id) = idsP.sample
          val ts = Prop.timestampAfter(latestTS).sample
          writeFactory(engine, scope, id)(ts, ts, Create, Data.empty, None)
        }

        for (_ <- 0 until tombstones) {
          val (scope, id) = idsP.sample
          val ts = Prop.timestamp(latestTS).sample
          val action = Prop.choose(Create, Delete).sample
          tombstoneFactory(engine, scope, id)(ts, ts, action)
        }

        val selector = Selector.from(Set(scopeID0, scopeID1, scopeID2, scopeID3))
        // Scan everything using varying page sizes.
        // 7 docs and 9 versions, so 7 scan versions total.
        val total = 7
        Seq(1, 2, 3, 4, 5, 6, 7, 8) map { countPer =>
          var count = 0
          var slice = Option(ScanSlice(Tables.Versions.CFName, ScanBounds.All))
          while (!slice.isEmpty) {
            val (res, next) =
              runDocScan(engine, DocScan(latestTS, slice.get, selector, countPer))
            res foreach { v =>
              // Should get the latest version-- no oldTS.
              val one = Timestamp.ofMicros(1)
              v.validTS match {
                case Some(`latestTS`) | Some(`one`) => ()
                case ts => fail(s"unexpected validTS $ts")
              }
            }

            res.size shouldBe (>=(countPer min (total - count)))
            count += res.size
            slice = next
          }
          count shouldBe total
        }

        // Scan subsections of the ring spanned by the documents.
        // There should be one version per location.
        val ids = Seq(
          (scopeID0, docID00),
          (scopeID0, docID01),
          (scopeID1, docID10),
          (scopeID1, docID11),
          (scopeID2, docID20),
          (scopeID2, docID21),
          (scopeID3, docID30))
        val locs = ids.map(keyToLocation).sorted
        (1 until locs.size) foreach { right =>
          (0 until right) foreach { left =>
            val bounds = ScanBounds(locs(left), locs(right))
            val slice = ScanSlice(Tables.Versions.CFName, bounds)
            val (res, next) =
              runDocScan(engine, DocScan(latestTS, slice, Selector.All, 64))
            next.isEmpty shouldBe true
            res.size shouldBe (right - left)
          }
        }
      }
    }
  }
}
