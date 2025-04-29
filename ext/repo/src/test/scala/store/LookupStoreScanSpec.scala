package fauna.repo.test

import fauna.atoms.{ CollectionID, DocID, GlobalDatabaseID, ScopeID, SubID }
import fauna.lang.{ Page, Timestamp }
import fauna.repo.query.Query
import fauna.storage.{ Add, Resolved, ScanBounds, ScanSlice, Selector, Tables }
import fauna.storage.api.scan.LookupScan
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import fauna.storage.ops.LookupAdd
import java.time.Instant
import scala.concurrent.duration._

class LookupStoreScanSpec extends PropSpec {
  val ctx = CassandraHelper.context("repo")

  test("lookup scan paginates correctly when page boundary ends at unborn cell") {
    val lookups = (1 to 4095).map { i =>
      LookupAdd(
        GlobalDatabaseID(1),
        ScopeID(1),
        DocID(SubID(i), CollectionID(i)),
        Resolved(Timestamp(Instant.now())),
        Add
      )
    }
    ctx ! (lookups.foldLeft(Query.unit) { case (qAcc, lp) =>
      qAcc.flatMap(_ => Query.write(lp))
    })

    val nextPageLookups = (1 to 110).map { i =>
      LookupAdd(
        GlobalDatabaseID(20000),
        ScopeID(200),
        DocID(SubID(i), CollectionID(1)),
        Resolved(Timestamp(Instant.now())),
        Add
      )
    }
    ctx ! nextPageLookups.foldLeft(Query.unit) { case (qAcc, lp) =>
      qAcc.flatMap(_ => Query.write(lp))
    }

    val snapTime = Timestamp(Instant.now())
    Thread.sleep(3000)
    // we want this cell to get ordered after the valid ones so it hits the page
    // boundary
    // the unborn comparison is done based on the cell write time, not the time
    // provided here
    val unbornTime = Timestamp(Instant.now()) - 100.days
    val unbornLookup = LookupAdd(
      GlobalDatabaseID(1),
      ScopeID(1),
      DocID(SubID(1), CollectionID(1)),
      Resolved(unbornTime),
      Add
    )
    ctx ! Query.write(unbornLookup)

    val scan = LookupScan(
      snapTime,
      LookupScan.Cursor(
        ScanSlice(Tables.Lookups.CFName, ScanBounds.All),
        skipStart = false)
    )
    val query = Page.unfold(scan) { op =>
      Query.scan(op) map { res =>
        (
          res.values,
          res.next map { slice => LookupScan(Timestamp(Instant.now()), slice) })
      }
    }
    val entries = ctx ! query.flattenT

    val engine = ctx.service.storage
    val keyspace = engine.keyspace
    val store = keyspace.getColumnFamilyStore(Tables.Lookups.CFName)

    store.forceBlockingFlush()
    val counter = new IteratorStatsCounter()

    val citer =
      new CassandraIterator(counter, store, ScanBounds.All, Selector.All, snapTime)

    val cassandraLookups = citer.toSeq.map { case (rowKey, cell) =>
      Tables.Lookups.decodeLookup(rowKey, cell)
    }

    entries.size shouldEqual lookups.size + nextPageLookups.size
    entries shouldEqual cassandraLookups
  }
}
