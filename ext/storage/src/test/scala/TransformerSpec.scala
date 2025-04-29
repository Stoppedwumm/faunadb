package fauna.storage.test

import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.stats.StatsRecorder
import fauna.storage.{ Cell => _, Row => _, _ }
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import io.netty.util.CharsetUtil
import java.nio.ByteBuffer
import org.apache.cassandra.db.{
  ArrayBackedSortedColumns,
  Cell,
  DecoratedKey,
  Mutation => CMutation,
  Row
}
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.utils.{ ByteBufferUtil, OutputHandler }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

class TransformerSpec extends Spec {

  val transformCF =
    ColumnFamilySchema[(Array[Byte], Array[Byte])]("TransformSpec", Seq(BytesType))

  override def beforeEach() = {
    super.beforeEach()
    val storage = Storage(keyspaceName, Seq(transformCF))
    storage.init()
  }

  test("works") {
    withStorageEngine(StatsRecorder.Null) { storage =>
      val keyspace = storage.keyspace
      val store = keyspace.getColumnFamilyStore(transformCF.name)

      store.clearUnsafe()
      store.disableAutoCompaction()

      val now = Clock.time

      val tom =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("tom"))
      val dick =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("dick"))
      val harry =
        StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes("harry"))

      Seq(tom, dick, harry).zipWithIndex foreach {
        case (key, idx) =>
          val mut = new CMutation(keyspace.getName(), key.getKey)

          val cell = cellname(Seq(ByteBufferUtil.bytes("jones")))

          mut.add(
            transformCF.name,
            cell,
            ByteBufferUtil.EMPTY_BYTE_BUFFER,
            (now + idx.minutes).micros)

          mut.apply()
      }

      store.forceBlockingFlush()

      val counter = new IteratorStatsCounter()
      val iter =
        new CassandraIterator(
          counter,
          store,
          ScanBounds.All,
          Selector.All,
          now + 60.minutes)

      val before = iter.toSeq map {
        case (key, cell) => (key.toUTF8String, cell.name.toUTF8String, cell.ts)
      }

      before should contain only (Seq(
        ("tom", "jones", now + 0.minutes),
        ("dick", "jones", now + 1.minutes),
        ("harry", "jones", now + 2.minutes)): _*)

      val lister = store.directories.sstableLister().skipTemporary(true)

      val b = Seq.newBuilder[SSTableReader]

      lister.list.entrySet forEach { entry =>
        val comps = entry.getValue

        if (comps.contains(Component.DATA)) {
          val sstable =
            SSTableReader.openNoValidation(entry.getKey, comps, store.metadata)
          b += sstable
        }
      }

      val output =
        new OutputHandler.SystemOutput(false /* debug */, false /* printStack */ )

      val rows = Seq.newBuilder[Row]

      val codec = new Transformer.Codec[Array[Byte]] {
        def decode(bytes: ByteBuffer) = bytes.array
        def encode(key: Array[Byte]) = ByteBuffer.wrap(key)
      }

      val stats = new Transformer.Stats {
        var rows = 0
        var discardedRows = 0
        var rewrittenRows = 0

        var cells = 0
        var rewrittenCells = 0
        var unbornCells = 0

        def incrRows() = rows += 1
        def recordRowBytes(bytes: Long) = ()
        def incrDiscardedRows() = discardedRows += 1
        def incrRowsRewritten() = rewrittenRows += 1

        def incrCells(count: Int = 1) = cells += count
        def incrCellsRewritten() = rewrittenCells += 1
        def incrCellsUnborn(count: Int = 1) = unbornCells += count

        def recordRuntime(elapsedMillis: Long): Unit = ()
        def recordSSTableBytes(bytes: Long): Unit = ()
      }

      b.result() foreach { reader =>
        val iter =
          new SSTableIterator(reader, output, stats) {
            def apply(key: DecoratedKey, dataSize: Long): Unit = {
              val transformer = new Transformer[Array[Byte]](
                key,
                dataSize,
                reader,
                dataFile,
                codec,
                now + 1.minute,
                output,
                stats) {

                def transformKey(key: Array[Byte]) =
                  new String(key, CharsetUtil.UTF_8) match {
                    case "tom"   => "toni".toUTF8Bytes
                    case "dick"  => "debbie".toUTF8Bytes
                    case "harry" => "harriet".toUTF8Bytes
                  }

                override def transformCell(key: Array[Byte], cell: Cell): Cell = {
                  val name = cellname(Seq(ByteBuffer.wrap("smith".toUTF8Bytes)))
                  cell.withUpdatedName(name)
                }

              }

              val iter = transformer.getRow()

              try {
                val factory = ArrayBackedSortedColumns.factory
                val cf =
                  iter.getColumnFamily.cloneMeShallow(factory, false /* reversed */ )

                while (iter.hasNext()) {
                  val atom = iter.next
                  cf.addAtom(atom)
                }

                rows += new Row(iter.getKey, cf)
              } finally {
                iter.close()
              }
            }
          }

        try {
          iter.run()
        } finally {
          iter.close()
          reader.selfRef().release()
        }
      }

      val after = rows.result() map { row =>
        val key = row.key.getKey
        val cells = row.cf.getSortedColumns().asScala map { cell =>
          cell.name.toByteBuffer().toUTF8String
        }

        (key.toUTF8String, cells.headOption)
      }

      // NB. Harriet's only cell is newer than the snapshot time.
      after should contain only (Seq(
        ("toni", Some("smith")),
        ("debbie", Some("smith")),
        ("harriet", None)): _*)

      // All keys are present.
      stats.rows should equal(3)
      stats.discardedRows should equal(0)
      stats.rewrittenRows should equal(3)

      // Harriet's cell is unborn.
      stats.cells should equal(2)
      stats.unbornCells should equal(1)
      stats.rewrittenCells should equal(2)
    }
  }
}
