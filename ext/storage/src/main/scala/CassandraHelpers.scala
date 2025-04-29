package fauna.storage

import fauna.atoms._
import java.nio.file.{ Files, Path, Paths }
import org.apache.cassandra.dht._
import scala.jdk.CollectionConverters._

object CassandraHelpers {
  val ForbiddenEndLocation = Location(Location.MinValue.token + 1)

  def toRange(segment: Segment): Range[Token] =
    segment match {
      // this is a quirk of the DT: the entire ring is not
      // arbitrary (unlike in the TM), it _must_ be (min, min].
      case Segment(l @ Location.MinValue, r @ Location.MinValue) =>
        toRange(l.token, r.token)

      // We can't represent the [min, min+1) segment as a C* range due to a
      // side effect of the logic explained in the next case; it would end up
      // being represented as range (min, min], which as we stated in the
      // previous case represents the entire ring instead.
      case Segment(Location.MinValue, ForbiddenEndLocation) =>
        throw new IllegalArgumentException(s"Can't convert $segment to a range")

      // We specifically convert [min, r) to (min, r - 1] instead of the
      // nominally correct (max, r - 1] as we've seen some anomalous behavior
      // with wraparound ranges. This way, we avoid them as it would be the
      // only wraparound range from a segment. This looks like it would cause
      // us to fail to address any data that hashes to min, but fortunately we
      // don't have any such data. This is because C* Murmur3Partitioner
      // explicitly avoids hashing anything to the min token except zero-length
      // keys but in FaunaDB we never produce them.
      case Segment(l @ Location.MinValue, r) =>
        toRange(l.token, r.token - 1)

      // All other segments [l, r) are converted to (l - 1, r - 1], including
      // those with r == min. For r == min, r - 1 correctly results in max
      // because of integer overflow.
      case Segment(l, r) =>
        toRange(l.token - 1, r.token - 1)
    }

  private def toRange(l: Long, r: Long): Range[Token] =
    new Range(new LongToken(l), new LongToken(r))

  private def swapCFs(root: Path, ksName: String, fromCFName: String, toCFName: String) = {

    val ksRoot = root.resolve(ksName)

    def find(base: Path, name: String) = {
      val stream = Files.find(base, 1, (p, _) => {
        p.getFileName.toString.startsWith(s"$name-")
      })
      stream.iterator.asScala.nextOption() getOrElse {
        sys.error(s"Failed to find column family $name in $base")
      }
    }

    def renameFiles(aName: String, bName: String) = {
      val cf = find(ksRoot, aName)
      val aFilePrefix = s"$ksName-$aName-"
      val bFilePrefix = s"$ksName-$bName-"

      Files.find(cf, 1, (p, _) => {
        p.getFileName.toString.startsWith(aFilePrefix)
      }) forEach { p =>
        val toName = s"$bFilePrefix${p.getFileName.toString.substring(aFilePrefix.length)}"
        Files.move(p, cf.resolve(toName))
      }
    }

    renameFiles(fromCFName, toCFName)
    renameFiles(toCFName, fromCFName)

    val fromCF = find(ksRoot, fromCFName)
    val toCF = find(ksRoot, toCFName)
    val temp = ksRoot.resolve("SWAP.temp")

    Files.move(toCF, temp)
    Files.move(fromCF, toCF)
    Files.move(temp, fromCF)
  }

  def promoteIndex2CFs(rootName: String) = {
    val dataRoot = Paths.get(rootName).resolve("data")
    val ks = dataRoot.resolve(Cassandra.KeyspaceName)
    val marker = ks.resolve("index2_cfs_promoted.marker")

    if (!Files.exists(ks)) {
      sys.error(s"Index column family promotion failed: Keyspace root '$ks' not found.")
    }

    if (Files.exists(marker)) {
      sys.error(s"Index column family promotion failed: Promotion marker found at '$marker'.")
    }

    Files.createFile(marker)
    swapCFs(dataRoot, Cassandra.KeyspaceName, Tables.SortedIndex.CFName2, Tables.SortedIndex.CFName)
    swapCFs(dataRoot, Cassandra.KeyspaceName, Tables.HistoricalIndex.CFName2, Tables.HistoricalIndex.CFName)
  }
}
