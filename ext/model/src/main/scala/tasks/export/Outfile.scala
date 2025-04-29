package fauna.model.tasks.export

import fauna.atoms._
import fauna.lang.syntax._
import java.io.OutputStream
import java.nio.file.{ Files, Path, StandardOpenOption }
import java.util.zip.GZIPOutputStream
import scala.collection.mutable.{ HashSet => MSet }

object Outfile {
  val SoftLimitBytes = 128 * 1024 * 1024
  val FragmentOverheadMult = 2.5
  val FragmentBytesMin = SoftLimitBytes * FragmentOverheadMult
  val FileRegex = "(\\d{6})_(tmp|\\d{9}).out".r
  val NullOrd = Ord(-1)

  final case class Ord(toInt: Int) extends AnyVal with Ordered[Ord] {
    def compare(other: Ord) = toInt.compareTo(other.toInt)
    override def toString() = if (this == NullOrd) "tmp" else f"$toInt%06d"
    def incr = Ord(toInt + 1)
  }

  final case class Info(maxOrd: Ord, maxIdx: Fragment.Idx, outfiles: List[Outfile])

  def unapply(path: Path): Option[Outfile] =
    path.getFileName.toString match {
      case FileRegex(ord, idx) =>
        val coll = path.getParent.getFileName.toString
        Some(Outfile(coll, Ord(ord.toInt), Fragment.Idx.fromString(idx), path))
      case _ => None
    }

  def apply(path: Path): Outfile =
    unapply(path).getOrElse {
      throw new IllegalArgumentException("Invalid outfile path")
    }

  def apply(
    info: ExportInfo,
    coll: String,
    ord: Ord,
    maxIdx: Fragment.Idx): Outfile =
    Outfile(coll, ord, maxIdx, path(info, coll, ord, maxIdx))

  def path(info: ExportInfo, coll: String, ord: Ord, maxIdx: Fragment.Idx): Path = {
    require(ord.toInt > Outfile.NullOrd.toInt, "Invalid outfile ordinal")
    val base = info.getCollectionsPath(isTemp = true)
    base / coll / s"${ord}_$maxIdx.out"
  }

  def getInfo(info: ExportInfo, coll: String) = {
    val outs = allForCollection(info, coll)
    val maxOrd = outs.map(_.ord).maxOption.getOrElse(Outfile.NullOrd)
    val maxIdx = outs.map(_.maxIdx).maxOption.getOrElse(Fragment.NullIdx)

    if (outs.nonEmpty) {
      val last = outs.last
      assert(
        last.ord == maxOrd && last.maxIdx == maxIdx,
        s"failed outfile sanity check ${last.ord} == $maxOrd && ${last.maxIdx} == $maxIdx")
    }

    Outfile.Info(maxOrd, maxIdx, outs)
  }

  def maxByCollection(of: Iterable[Outfile]): Map[String, Outfile] =
    of.groupBy(_.coll).map { case (c, ofs) => c -> ofs.maxBy(_.ord) }

  private def allForCollection(info: ExportInfo, coll: String): List[Outfile] = {
    val b = List.newBuilder[Outfile]
    (info.getCollectionsPath(isTemp = true) / coll)
      .foreachFile(unapply(_).foreach(b += _), ignoreErrors = true)
    b.result().sortBy(_.ord)
  }
}

final case class Outfile(
  coll: String,
  ord: Outfile.Ord,
  maxIdx: Fragment.Idx,
  path: Path) {
  def isTemp = maxIdx == Fragment.NullIdx
  def committedPath(idx: Fragment.Idx) = path.getParent / s"${ord}_$idx.out"
}

object OutfileWriter {
  def apply(info: ExportInfo, coll: String, ord: Outfile.Ord): OutfileWriter =
    OutfileWriter(info, Outfile(info, coll, ord, Fragment.NullIdx))

  def apply(info: ExportInfo, outfile: Outfile) = {
    require(outfile.maxIdx == Fragment.NullIdx, "fragment maxIdx must be null")
    outfile.path.getParent.mkdirs()
    var s: OutputStream = Files.newOutputStream(
      outfile.path,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING)
    if (info.datafileCompression) s = new GZIPOutputStream(s)
    new OutfileWriter(outfile, s, info.datafileFormat)
  }
}

final class OutfileWriter(
  val outfile: Outfile,
  val out: OutputStream,
  val format: DatafileFormat) {
  private[this] var _size = 0L
  private[this] val seen = MSet.empty[SubID]

  def isEmpty: Boolean = _size == 0
  def uncompressedSize: Long = _size
  def atSoftLimit = uncompressedSize >= Outfile.SoftLimitBytes

  def writeFragment(frag: Fragment): Unit = {
    def writeDelim() =
      format match {
        case DatafileFormat.JSON =>
          if (isEmpty) writeByte('[') else writeByte(',')
        case DatafileFormat.JSONL =>
          if (!isEmpty) writeByte('\n')
      }

    val reader = FragmentReader(frag)

    try {
      reader.foreach { case (sub, buf) =>
        if (!seen.add(sub)) {
          sys.error(s"$sub emitted twice!")
        }

        writeDelim()
        _size += buf.readableBytes
        buf.readBytes(out, buf.readableBytes)
      }
    } finally {
      reader.close()
    }
  }

  private def writeByte(byte: Byte) = {
    out.write(byte)
    _size += 1
  }

  def fail(): Unit = out.close()

  def commit(lastIdx: Fragment.Idx): Long = {
    format match {
      case DatafileFormat.JSON =>
        if (isEmpty) writeByte('[')
        writeByte(']')
      case DatafileFormat.JSONL =>
        writeByte('\n')
    }
    out.close()

    val sz = Files.size(outfile.path)
    outfile.path.moveAtomically(outfile.committedPath(lastIdx))
    sz
  }
}
