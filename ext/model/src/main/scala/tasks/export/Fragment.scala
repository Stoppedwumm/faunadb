package fauna.model.tasks.export

import fauna.atoms._
import fauna.lang.syntax._
import fauna.storage.doc.FieldType
import fauna.storage.ir.LongV
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.io.{ BufferedOutputStream, OutputStream }
import java.nio.file.{ Files, Path, StandardOpenOption }

object Fragment {
  val FileRegex = "(\\d{9})\\.fragment".r
  val NullIdx = Idx(-1)

  implicit val FieldT = FieldType[Idx]("Fragment.Idx") { f =>
    LongV(f.toInt)
  } { case LongV(l) =>
    Idx(l.toInt)
  }

  final case class Idx(toInt: Int) extends AnyVal with Ordered[Idx] {
    def compare(other: Idx) = toInt.compareTo(other.toInt)
    override def toString() = if (this == NullIdx) "tmp" else f"$toInt%09d"
    def incr = Idx(toInt + 1)
  }
  object Idx {
    def fromString(str: String): Idx =
      if (str == "tmp") NullIdx else Idx(str.toInt)
  }

  def unapply(path: Path): Option[Fragment] =
    path.getFileName.toString match {
      case FileRegex(idx) =>
        val coll = path.getParent.getFileName.toString
        Some(Fragment(coll, Idx(idx.toInt), path))
      case _ => None
    }

  def apply(path: Path): Fragment =
    unapply(path).getOrElse {
      throw new IllegalArgumentException("Invalid fragment path")
    }

  def apply(info: ExportInfo, coll: String, idx: Idx): Fragment =
    Fragment(coll, idx, path(info, coll, idx))

  def path(info: ExportInfo, coll: String, idx: Idx): Path = {
    require(idx.toInt > Fragment.NullIdx.toInt, "Invalid fragment index")
    val base = info.getCollectionsPath(isTemp = true)
    base / coll / s"$idx.fragment"
  }

  def allForCollection(info: ExportInfo, coll: String): List[Fragment] = {
    val b = List.newBuilder[Fragment]
    (info.getCollectionsPath(isTemp = true) / coll)
      .foreachFile(unapply(_).foreach(b += _), ignoreErrors = true)
    b.result().sortBy(_.idx)
  }
}

final case class Fragment(coll: String, idx: Fragment.Idx, path: Path) {
  lazy val onDiskSize = Files.size(path)
}

object FragmentWriter {
  def apply(info: ExportInfo, coll: String, idx: Fragment.Idx): FragmentWriter =
    FragmentWriter(Fragment(info, coll, idx))

  def apply(frag: Fragment): FragmentWriter = {
    frag.path.getParent.mkdirs()
    val s = Files.newOutputStream(
      frag.path,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING)
    new FragmentWriter(frag, new BufferedOutputStream(s))
  }

}

final class FragmentWriter(val frag: Fragment, val out: OutputStream) {
  def writeEntry(docID: DocID, buf: ByteBuf): Unit = {
    val id = Unpooled.copyLong(docID.subID.toLong)
    val len = Unpooled.copyInt(buf.readableBytes)
    id.readBytes(out, id.readableBytes)
    len.readBytes(out, len.readableBytes)
    buf.readBytes(out, buf.readableBytes)
  }

  def close() = out.close()
}

object FragmentReader {
  def apply(frag: Fragment): FragmentReader = {
    val arr = Files.readAllBytes(frag.path)
    val buf = Unpooled.wrappedBuffer(arr)
    new FragmentReader(frag, buf)
  }
}

final class FragmentReader(val frag: Fragment, val in: ByteBuf)
    extends Iterator[(SubID, ByteBuf)] {

  def hasNext = in.isReadable()

  def next(): (SubID, ByteBuf) =
    if (!hasNext) {
      Iterator.empty.next()
    } else {
      val sub = SubID(in.readLong())
      val len = in.readInt()
      val buf = in.readSlice(len)
      (sub, buf)
    }

  def close() = in.release()
}
