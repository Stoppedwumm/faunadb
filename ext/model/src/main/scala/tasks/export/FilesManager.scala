package fauna.model.tasks.export

import fauna.atoms._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.model.schema.CollectionConfig
import fauna.model.tasks.export._
import io.netty.buffer.ByteBuf
import java.nio.file.{ Files, Path }
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ blocking, Future }
import scala.util.control.NonFatal

object FilesManager {

  // For testing. Also flushes files. Does _not_ close them as that slightly changes
  // behavior for some streams (GZIP, for one.)
  def resetAll(): Unit = {
    states.values.forEach(_.fragmentWriters.values.foreach(_.out.flush()))
    states.clear()
  }

  // TODO: make this a caffeine map so we can better track & expire long-lived
  // entries
  private val states = new ConcurrentHashMap[TaskID, FilesManager]()

  def init(
    id: TaskID,
    info: ExportInfo,
    collections: Map[CollectionID, CollectionConfig],
    partition: Int,
    prevOut: Seq[Path],
    prevIdx: Fragment.Idx): FilesManager =
    Option(states.get(id)) match {
      case Some(_) => sys.error(s"Already existing state for task $id!")
      case None =>
        val mgr = new FilesManager(info, collections, partition, prevOut)
        states.put(id, mgr)
        mgr.setFragmentIdx(prevIdx)
        mgr
    }

  def forIdx(id: TaskID, idx: Fragment.Idx): FilesManager = {
    val mgr = Option(states.get(id))
      .getOrElse(sys.error(s"Did not find state for task $id!"))
    mgr.setFragmentIdx(idx)
    mgr
  }

  def drop(id: TaskID): Unit =
    Option(states.remove(id)).foreach(_.close())

  def contains(id: TaskID) = states.containsKey(id)
}

final class FilesManager(
  val info: ExportInfo,
  val collections: Map[CollectionID, CollectionConfig],
  val partition: Int,
  val prevOut: Seq[Path]) {

  private[this] var fragmentIdx = Fragment.NullIdx
  private[FilesManager] def setFragmentIdx(idx: Fragment.Idx): Unit = {
    fragmentIdx = idx
  }
  private[this] var processFut: Option[Future[Vector[Outfile]]] = None

  private[FilesManager] val fragmentWriters =
    MMap.empty[CollectionID, FragmentWriter]

  private[FilesManager] val lastOrds =
    Outfile
      .maxByCollection(prevOut.view.map(Outfile(_)))
      .view
      .map { case (n, of) =>
        val id = collections.values.find(_.name == n).get.id
        id -> of.ord
      }
      .to(MMap)

  private lazy val outPath = info.getCollectionsPath(isTemp = false)

  private def emittedPath(out: Outfile) = {
    val emitted = f"${out.coll}%s_$partition%02d_${out.ord}%s.${info.datafileExt}%s"
    outPath / out.coll / emitted
  }

  // version entries interface

  def formatter = new VersionFormatter(collections, info.docFormat)

  def shouldExport(id: DocID): Boolean =
    collections.contains(id.collID)

  def writeEntry(docID: DocID, buf: ByteBuf): Unit = {
    val writer = fragmentWriters
      .getOrElseUpdate(
        docID.collID,
        FragmentWriter(info, collections(docID.collID).name, fragmentIdx))

    assert(writer.frag.idx == fragmentIdx, "mismatched writer fragment idx")

    writer.writeEntry(docID, buf)
  }

  // outfile management

  /** Steps in processOutifiles are "inverted" in order to be able to checkpoint
    * progress between compaction and emission. Returned compacted files are
    * emitted on the next call.
    */
  def processOutfiles(prevOut: Vector[Path], incremental: Boolean = true): Unit =
    if (fragmentIdx == Fragment.NullIdx) {
      require(prevOut == Vector.empty)
      processFut = Some(Future(Vector.empty))

    } else {
      val fragIdx = fragmentIdx

      // we must cleanup and get fragments before yielding so that we do not race
      // with the task's fragment generation.
      val fragments = collections.keys.map { id =>
        cleanupCollFiles(id, fragIdx)
        id -> getFragments(id)
      }

      processFut = Some(Future {
        // FIXME: blocking currently does nothing with our global pool
        blocking {
          emitOutfiles(prevOut)
          compactOutfiles(fragments.toMap, incremental)
        }
      })
    }

  def processOutfilesResult: Future[Vector[Outfile]] = processFut.get

  def close(): Unit = {
    fragmentWriters.values.foreach(_.close())
    fragmentWriters.clear()
  }

  // helpers

  private def compactOutfiles(
    fragmentsByColl: Map[CollectionID, List[Fragment]],
    incremental: Boolean): Vector[Outfile] = {
    val outfiles = Vector.newBuilder[Outfile]

    collections.values.foreach { coll =>
      var fragments = fragmentsByColl(coll.id)
      var ord = lastOrds.getOrElse(coll.id, Outfile.NullOrd)

      def cont =
        (!incremental && fragments.nonEmpty) ||
          (fragments.view.map(_.onDiskSize).sum > Outfile.FragmentBytesMin)

      while (cont) {
        fragments = generateOutfile(coll.name, ord.incr, fragments)
        ord = ord.incr
      }

      lastOrds(coll.id) = ord
      outfiles ++= getOutfileInfo(coll.id).outfiles.view
    }

    outfiles.result()
  }

  private def generateOutfile(
    coll: String,
    ord: Outfile.Ord,
    fragments: List[Fragment]): List[Fragment] = {
    val iter = fragments.iterator

    if (iter.nonEmpty) {
      val out = OutfileWriter(info, coll, ord)
      val consumed = List.newBuilder[Fragment]
      var lastIdx = Fragment.NullIdx

      try {
        while (iter.hasNext && !out.atSoftLimit) {
          val frag = iter.next()
          out.writeFragment(frag)
          consumed += frag
          lastIdx = frag.idx
        }

        out.commit(lastIdx)
      } catch {
        case NonFatal(ex) =>
          out.fail()
          throw ex
      }

      consumed.result().foreach(f => Files.delete(f.path))
    }

    iter.toList
  }

  private def cleanupCollFiles(collID: CollectionID, prevIdx: Fragment.Idx) = {
    val oinfo = getOutfileInfo(collID)

    oinfo.outfiles.foreach { of =>
      if (of.isTemp) of.path.delete()
    }

    getFragments(collID).foreach { frag =>
      // delete fragment if already written to outfile, or is gt current index
      if (frag.idx <= oinfo.maxIdx || frag.idx > prevIdx) {
        frag.path.delete()
      }
    }
  }

  private def emitOutfiles(paths: Seq[Path]): Unit =
    paths.foreach { p =>
      if (Files.exists(p)) {
        p.moveAtomically(emittedPath(Outfile(p)), mkdirs = true)
      }
    }

  private def getOutfileInfo(collID: CollectionID): Outfile.Info =
    Outfile.getInfo(info, collections(collID).name)

  private def getFragments(collID: CollectionID): List[Fragment] =
    Fragment.allForCollection(info, collections(collID).name)
}
