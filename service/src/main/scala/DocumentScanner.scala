package fauna.core

import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.schema._
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query._
import fauna.storage.api.set.Scalar
import fauna.storage.index._
import fauna.storage.ir._

/** This class is used to scan through the documents index for a given collection
  * and log any documents it finds in that index that dont' actually exist.
  * It also takes an optional function that will be invoked with each document
  * that does exist (along with the logger), in case there is some other document
  * processing that is desired.
  */
object DocumentScanner {
  def scanDocs(
    repo: RepoContext,
    scope: ScopeID,
    coll: CollectionID,
    processDoc: Option[(Logger, Version.Live) => Query[Unit]])(
    implicit ctl: ConsoleControl) = {
    val logger = getLogger
    // used to make reads as we go over docs stable
    val queryTS = repo ! Query.snapshotTime
    def info(msg: String) = logger.info(s"[DocumentScanner] $msg")
    def scan0(page0: Page[Query, Iterable[IndexValue]]): Unit = {
      var page = page0
      var isDone = false
      var i = 0
      while (!isDone) {
        repo ! page.value.map { v =>
          if (i % 1000 == 0) {
            info(s"progress: $v")
          }
          i += 1
          Store.getUnmigrated(scope, v.docID, queryTS).map {
            case Some(doc) => processDoc.foreach(pd => repo ! pd(logger, doc))
            case None      => info(s"found index entry for missing doc $v")
          }
        }.sequence
        page.next match {
          case Some(f) => page = repo ! f()
          case None    => isDone = true
        }
      }
      info(s"document scan for $scope $coll complete.")
    }
    val page = repo ! Store
      .collection(
        NativeIndex.DocumentsByCollection(scope),
        Vector(Scalar(DocIDV(coll.toDocID))),
        queryTS,
        pageSize = 16
      )
    scan0(page)
  }
}
