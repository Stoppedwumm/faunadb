package fauna.repo.test

import fauna.atoms.{ CollectionID, DocID, ScopeID, SubID }
import fauna.lang.Timestamp
import fauna.prop.Prop
import fauna.repo.Store
import org.scalatest.{ OptionValues, TryValues }
import scala.concurrent.Await
import scala.concurrent.duration._

class KeyspaceSpec extends PropSpec with OptionValues with TryValues {

  val ctx = CassandraHelper.context("repo")

  def newScope = Prop.const(ScopeID(ctx.nextID()))

  val faunaClass = CollectionID(1024)

  def ID(id: Long) = DocID(SubID(id), faunaClass)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  once("locateDocumentSegment works") {
    for {
      scope <- newScope
    } {
      val docID = ID(1)
      val v = ctx ! Store.getUnmigrated(scope, docID, TS(1))
      // the document does not need to exist
      v.isEmpty should be(true)

      // validate that segment is local
      val segment = ctx.keyspace.segmentForDocument(scope, docID).success.value
      segment.length.intValue() should be(1)
      ctx.keyspace.segmentIsLocal(segment) should be(true)

      val locateF = ctx.keyspace.locateDocumentSegment(scope, docID)
      val (locationSegment, locationHosts) = Await.result(locateF, 10.seconds).value
      locationSegment should be(segment)
      locationHosts.size should be(1)
    }
  }

}
