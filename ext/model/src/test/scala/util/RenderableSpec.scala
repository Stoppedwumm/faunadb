package fauna.model.test

import fauna.ast.{ BytesL, DocEventL, UnresolvedRefL }
import fauna.atoms.{ APIVersion, CollectionID, IndexID, ScopeID }
import fauna.auth.{ Auth, RootAuth }
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.lang.clocks.Clock
import fauna.model.RefParser.RefScope.AccessProviderRef
import fauna.model.RenderContext
import fauna.storage.ir.MapV
import fauna.storage.{ Create, DocEvent, Resolved }
import fauna.repo.Store
import fauna.repo.test.CassandraHelper
import fauna.storage.doc.Data
import io.netty.buffer.Unpooled

class RenderableSpec extends Spec {

  val ctx = CassandraHelper.context("model")

  "render DocEventL" in {
    val scope = ScopeID(192596812167643660L)
    val auth = Auth.forScope(scope)

    val indexID = IndexID(32872)
    val collID = CollectionID(1043)

    ctx ! Store.insertUnmigrated(scope, indexID.toDocID, Data(MapV("name" -> "index")))
    ctx ! Store.insertUnmigrated(scope, collID.toDocID, Data(MapV("name" -> "coll")))

    val data = MapV(
      "active" -> true,
      "serialized" -> true,
      "name" -> "all_AccountStatsUpdates1",
      "source" -> MapV("class" -> collID.toDocID),
      "data" -> MapV("foo" -> "bar"),
      "partitions" -> 8
    )

    val ts = Resolved(Timestamp.parse("2020-02-18T17:12:41.840Z"))
    val doc = DocEventL(DocEvent(ts, scope, indexID.toDocID, Create, data))

    val render = RenderContext.render(auth, APIVersion.V21, Clock.time, doc)

    (ctx ! render).toUTF8String shouldBe """{"ts":1582045961840000,"action":"create","instance":{"@ref":{"id":"index","class":{"@ref":{"id":"indexes"}}}},"data":{"foo":"bar"}}"""
  }

  "render BytesL" - {
    def render(byte: Int, apiVersion: APIVersion): String = {
      val buf = Unpooled.wrappedBuffer(Array[Byte](byte.toByte))
      (ctx ! RenderContext.render(RootAuth, apiVersion, Clock.time, BytesL(buf))).toUTF8String
    }

    "use standard encoding" forAll { apiVersion =>
      render(0xf8, apiVersion) shouldBe """{"@bytes":"+A=="}"""
      render(0xf9, apiVersion) shouldBe """{"@bytes":"+Q=="}"""
      render(0xfa, apiVersion) shouldBe """{"@bytes":"+g=="}"""
      render(0xfb, apiVersion) shouldBe """{"@bytes":"+w=="}"""
      render(0xfc, apiVersion) shouldBe """{"@bytes":"/A=="}"""
      render(0xfd, apiVersion) shouldBe """{"@bytes":"/Q=="}"""
      render(0xfe, apiVersion) shouldBe """{"@bytes":"/g=="}"""
      render(0xff, apiVersion) shouldBe """{"@bytes":"/w=="}"""
    }
  }

  "render unresolved AccessProvider ref" in {
    val str = (ctx ! RenderContext.render(RootAuth, APIVersion.V3, Clock.time, UnresolvedRefL(AccessProviderRef("name", None)))).toUTF8String

    str shouldBe """{"@ref":{"id":"name","collection":{"@ref":{"id":"access_providers"}}}}"""
  }
}
