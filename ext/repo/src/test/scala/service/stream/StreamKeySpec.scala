package fauna.repo.service.stream

import fauna.atoms._
import fauna.repo.test._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.storage.ops._

class StreamKeySpec extends Spec {
  import StreamKey._

  val scopeID = ScopeID(1234)
  val docID = DocID(SubID(1324), CollectionID(1234))
  val indexID = IndexID(1234)
  val terms = Vector(StringV("foo"))

  "StreamKey" - {

    "get rowKey from doc id" in {
      DocKey(scopeID, docID).rowKey shouldBe
        Tables.Versions.rowKeyByteBuf(scopeID, docID)
    }

    "get rowKey from set key" in {
      SetKey(scopeID, indexID, terms).rowKey shouldBe
        Tables.Indexes.rowKey(scopeID, indexID, terms)
    }

    "derives doc key from version add" in {
      val write = VersionAdd(
        scopeID,
        docID,
        Unresolved,
        Create,
        SchemaVersion.Min,
        Data.empty,
        None)
      StreamKey(write).value shouldBe DocKey(scopeID, docID)
    }

    "derives doc key from version remove" in {
      val write = VersionRemove(scopeID, docID, Unresolved, Create)
      StreamKey(write).value shouldBe DocKey(scopeID, docID)
    }

    "derives doc key from doc remove" in {
      val write = DocRemove(scopeID, docID)
      StreamKey(write).value shouldBe DocKey(scopeID, docID)
    }

    "derives set key from set add" in {
      val write =
        SetAdd(
          scope = scopeID,
          index = indexID,
          terms = terms,
          values = Vector.empty,
          doc = docID,
          writeTS = Unresolved,
          action = Add,
          ttl = None
        )

      StreamKey(write).value shouldBe SetKey(scopeID, indexID, terms)
    }

    "derives set key from set remove" in {
      val write =
        SetRemove(
          scope = scopeID,
          index = indexID,
          terms = terms,
          values = Vector.empty,
          doc = docID,
          writeTS = Unresolved,
          action = Add,
          ttl = None
        )

      StreamKey(write).value shouldBe SetKey(scopeID, indexID, terms)
    }

    "groups writes by key" in {
      val writes = Vector(
        VersionAdd(
          scopeID,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data.empty,
          None),
        VersionRemove(scopeID, docID, Unresolved, Create),
        VersionRemove(scopeID, docID, Unresolved, Create)
      )

      StreamKey.groupWritesByKey(writes) shouldBe Map(
        DocKey(scopeID, docID) -> writes)
    }
  }
}
