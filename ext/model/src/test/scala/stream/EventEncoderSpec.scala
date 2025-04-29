package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.lang._
import fauna.model.stream._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.storage.ops._
import scala.concurrent.duration._

class EventEncoderSpec extends Spec {

  val txnTS = Timestamp.ofMicros(12345)
  val docID = DocID(SubID.MinValue, CollectionID(1234))
  val idxID = IndexID(1025)

  def encoder: EventEncoder = encoder(EventField.Defaults)
  def encoder(fields: Set[EventField]) = EventEncoder(txnTS, fields)

  "EventEncoder" - {

    "should encode a start event" in {
      encoder.encode(StreamStart(txnTS)) shouldBe
        ObjectL(
          "type" -> StringL("start"),
          "txn" -> LongL(txnTS.micros),
          "event" -> LongL(txnTS.micros)
        )
    }

    "should encode an error event" in {
      encoder.encode(StreamError("code", "desc")) shouldBe
        ObjectL(
          "type" -> StringL("error"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "code" -> StringL("code"),
            "description" -> StringL("desc")
          ))
    }

    "should encode a new version" in {
      val event = VersionAdded(
        txnTS,
        VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data(MapV("data" -> MapV("foo" -> "bar"))),
          diff = None
        ))

      encoder.encode(event) shouldBe
        ObjectL(
          "type" -> StringL("version"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Create),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros),
              "data" -> ObjectL("foo" -> StringL("bar"))
            ))
        )
    }

    "should encode an update" in {
      val event = VersionAdded(
        txnTS,
        VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          // Storage has only create and delete.
          // Update is derived from the presence of a diff.
          Create,
          SchemaVersion.Min,
          Data(MapV("data" -> MapV("foo" -> "bar"))),
          Some(Diff(MapV("data" -> MapV("foo" -> "baz"))))
        )
      )

      encoder.encode(event) shouldBe
        ObjectL(
          "type" -> StringL("version"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Update),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros),
              "data" -> ObjectL("foo" -> StringL("bar"))
            ))
        )
    }

    "should encode a delete" in {
      val event = VersionAdded(
        txnTS,
        VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          Delete,
          SchemaVersion.Min,
          Data.empty,
          Some(Diff(MapV("data" -> MapV("foo" -> "bar"))))
        ))

      encoder.encode(event) shouldBe
        ObjectL(
          "type" -> StringL("version"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Delete),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros),
              "data" -> ObjectL("foo" -> StringL("bar"))
            ))
        )
    }

    "should encode history rewrite" in {
      val event = VersionAdded(
        txnTS,
        VersionAdd(
          ScopeID.RootID,
          docID,
          AtValid(txnTS - 1.day),
          Create,
          SchemaVersion.Min,
          Data(MapV("data" -> MapV("foo" -> "bar"))),
          None
        ))

      encoder.encode(event) shouldBe
        ObjectL(
          "type" -> StringL("history_rewrite"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Create),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL((txnTS - 1.day).micros),
              "data" -> ObjectL("foo" -> StringL("bar"))
            )
          )
        )
    }

    "should encode prev data" in {
      val event = VersionAdded(
        txnTS,
        VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data(MapV("data" -> MapV("field" -> "new"))),
          Some(Diff(MapV("data" -> MapV("field" -> "old"))))
        ))

      encoder(Set(EventField.Prev)).encode(event) shouldBe
        ObjectL(
          "type" -> StringL("version"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "prev" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros),
              "data" -> ObjectL("field" -> StringL("old"))
            ))
        )
    }

    "should encode a diff" in {
      val event = VersionAdded(
        txnTS,
        VersionAdd(
          ScopeID.RootID,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data(MapV("data" -> MapV("fieldA" -> "new"))),
          Some(
            Diff(
              MapV(
                "data" -> MapV(
                  "fieldA" -> NullV,
                  "fieldB" -> "old"
                )))))
      )

      encoder(Set(EventField.Diff)).encode(event) shouldBe
        ObjectL(
          "type" -> StringL("version"),
          "txn" -> LongL(txnTS.micros),
          "event" ->
            ObjectL(
              "diff" -> ObjectL(
                "ref" -> RefL(ScopeID.RootID, docID),
                "ts" -> LongL(txnTS.micros),
                "data" -> ObjectL(
                  "fieldA" -> StringL("new"),
                  "fieldB" -> NullL
                )))
        )
    }

    "should encode a removed version" in {
      val event =
        VersionRemoved(
          VersionRemove(
            ScopeID.RootID,
            docID,
            Unresolved,
            Delete
          ))

      encoder.encode(event) shouldBe
        ObjectL(
          "type" -> StringL("history_rewrite"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Delete),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros)
            ))
        )
    }

    "should a removed document" in {
      encoder.encode(DocumentRemoved(DocRemove(ScopeID.RootID, docID))) shouldBe
        ObjectL(
          "type" -> StringL("history_rewrite"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Delete),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros)
            ))
        )
    }

    "should encoded an added set event" in {
      val event =
        SetAdded(
          SetAdd(
            ScopeID.RootID,
            idxID,
            Vector(StringV("term")),
            Vector(IndexTerm(StringV("value"))),
            docID,
            Unresolved,
            Add,
            None
          ),
          isPartitioned = false
        )

      encoder(EventField.All).encode(event) shouldBe
        ObjectL(
          "type" -> StringL("set"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Add),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros)
            ),
            "index" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, idxID.toDocID),
              "terms" -> ArrayL(StringL("term")),
              "values" -> ArrayL(StringL("value"))
            )
          )
        )
    }

    "should encoded a removed set event" in {
      val event =
        SetRemoved(
          SetRemove(
            ScopeID.RootID,
            idxID,
            Vector(StringV("term")),
            Vector(IndexTerm(StringV("value"))),
            docID,
            Unresolved,
            Add,
            None
          ),
          isPartitioned = false
        )

      encoder(EventField.All).encode(event) shouldBe
        ObjectL(
          "type" -> StringL("set"),
          "txn" -> LongL(txnTS.micros),
          "event" -> ObjectL(
            "action" -> ActionL(Add),
            "document" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, docID),
              "ts" -> LongL(txnTS.micros)
            ),
            "index" -> ObjectL(
              "ref" -> RefL(ScopeID.RootID, idxID.toDocID),
              "terms" -> ArrayL(StringL("term")),
              "values" -> ArrayL(StringL("value"))
            )
          )
        )
    }

    "should ommit partition terms from set events" in {
      val add =
        SetAdded(
          SetAdd(
            ScopeID.RootID,
            idxID,
            Vector(StringV("termA"), StringV("termB"), LongV(0)),
            Vector(IndexTerm(StringV("value"))),
            docID,
            Unresolved,
            Add,
            None
          ),
          isPartitioned = true
        )

      val remove =
        SetRemoved(
          SetRemove(
            ScopeID.RootID,
            idxID,
            Vector(StringV("termA"), StringV("termB"), LongV(0)),
            Vector(IndexTerm(StringV("value"))),
            docID,
            Unresolved,
            Add,
            None
          ),
          isPartitioned = true
        )

      def assertNoPartitionKeys(lit: Literal) =
        lit match {
          case obj: ObjectL =>
            val path = List("event", "index", "terms")
            val terms = ArrayV(StringV("termA"), StringV("termB"))
            obj.irValue.get(path).value shouldBe terms
          case other =>
            fail(s"unexpected literal $other")
        }

      assertNoPartitionKeys(encoder(EventField.All).encode(add))
      assertNoPartitionKeys(encoder(EventField.All).encode(remove))
    }
  }
}
