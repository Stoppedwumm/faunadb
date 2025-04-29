package fauna.model.test

import fauna.atoms.DocID
import fauna.auth.Auth
import fauna.exec.ImmediateExecutionContext
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.stream.{ Event, EventTransformer, EventType, StreamValue }
import fauna.repo.values.Value
import fauna.storage.ir.{ LongV, StringV }
import org.scalactic.source.Position

class EventTransformerSpec extends FQL2Spec with StreamContextHelpers {

  implicit val ec = ImmediateExecutionContext

  var auth: Auth = _

  before {
    auth = newDB
    evalOk(auth, "Collection.create({ name: 'Foo' })")
  }

  "EventTransformer" - {
    "transforms (no filters)" in {
      val create = evalRes(auth, "Foo.create({})")
      val update = evalRes(auth, "Foo.all().first()!.update({ foo: 'bar' })")
      val remove = evalRes(auth, "Foo.all().first()!.delete()")
      val docID = create.value.to[Value.Doc].id

      inside(transformEvent(docID, create.ts).value) { case event: Event.Data =>
        event.eventType shouldBe EventType.Add
        event.cursor.ts shouldBe create.ts
      }

      inside(transformEvent(docID, update.ts).value) { case event: Event.Data =>
        event.eventType shouldBe EventType.Update
        event.cursor.ts shouldBe update.ts
      }

      inside(transformEvent(docID, remove.ts).value) { case event: Event.Data =>
        event.eventType shouldBe EventType.Remove
        event.cursor.ts shouldBe remove.ts

        // returned remove event contains the data prior
        inside(event.data.value) { case doc: Value.Doc =>
          val data = doc.versionOverride.value.data.fields
          data.get("data" :: "foo" :: Nil).value shouldBe StringV("bar")
        }
      }
    }

    "transforms (w/ filters)" - {
      def transformer(implicit pos: Position) = {
        // Equivalent to `Foo.where(.n >= 41).where(.n < 43).toStream()`
        val pred0 = evalOk(auth, "(doc) => doc.n >= 41").to[Value.Func]
        val pred1 = evalOk(auth, "(doc) => doc.n < 43").to[Value.Func]

        new EventTransformer(
          Seq(
            new EventTransformer.Filter(pred0),
            new EventTransformer.Filter(pred1)
          ))
      }

      def create(n: Int)(implicit pos: Position) = {
        val create = evalRes(auth, s"Foo.create({ n: $n })")
        val docID = create.value.to[Value.Doc].id
        val subID = docID.subID.toLong
        (docID, subID, create.ts)
      }

      "no event for creates outside of the set" in {
        val (docID, _, ts) = create(40)
        transformEvent(docID, ts, transformer) should be(empty)
      }

      "add event for documents created in the set" in {
        val (docID, _, ts) = create(42)
        inside(transformEvent(docID, ts, transformer).value) {
          case event: Event.Data =>
            event.eventType shouldBe EventType.Add
            event.cursor.ts shouldBe ts
        }
      }

      "add event for updates moving docs into the set" in {
        val (docID, subID, _) = create(40)
        val update = evalRes(auth, s"Foo.byId('$subID')!.update({ n: 42 })")

        inside(transformEvent(docID, update.ts, transformer).value) {
          case event: Event.Data =>
            event.eventType shouldBe EventType.Add
            event.cursor.ts shouldBe update.ts
        }
      }

      "update event for updates preserving docs in the set" in {
        val (docID, subID, _) = create(41)
        val update = evalRes(auth, s"Foo.byId('$subID')!.update({ n: 42 })")

        inside(transformEvent(docID, update.ts, transformer).value) {
          case event: Event.Data =>
            event.eventType shouldBe EventType.Update
            event.cursor.ts shouldBe update.ts
        }
      }

      "no events when updating documents outside of the set" in {
        val (docID, subID, _) = create(40)
        val update = evalRes(auth, s"Foo.byId('$subID')!.update({ n: 39 })")
        transformEvent(docID, update.ts, transformer) should be(empty)
      }

      "remove event for updates removing documents from the set" in {
        val (docID, subID, _) = create(42)
        val update = evalRes(auth, s"Foo.byId('$subID')!.update({ n: 43 })")

        inside(transformEvent(docID, update.ts, transformer).value) {
          case event: Event.Data =>
            event.eventType shouldBe EventType.Remove
            event.cursor.ts shouldBe update.ts

            // returned remove event contains the data prior
            inside(event.data.value) { case doc: Value.Doc =>
              val data = doc.versionOverride.value.data.fields
              data.get("data" :: "n" :: Nil).value shouldBe LongV(42)
            }
        }
      }

      "remove event when deleting documents in the set" in {
        val (docID, subID, _) = create(41)
        val delete = evalRes(auth, s"Foo.byId('$subID')!.delete()")

        inside(transformEvent(docID, delete.ts, transformer).value) {
          case event: Event.Data =>
            event.eventType shouldBe EventType.Remove
            event.cursor.ts shouldBe delete.ts

            // returned remove event contains the data prior
            inside(event.data.value) { case doc: Value.Doc =>
              val data = doc.versionOverride.value.data.fields
              data.get("data" :: "n" :: Nil).value shouldBe LongV(41)
            }
        }
      }

      "no events when removing a document outside of the set" in {
        val (docID, subID, _) = create(40)
        val delete = evalRes(auth, s"Foo.byId('$subID')!.delete()")
        transformEvent(docID, delete.ts, transformer) should be(empty)
      }
    }
  }

  private def transformEvent(
    docID: DocID,
    validTS: Timestamp,
    transformer: EventTransformer = EventTransformer.empty
  )(implicit pos: Position): Option[Event] = {
    val interp = new FQLInterpreter(auth)
    val version = ctx ! ModelStore.getVersion(auth.scopeID, docID, validTS)
    val value =
      StreamValue.DocVersion(version.get, Value.EventSource.Cursor(validTS))
    ctx ! transformer.transform(interp, value).map {
      case Result.Ok(event) => event
      case Result.Err(err)  => fail(err.failureMessage)
    }
  }
}
