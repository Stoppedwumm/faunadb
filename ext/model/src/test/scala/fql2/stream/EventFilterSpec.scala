package fauna.model.test

import fauna.atoms.DocID
import fauna.auth.Auth
import fauna.exec.ImmediateExecutionContext
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.runtime.stream.{ EventFilter, StreamValue }
import fauna.repo.schema.Path
import fauna.repo.values.Value

class FQL2EventFilterSpec extends FQL2Spec with StreamContextHelpers {

  implicit val ec = ImmediateExecutionContext

  var auth: Auth = _
  var interp: FQLInterpreter = _

  before {
    auth = newDB
    interp = new FQLInterpreter(auth)
    evalOk(auth, "Collection.create({ name: 'Foo' })")
  }

  "EventFilter" - {

    "keep all when no watched fields" in {
      val create = evalRes(auth, "Foo.create({})")
      val update = evalRes(auth, "Foo.all().first()!.update({})")
      val delete = evalRes(auth, "Foo.all().first()!.delete()")
      val doc = create.value.to[Value.Doc]

      val filter = EventFilter.empty
      assert(ctx ! keep(filter, doc.id, create.ts), "didn't keep create")
      assert(ctx ! keep(filter, doc.id, update.ts), "didn't keep update")
      assert(ctx ! keep(filter, doc.id, delete.ts), "didn't keep delete")
    }

    "keep only changes to watched fields" in {
      val create = evalRes(auth, "Foo.create({ foo: 1, bar: 1 })")
      val update1 = evalRes(auth, "Foo.all().first()!.update({ foo: 1, bar: 2 })")
      val update2 = evalRes(auth, "Foo.all().first()!.update({ foo: 2, bar: 2 })")
      val update3 = evalRes(auth, "Foo.all().first()!.update({ foo: 2, bar: 2 })")
      val delete = evalRes(auth, "Foo.all().first()!.delete()")
      val doc = create.value.to[Value.Doc]

      val filter =
        new EventFilter(watchedFields = Seq(
          Path(Right("foo")),
          Path(Right("bar"))
        ))

      assert(ctx ! keep(filter, doc.id, create.ts), "didn't keep create")
      assert(ctx ! keep(filter, doc.id, update1.ts), "didn't keep 1st update")
      assert(ctx ! keep(filter, doc.id, update2.ts), "didn't keep 2nd update")
      assert(!(ctx ! keep(filter, doc.id, update3.ts)), "kept 3nd update")
      assert(ctx ! keep(filter, doc.id, delete.ts), "didn't keep delete")
    }

    "discard creates/deletes witout watched fields" in {
      val create = evalRes(auth, "Foo.create({})")
      val update1 = evalRes(auth, "Foo.all().first()!.update({ foo: 1 })")
      val update2 = evalRes(auth, "Foo.all().first()!.update({ foo: null })")
      val delete = evalRes(auth, "Foo.all().first()!.delete()")
      val doc = create.value.to[Value.Doc]

      val filter =
        new EventFilter(watchedFields = Seq(
          Path(Right("foo"))
        ))

      assert(!(ctx ! keep(filter, doc.id, create.ts)), "kept create")
      assert(ctx ! keep(filter, doc.id, update1.ts), "didn't keep 1st update")
      assert(ctx ! keep(filter, doc.id, update2.ts), "didn't keep 2nd update")
      assert(!(ctx ! keep(filter, doc.id, delete.ts)), "kept delete")
    }
  }

  private def keep(filter: EventFilter, docID: DocID, validTS: Timestamp) = {
    val version = ctx ! ModelStore.getVersion(auth.scopeID, docID, validTS)
    val value =
      StreamValue.DocVersion(version.get, Value.EventSource.Cursor(validTS))
    filter.keep(interp, value)
  }
}
