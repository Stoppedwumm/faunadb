package fauna.model.test

import fauna.model.runtime.fql2.stdlib
import fauna.repo.values.Value
import org.scalactic.source.Position

class FQL2EventSourceSpec
    extends FQL2StdlibHelperSpec("EventSource", stdlib.EventSourcePrototype) {

  val auth = newDB

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    evalOk(auth, "Collection.create({ name: 'Foo' })")
  }

  "toString" - {
    testSig("toString() => String")

    "works" in {
      checkOk(
        auth,
        "Foo.all().toStream().toString()",
        Value.Str("[event source]")
      )

      checkOk(
        auth,
        s"""|let legacy: Stream<Any> = Foo.all().eventSource()
            |legacy.toString()
            |""".stripMargin,
        Value.Str("[event source]")
      )
    }
  }

  "where" - {
    testSig("where(predicate: A => Boolean | Null) => EventSource<A>")

    "filters inner set" in {
      checkSameSource(
        "Foo.all()           .where(.id == 42).toStream()",
        "Foo.all().toStream().where(.id == 42)"
      )
    }
  }

  "map" - {
    testSig("map(mapper: A => B) => EventSource<B>")

    "maps inner set" in {
      checkSameSource(
        "Foo.all()           .map(.id).toStream()",
        "Foo.all().toStream().map(.id)"
      )
    }
  }

  /** NOTE: align target method calls at the same column in both queries to prove out
    * that both are equivalent despite their spans.
    */
  private def checkSameSource(queryA: String, queryB: String)(
    implicit pos: Position) = {
    val a = evalOk(auth, queryA).to[Value.EventSource]
    val b = evalOk(auth, queryB).to[Value.EventSource]
    a.set shouldBe b.set
  }
}
