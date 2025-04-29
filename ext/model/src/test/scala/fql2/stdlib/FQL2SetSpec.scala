package fauna.model.test

import fauna.model.runtime.fql2.{ stdlib, FQLInterpreter, PagedSet, SequenceSet }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }

class FQL2SetSpec extends FQL2StdlibHelperSpec("Set", stdlib.SetPrototype) {
  val auth = newDB

  override def checkAllTested() = {
    val arrFields = typer.typeShapes("Array").fields.keySet
    val setFields = typer.typeShapes("Set").fields.keySet.filter(!_.startsWith("$"))

    // Fields on both are iterable functions, so we don't test those here.
    val allFields = setFields.filter { !arrFields.contains(_) }
    allFields.foreach { field =>
      if (!skipFields.contains(field) && !tests.contains(field)) {
        fail(s"function Set.$field is not tested")
      }
    }
  }

  def testStaticSig(sig: String*) =
    s"has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.SetCompanion, test.function) shouldBe sig
    }

  "count" - {
    testSig("count() => Number")

    "returns the number of elements in a set" in {
      checkOk(auth, "[].toSet().count()", Value.Number(0))
      checkOk(auth, "[1].toSet().count()", Value.Number(1))
      checkOk(auth, "[1, 2].toSet().count()", Value.Number(2))
    }
  }

  // Note that this is tested more thoroughly elsewhere.
  "paginate" - {
    testSig(
      "paginate() => { data: Array<A>, after: String | Null }",
      "paginate(size: Number) => { data: Array<A>, after: String | Null }"
    )

    "converts set to a page" in {
      checkOk(auth, "[].toSet().paginate()", Value.Struct("data" -> Value.Array()))
    }

    "cursor can be paginated in-transaction" in {
      checkOk(
        auth,
        """|let s = [1, 2, 3].toSet()
           |let p1 = s.paginate(1)
           |let p2 = Set.paginate(p1.after!, 1)            // raw cursor
           |let p3 = Set.paginate(p2.after!.toString(), 1) // cursor toString
           |[p1.data, p2.data, p3.data].flatten()""".stripMargin,
        Value.Array(Value.Int(1), Value.Int(2), Value.Int(3))
      )
    }

    "cursors embed page size" in {
      checkOk(
        auth,
        """|let s = [1, 2, 3].toSet()
           |let p1 = s.paginate(1)
           |let p2 = Set.paginate(p1.after!)
           |let p3 = Set.paginate(p2.after!)
           |[p1.data, p2.data, p3.data].flatten()""".stripMargin,
        Value.Array(Value.Int(1), Value.Int(2), Value.Int(3))
      )
    }
  }

  "single" - {
    testStaticSig("single(element: A) => Set<A>")

    "returns a set with just one element" in {
      checkOk(
        auth,
        "Set.single(0).paginate(16)",
        Value.Struct("data" -> Value.Array(Value.Int(0)))
      )

      checkOk(
        auth,
        "Set.single(0).where(n => n > 0).paginate(16)",
        Value.Struct("data" -> Value.Array.empty)
      )
    }
  }

  "sequence" - {
    testStaticSig("sequence(from: Number, until: Number) => Set<Number>")

    "returns a set with the expected contents" in {
      checkOk(
        auth,
        "Set.sequence(0, 4).paginate(16)",
        Value.Struct(
          "data" -> Value
            .Array(Value.Int(0), Value.Int(1), Value.Int(2), Value.Int(3))))
      checkOk(
        auth,
        "Set.sequence(0, 4).paginate(4)",
        Value.Struct(
          "data" -> Value
            .Array(Value.Int(0), Value.Int(1), Value.Int(2), Value.Int(3))))
    }

    "out of order args" in {
      checkOk(
        auth,
        "Set.sequence(4, 0).paginate(16)",
        Value.Struct("data" -> Value.Array()))
    }
  }

  "toArray" - {
    testSig("toArray() => Array<A>")

    "converts set to an array" in {
      checkOk(auth, "[].toSet().toArray()", Value.Array())
      checkOk(auth, "[1].toSet().toArray()", Value.Array(Value.Number(1)))
      checkOk(
        auth,
        "[1, 2].toSet().toArray()",
        Value.Array(Value.Int(1), Value.Int(2)))
    }
  }

  "pageSize" - {
    testSig("pageSize(size: Number) => Set<A>")

    "returns a set with the expected contents" in {
      checkOk(
        auth,
        "Set.sequence(0, 4).pageSize(2)",
        PagedSet(
          SequenceSet(0, 4),
          2,
          FQLInterpreter.StackTrace(Seq(Span(27, 30, Src.Id(Src.QueryName))))))
    }
  }

  "toStream" - {
    testSig("toStream() => EventSource<A>")
  }

  "eventSource" - {
    testSig("eventSource() => EventSource<A>")
  }

  "changesOn" - {
    testSig("changesOn(...fields: A => Any) => EventSource<A>")
  }

  "eventsOn" - {
    testSig("eventsOn(...fields: A => Any) => EventSource<A>")
  }
}
