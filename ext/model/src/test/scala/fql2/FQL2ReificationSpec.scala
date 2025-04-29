package fauna.model.test

import fauna.codex.json2.JSONWriter
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.fql2.serialization.{
  FQL2ValueEncoder,
  FQL2ValueMaterializer,
  ValueFormat
}
import fauna.model.runtime.Effect
import fauna.repo.values.{ Value, ValueReification }
import io.netty.buffer.ByteBufAllocator
import org.scalactic.source.Position

// NOTE: This tests the set and stream functions for reification. Reifying actual
// values is tested in `repo/ValueReificationSpec`.
class FQL2ReificationSpec extends FQL2Spec {
  val auth = newDB

  "tests all of the set types" in {
    import fauna.model.runtime.fql2._
    val v: ValueSet = ArraySet(Value.Array())
    // NOTE: Add a test below when adding a new ValueSet!
    v match {
      case _: ArraySet | _: ConcatSet | _: DistinctSet | _: DropSet | _: FlatMapSet |
          _: TakeSet | _: IndexSet | _: ProjectedSet | _: ReverseSet |
          _: WhereFilterSet | _: OrderedSet | _: SequenceSet | _: SingletonSet |
          _: PagedSet =>
    }
  }

  "reifies ArraySet" in {
    reify("[].toSet()")
    reify("[1, 2].toSet()")
  }

  "reifies ConcatSet" in {
    reify("[1, 2].toSet().concat([3, 4].toSet())")
  }

  "reifies DistinctSet" in {
    reify("[1, 2, 3, 2].toSet().distinct()")
  }

  "reifies DropSet" in {
    reify("[1, 2, 3, 2].toSet().drop(2)")
  }

  "reifies FlatMapSet" in {
    reify("[1, 2].toSet().flatMap(v => [v, v + 10].toSet())")
  }

  "reifies TakeSet" in {
    reify("[1, 2].toSet().take(1)")
  }

  "reifies OrderedSet" in {
    reify("[1, 2].toSet().order(asc(n => n))")
    reify("[1, 2].toSet().order(desc(n => n))")
  }

  "reifies SingletonSet" in {
    reify("Set.single(0)")
  }

  "reifies PagedSet" in {
    reify(
      "[1, 2, 3, 4, 5].toSet().pageSize(2)",
      alt = Some("[1, 2, 3, 4, 5].toSet()"))
  }

  "reifies IndexSet" in {
    evalOk(
      auth,
      s"""|Collection.create({
          |  name: 'Foo',
          |  indexes: {
          |    idx: {
          |      values: [{ field: 'bar' }]
          |    }
          |  }
          |})""".stripMargin
    )
    reify("Foo.all()")
    reify("Foo.all({from: Foo.byId('123')})")
    reify("Foo.all({from: Foo.byId('123'), to: Foo.byId('456')})")
    reify("Foo.idx({from: ['xxx', Foo.byId('123')]})")
    reify("Foo.idx({from: ['xxx', Foo.byId('123')], to: 'xxx'})")
    reify("Foo.idx({from: ['xxx', Foo.byId('123')], to: ['xxx', Foo.byId('456')]})")
  }

  "reifies partials" in {
    evalOk(
      auth,
      s"""|Collection.create({
          |  name: 'Partials',
          |  indexes: {
          |    idx: {
          |      values: [{ field: 'foo' }]
          |    }
          |  }
          |})""".stripMargin
    )

    evalOk(
      auth,
      """|Set.sequence(0, 10).forEach(_ => {
         |  Partials.create({
         |    foo: 'bar'
         |  })
         |})
         |""".stripMargin
    )

    reify(
      s"""|let data = Partials.idx().map(.data).first()!
          |Partials.all().where(.foo == data.foo).paginate(1).data
          |""".stripMargin
    )

    reify(
      s"""|let data = Partials.idx().map(.data).first()!
          |Partials.all().where(.data == { foo: data.foo }).paginate(1).data
          |""".stripMargin
    )
  }

  "reifies ProjectedSet" in {
    // FIXME: The span is stored here, and even after being reified, it retains
    // the original *query* span, which is incorrect.
    reify("[1, 2].toSet().map(v => v + 1)")
  }

  "reifies ReverseSet" in {
    reify("[1, 2, 3, 2].toSet().reverse()")
    reify("[1, 2, 3, 2].toSet().reverse().reverse()")
  }

  "reifies WhereSet" in {
    reify("[1, 2].toSet().where(v => v > 1)")
  }

  "reifies SequenceSet" in {
    reify("Set.sequence(0, 5)")
    reify("Set.sequence(0, 5).reverse()")
  }

  "reifies Stream" in {
    evalOk(
      auth,
      """|Collection.create({
         |  name: 'Bar',
         |  indexes: {
         |    byBaz: {
         |      terms: [{ field: '.baz' }]
         |    }
         |  }
         |})""".stripMargin
    )
    reify("Bar.all().toStream()")
    reify("Bar.all().changesOn(.fizz, .buzz[0], .fizz.buzz, .fizz.buzz[0])")
    reify("Bar.all().where(.foo == 42).toStream()")
    reify("Bar.all().where(.foo == 42).where(.bar == 44).toStream()")
    reify("Bar.where(.foo == 42).toStream()")
    reify("Bar.where(.foo == 42).where(.bar == 44).toStream()")
    reify("Set.single(Bar.create({})).toStream()")
    reify("Bar.byBaz(42).toStream()")
    reify("Bar.byBaz(42).changesOn(.baz)")
  }

  /** This evals the given query. Then, it reifies the resulting value, and evals
    * that expr, and ensures that it is the same value as the initial result.
    */
  private def reify(
    query: String,
    typecheck: Boolean = true,
    alt: Option[String] = None)(implicit pos: Position) = {
    import FQLInterpreter.TypeMode

    val original = evalOk(auth, query, typecheck)
    val (expr, vars) = ValueReification.reify(original)
    val vctx = ValueReification.vctx(vars)
    val typemode = if (typecheck) TypeMode.InferType else TypeMode.Disabled

    val reified =
      eval(
        auth,
        expr,
        typemode,
        vctx,
        Effect.Read,
        performanceHintsEnabled = false).res
        .getOrElse(fail(s"fail to reify expr $expr"))
        .value

    val txnTS = Clock.time
    val matOriginal =
      encode(alt.map(evalOk(auth, _, typecheck)).getOrElse(original), txnTS)
    val matReified = encode(reified, txnTS)

    if (matOriginal != matReified) {
      fail(
        s"""|Reified value produced a different result than original value.
            |
            |Results:
            |  Original: $matOriginal
            |   Reified: $matReified
            |
            |Values:
            |  Original: $original
            |   Reified: $reified
            |
            |Reified expr:
            |  Expr: $expr
            |  Vars: $vars
            |""".stripMargin
      )
    }
  }

  private def encode(value: Value, txnTS: Timestamp)(implicit pos: Position) = {
    val intp = new FQLInterpreter(auth)
    val materialized =
      (ctx ! FQL2ValueMaterializer.materialize(intp, value)) match {
        case Result.Ok(value) => value
        case Result.Err(err) =>
          val errs = err.errors.map(_.renderWithSource(Map.empty)).mkString("\n\n")
          fail(s"Fail to materialize value $value:\n$errs")
      }

    val buf = ByteBufAllocator.DEFAULT.buffer
    FQL2ValueEncoder.encode(
      ValueFormat.Simple,
      JSONWriter(buf),
      materialized,
      txnTS
    )
    buf.toUTF8String
  }

}
