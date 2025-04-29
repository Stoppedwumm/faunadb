package fauna.model.test

import fauna.atoms.{ DocID, UserIndexID }
import fauna.auth.Auth
import fauna.model.runtime.fql2._
import fauna.repo.schema.Path
import fauna.repo.values.Value
import fauna.storage.index.NativeIndexID
import org.scalactic.source.Position

class FQL2StreamsSpec extends FQL2Spec {

  var auth: Auth = _
  var id: Long = _

  before {
    auth = newDB
    evalOk(
      auth,
      """|Collection.create({
         |  name: 'Foo',
         |  indexes: {
         |    byBar: {
         |      terms: [{ field: '.bar' }],
         |      values: [{ field: '.bar' }]
         |    },
         |    allChanges: {
         |      values: [{ field: '.ts' }]
         |    }
         |  }
         |})""".stripMargin
    )
    id = evalOk(auth, "Foo.create({}).id").as[Long]
  }

  "FQL2Streams" - {

    "streams a singleton set" in {
      val stream = evalOk(auth, s"Set.single(Foo.byId($id)).toStream()")
      val source = stream.to[Value.EventSource].set.to[SingletonSet]
      source.value.as[DocID].subID.toLong shouldBe id
    }

    "streams an user index" in {
      val stream = evalOk(auth, s"Foo.byBar(42).toStream()")
      val source = stream.to[Value.EventSource].set.to[IndexSet]
      source.config.id should matchPattern { case UserIndexID(_) => }
    }

    "converts documents to changes index" in {
      val stream = evalOk(auth, "Foo.all().toStream()").to[Value.EventSource]
      val source = stream.set.to[IndexSet]
      source.config.id shouldBe NativeIndexID.ChangesByCollection.id
    }

    "allows for filtered sets" in {
      evalOk(auth, "Foo.where(.bar == 42).toStream()")
      evalOk(auth, "Foo.all().where(.bar == 42).toStream()")
      evalOk(auth, "Foo.all().where(.bar == 42).toStream()")
      evalOk(auth, "Foo.where(.bar == 42).where(.baz == 0).toStream()")
      evalOk(auth, "Foo.all().where(.bar == 42).where(.baz == 0).toStream()")
      evalOk(auth, s"Set.single(Foo.byId($id)!).where(.bar == 42).toStream()")
    }

    "tracks watched fields" in {
      def watchedFields(query: String) =
        evalOk(auth, query).to[Value.EventSource].watchedFields

      watchedFields("Foo.all().toStream()") shouldBe empty
      watchedFields("Foo.all().changesOn()") shouldBe empty

      all(
        Seq(
          watchedFields("Foo.all().changesOn(.bar, .baz[0])"),
          watchedFields("Foo.allChanges().changesOn(.bar, .baz[0])"),
          watchedFields(s"Set.single(Foo.byId($id)!).changesOn(.bar, .baz[0])")
        )
      ) should contain.inOrderOnly(
        Path(Right("bar")),
        Path(Right("baz"), Left(0))
      )

      watchedFields(s"Foo.byBar(42).changesOn(.bar, .id)") should
        contain.inOrderOnly(
          Path(Right("bar")),
          Path(Right("id"))
        )
    }

    "rejects invalid shape" in {
      def reject(query: String, message: String)(implicit pos: Position) =
        evalErr(auth, query) should matchPattern {
          case QueryRuntimeFailure.Simple("invalid_receiver", `message`, _, _) =>
        }

      reject(
        """|Database.all().toStream()
           |""".stripMargin,
        "can't call `.toStream()` because only collections and user defined indexes are streamable."
      )

      reject(
        """|at(Time.now().subtract(1, 'day')) {
           |  Foo.all().toStream()
           |}
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported for sets created within an `at()` expression."
      )

      reject(
        """|at(Time.now().subtract(1, 'day')) {
           |  Foo.byBar(42).toStream()
           |}
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported for sets created within an `at()` expression."
      )

      reject(
        s"""|at(Time.now().subtract(1, 'day')) {
            |  Set.single(Foo.byId($id)).toStream()
            |}
            |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported for sets created within an `at()` expression."
      )

      reject(
        """|let all = Foo.all()
           |at(Time.now().subtract(1, 'day')) {
           |  all.where(.foo == 42).toStream()
           |}
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported for sets created within an `at()` expression."
      )

      reject(
        s"""|Set.single(10).toStream()
            |""".stripMargin,
        "can't call `.toStream()` because streaming on `Set.single()` is only supported if its element is a document."
      )

      reject(
        """|Foo.all()
           |  .flatMap(_ => [].toSet())
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on `.flatMap()` sets."
      )

      reject(
        """|Foo.all()
           |  .concat(Foo.all())
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on concatenated sets."
      )

      reject(
        """|Foo.all()
           |  .take(10)
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on sets returned from `.take()`."
      )

      reject(
        """|Foo.all()
           |  .drop(10)
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on sets returned from `.drop()`."
      )

      reject(
        """|Foo.all()
           |  .order(.asc)
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on sets returned from `.order()`."
      )

      reject(
        """|Foo.all()
           |  .reverse()
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on sets that have been reversed."
      )

      reject(
        """|Foo.all()
           |  .distinct()
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on distinct sets."
      )

      reject(
        """|[].toSet()
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on array sets."
      )

      reject(
        """|Set.sequence(1, 10)
           |  .toStream()
           |""".stripMargin,
        "can't call `.toStream()` because streaming is not supported on sets created from `Set.sequence()`."
      )

      reject(
        """|Foo.all({from: Foo.byId(1234)}).toStream()
           |""".stripMargin,
        "can't call `.toStream()` because range queries are not supported yet, use `.where()` method instead."
      )

      reject(
        """|Foo.byBar(42, {from: 0}).toStream()
           |""".stripMargin,
        "can't call `.toStream()` because range queries are not supported yet, use `.where()` method instead."
      )
    }

    "reject invalid .changesOn(..) position" in {
      def reject(query: String, message: String)(implicit pos: Position) =
        evalErr(auth, query) should matchPattern {
          case QueryRuntimeFailure.Simple("invalid_receiver", `message`, _, _) =>
        }

      reject(
        s"""|Set.single(Foo.byId($id)!).map(.bar).changesOn(.bar)
            |""".stripMargin,
        "can't call `.changesOn()` because it was not called on the source set."
      )

      reject(
        """|Foo.all().map(.bar).changesOn(.bar)
           |""".stripMargin,
        "can't call `.changesOn()` because it was not called on the source set."
      )

      reject(
        """|Foo.byBar(42).map(.bar).changesOn(.bar)
           |""".stripMargin,
        "can't call `.changesOn()` because it was not called on the source set."
      )
    }

    "reject invalid .changesOn(..) field set" in {
      def reject(query: String, message: String)(implicit pos: Position) =
        evalErr(auth, query) should matchPattern {
          case QueryRuntimeFailure.Simple("invalid_argument", `message`, _, _) =>
        }

      reject(
        s"""|Foo.byBar('bar').changesOn(.fizz, .buzz[0])
            |""".stripMargin,
        "invalid argument `fields`: `fizz`, `buzz[0]` not covered by the `byBar` index."
      )
    }
  }
}
