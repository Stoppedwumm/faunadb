package fauna.model.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{
  FQLInterpreter,
  IndexSet,
  QueryCheckFailure,
  QueryRuntimeFailure
}
import fauna.repo.schema.ConstraintFailure
import fauna.repo.values.{ Value, ValueType }
import fauna.repo.Store
import fauna.storage.doc.Data
import fauna.storage.ir.{ ArrayV, MapV, TimeV }
import fql.ast.{ Span, Src }
import fql.error.TypeError
import fql.parser.Tokens
import java.time.Instant

class FQL2DocsSpec extends FQL2Spec {
  "FQL2Docs" - {
    "writes" - {
      val auth = newAuth
      val coll = mkColl(auth, "Author")

      "basics" in {
        val res = evalOk(auth, s"""Author.create({ foo: "bar" })""")
        res should matchPattern { case Value.Doc(_, _, _, _, _) => }
      }

      // `data` is special and covered in another test.
      for (f <- Tokens.ReservedFieldNames + "exists" - "data") {
        s"rejects reserved field `$f`" in {
          // id may be set in creates.
          if (f != "id") {
            val err = evalErr(auth, s"""Author.create({ $f: "bar" })""")
            err should matchPattern {
              case QueryRuntimeFailure.Simple("constraint_failure", _, _, _) =>
            }
          }
          val updateErr = evalErr(
            auth,
            s"""
               |let author = Author.create({ name: "bar" })
               |author.update({ $f: "baz" })
               |""".stripMargin)
          updateErr should matchPattern {
            case QueryRuntimeFailure.Simple("constraint_failure", _, _, _) =>
          }
          updateErr
            .asInstanceOf[QueryRuntimeFailure]
            .constraintFailures
            .forall(_.isInstanceOf[ConstraintFailure.ReservedField]) shouldBe true
        }
      }

      "can set, read and update ttl" in {
        // FIXME: we don't have timestamp manipulation functions yet, so
        // get a timestamp from a document.
        val srcDoc = DocID(SubID(1), coll)
        val updatedTsDoc = DocID(SubID(2), coll)
        val updatedTime = Timestamp(Instant.now().plusSeconds(10000000))
        ctx ! Store.insertUnmigrated(
          auth.scopeID,
          srcDoc,
          Data(MapV("data" -> MapV("some_ts" -> TimeV(Timestamp.MaxMicros)))))
        ctx ! Store.insertUnmigrated(
          auth.scopeID,
          updatedTsDoc,
          Data(MapV("data" -> MapV("some_ts" -> TimeV(updatedTime)))))

        val res1 = evalOk(
          auth,
          s"""|let ttl = Author.byId("${srcDoc.subID.toLong}")!.some_ts
              |Author.create({ ttl: ttl, foo: "bar" })""".stripMargin
        ).to[Value.Doc]

        val docID = res1.asInstanceOf[Value.Doc].id

        // TTL is returned properly
        val res2 = evalOk(auth, s"""Author.byId("${docID.subID.toLong}")!.ttl""")
        res2 shouldEqual Value.Time(Timestamp.MaxMicros)

        // TTL is stored properly
        val res3 = (ctx ! Store.getUnmigrated(auth.scopeID, docID)).get
        res3.data.fields.get(List("ttl")) shouldEqual Some(
          TimeV(Timestamp.MaxMicros))

        // TTL updated
        val res4 = evalOk(
          auth,
          s"""|let ttl = Author.byId("${updatedTsDoc.subID.toLong}")!.some_ts
              |let author = Author.byId("${docID.subID.toLong}")!
              |author.update({"ttl": ttl}).ttl
              |""".stripMargin
        )
        res4 shouldEqual Value.Time(updatedTime)

        // updated TTL is stored properly
        val res5 = (ctx ! Store.getUnmigrated(auth.scopeID, docID)).get
        res5.data.fields.get(List("ttl")) shouldEqual Some(TimeV(updatedTime))
      }

      "update/replace short-circuit on null doc" in {
        val res1 = evalOk(auth, s"Author.byId('123')?.update({ foo: 'bar' })")
        res1 shouldEqual Value.Null(Span.Null)
        val res2 = evalOk(auth, s"Author.byId('123')?.replace({ foo: 'bar' })")
        res2 shouldEqual Value.Null(Span.Null)
      }

      // NOTE: partials are not supposed to come from query arguments (globalCtx),
      // therefore they don't typecheck. This test disables type checking for its
      // queries.
      "can write a partial value to a document" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Book',
             |  indexes: {
             |    titles: {
             |      values: [{ field: 'title' }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(auth, "Book.create({ title: 'A book' })")
        val partial = evalOk(auth, "Book.titles().map(.data).first()")
        partial should matchPattern { case _: Value.Struct.Partial => }

        val id =
          evalOk(
            auth,
            "Author.create({ name: 'Bob', book: book }).id",
            globalCtx = Map("book" -> partial),
            typecheck = false
          ).as[Long]

        evalOk(auth, s"Author.byId('$id')!.book.title")
          .as[String] shouldBe "A book"

        evalOk(
          auth,
          s"Author.byId('$id').update({ books: [ book ] })",
          globalCtx = Map("book" -> partial),
          typecheck = false
        )

        evalOk(auth, s"Author.byId('$id')!.books[0].title")
          .as[String] shouldBe "A book"

        evalOk(
          auth,
          s"Author.byId('$id').replace({ books: { favorite: book } })",
          globalCtx = Map("book" -> partial),
          typecheck = false
        )

        evalOk(auth, s"Author.byId('$id')!.books.favorite.title")
          .as[String] shouldBe "A book"
      }
    }

    "reads" - {
      val auth = newAuth
      mkColl(auth, "Author")

      "can get a set" in {
        evalOk(auth, s"""Author.create({ foo: "bar" })""")

        val author = evalOk(auth, "Author")
        val res1 = evalOk(auth, s"""Author.all()""").to[IndexSet]
        res1.parent shouldEqual author
        res1.name shouldEqual "all"
        res1.args shouldEqual Vector()
      }

      "can get doc byId" in {
        val doc = evalOk(auth, s"""Author.create({ foo: "bar" })""").to[Value.Doc]

        val res = evalOk(auth, s"""Author.byId("${doc.id.subID.toLong}")""")
        res shouldEqual doc
      }

      "can get schema doc byName" in {
        evalOk(auth, s"Collection.byName('Author')") shouldBe Value.Doc(
          CollectionID(1024).toDocID)
      }

      "exists returns false if document does not exist" in {
        evalOk(auth, s"""Author.byId("1234").exists()""") shouldBe Value.False
      }

      "doc == null returns true if document does not exist" in {
        evalOk(auth, s"""Author.byId("1234") == null""") shouldBe Value.True
      }

      "exists returns true for existing document" in {
        evalOk(auth, s"""Author.create({}).exists()""") shouldBe Value.True
      }

      "doc == null returns false for existing document" in {
        evalOk(auth, s"""Author.create({}) == null""") shouldBe Value.False
      }

      "exists returns false for a deleted document" in {
        evalOk(
          auth,
          s"""|let author = Author.create({})
              |author.delete()
              |author.exists()
              |""".stripMargin) shouldBe Value.False
      }

      "doc == null returns true for a deleted document" in {
        evalOk(
          auth,
          s"""|let author = Author.create({})
              |author.delete()
              |author == null
              |""".stripMargin) shouldBe Value.True
      }

      "can project non-existant docs" in {
        evalOk(
          auth,
          s"""|let author = Author.create({ name: "Write" })
              |author.delete()
              |author { id, name }
              |""".stripMargin
        ) shouldBe Value.Null(Span.Null)
      }

      "can get doc with firstWhere .id == \"id\"" in {
        val doc = evalOk(auth, s"""Author.create({ foo: "bar" })""").to[Value.Doc]

        val res =
          evalOk(auth, s"""Author.firstWhere(.id == "${doc.id.subID.toLong}")""")
        res shouldEqual doc
      }

      "can compare id with id" in {
        val doc = evalOk(auth, s"""Author.create({ foo: "bar" })""").to[Value.Doc]

        val res0 = evalOk(
          auth,
          s"""|let doc = Author.byId("${doc.id.subID.toLong}")
              |
              |doc.id == doc.id
           """.stripMargin)
        res0 shouldEqual Value.True

        val res1 = evalOk(
          auth,
          s"""|let doc1 = Author.byId("${doc.id.subID.toLong}")
              |let doc2 = Author.create({})
              |
              |doc1.id == doc2.id
           """.stripMargin
        )
        res1 shouldEqual Value.False

        // Make sure the .where is filtering something
        evalOk(auth, s"""Author.create({ foo: "bar" })""")
        evalOk(auth, s"""Author.create({ foo: "bar" })""")
        evalOk(auth, s"""Author.create({ foo: "bar" })""")
        val res2 = evalOk(
          auth,
          s"""|let doc_id = Author.byId("${doc.id.subID.toLong}").id
              |
              |(Author.all().where(.id == doc_id) { id }).toArray()
           """.stripMargin
        )
        // This should only contain the doc that matches
        res2 shouldEqual Value.Array(
          Value.Struct("id" -> Value.ID(doc.id.subID.toLong)))
      }

      "can deref documents when reading fields" in {
        val doc = evalOk(auth, s"""Author.create({ foo: "bar" })""").to[Value.Doc]

        val res = evalOk(
          auth,
          s"""|let doc = Author.byId("${doc.id.subID.toLong}")!
              |doc.foo""".stripMargin)
        res shouldEqual Value.Str("bar")
      }

      "can select fields with []" in {
        val doc = evalOk(auth, s"""Author.create({ foo: "bar" })""").to[Value.Doc]

        val res1 =
          evalOk(auth, s"""Author.byId("${doc.id.subID.toLong}")["foo"]""")
        res1 shouldEqual Value.Str("bar")

        val res2 =
          evalOk(auth, s"""Author.byId("${doc.id.subID.toLong}")["id"]""")
        res2 shouldEqual Value.ID(doc.id.subID.toLong)

        val res3 =
          evalOk(auth, s"""Author.byId("${doc.id.subID.toLong}")["baz"]""")
        res3 shouldEqual Value.Null(Span.Null)
      }

      "don't elide nulls on arrays" in {
        val doc =
          evalOk(auth, s"""Author.create({ foo: [1, null, 2] })""").to[Value.Doc]

        val res = evalOk(
          auth,
          s"""|let doc = Author.byId("${doc.id.subID.toLong}")!
              |doc.foo""".stripMargin)

        res shouldEqual Value.Array(
          Value.Int(1),
          Value.Null(Span.Null),
          Value.Int(2))
      }

      "null doc is a doc value" in {
        evalOk(auth, s"Author.byId('1234')") shouldBe Value.Doc(
          DocID(SubID(1234), CollectionID(1024)))
      }

      "null schema doc is a doc value" in {
        evalOk(auth, s"Collection.byName('Ghost')") shouldBe Value.Doc(
          CollectionID(-1).toDocID,
          Some("Ghost"))
      }

      "null allow ref field access" in {
        val res1 = evalOk(auth, "Author.byId('123').id")
        res1 shouldEqual Value.ID(123)
        val res2 = evalOk(auth, "Author.byId('123').coll")
        res2 shouldEqual evalOk(auth, "Author")
      }

      "null docs error on .field access" in {
        val err1 = evalErr(auth, "Author.byId('123').foo", typecheck = false)
        err1 shouldEqual QueryRuntimeFailure.DocumentNotFound(
          "Author",
          // collectionID is not used in error gen
          Value.Doc(DocID(SubID(123), CollectionID(0))),
          FQLInterpreter.StackTrace(Seq(Span(11, 18, Src.Query(""))))
        )

        val err2 = evalErr(auth, "Author.byId('123')!.foo")
        err2 shouldEqual QueryRuntimeFailure.DocumentNotFound(
          "Author",
          // collectionID is not used in error gen
          Value.Doc(DocID(SubID(123), CollectionID(0))),
          FQLInterpreter.StackTrace(Seq(Span(11, 19, Src.Query(""))))
        )
      }

      "null docs short-circuit on ?.field access" in {
        val res = evalOk(auth, "Author.byId('123')?.foo")
        res shouldEqual Value.Null(Span.Null)
      }

      "null docs error on ['field'] access" in {
        val err = evalErr(auth, "Author.byId('123')['foo']")
        err shouldEqual QueryRuntimeFailure.DocumentNotFound(
          "Author",
          // collectionID is not used in error gen
          Value.Doc(DocID(SubID(123), CollectionID(0))),
          FQLInterpreter.StackTrace(Seq(Span(11, 18, Src.Query(""))))
        )
      }

      "null docs short-circuit on ?.['field'] access" in {
        val res = evalOk(auth, "Author.byId('123')?.['foo']")
        res shouldEqual Value.Null(Span.Null)
      }

      "at operator" - {
        "works" in {
          val id = evalOk(auth, "Author.create({ name: 'Samuel Clemens' })")
            .to[Value.Doc]
            .id
            .subID
            .toLong
          val ts = evalOk(auth, s"Author.byId('$id')['ts']").to[Value.Time].value
          evalOk(auth, s"Author.byId('$id')!.update({ name: 'Mark Twain' })")
          evalOk(auth, s"Author.byId('$id')['name']") shouldBe (Value.Str(
            "Mark Twain"))
          evalOk(
            auth,
            s"at (Time('$ts')) { Author.byId('$id')['name'] }") shouldBe (Value
            .Str("Samuel Clemens"))
        }

        "nests and can return to snapshot time" in {
          val id = evalOk(auth, "Author.create({ name: 'Samuel Clemens' })")
            .to[Value.Doc]
            .id
            .subID
            .toLong
          val ts = evalOk(auth, s"Author.byId('$id')['ts']").to[Value.Time].value

          // The 'at' field is the time at which one should read the other document.
          val refid =
            evalOk(auth, s"Author.create({ at: '$ts' })")
              .to[Value.Doc]
              .id
              .subID
              .toLong
          val refts =
            evalOk(auth, s"Author.byId('$refid')['ts']").to[Value.Time].value

          evalOk(auth, s"Author.byId('$id')!.update({ name: 'Mark Twain' })")
          val tsNew = evalOk(auth, s"Author.byId('$id')['ts']").to[Value.Time].value
          evalOk(auth, s"Author.byId('$refid')!.update({ at: '$tsNew' })")

          evalOk(
            auth,
            s"""|at (Time('$refts')) {
                |  at (Time(Author.byId('$refid')['at'])) {
                |    Author.byId('$id')['name']
                |  }
                |}
                |""".stripMargin
          ) shouldBe (Value.Str("Samuel Clemens"))

          evalOk(
            auth,
            s"""|at (Time('$refts')) {
                |  at (TransactionTime()) {
                |    Author.byId('$id')['name']
                |  }
                |}
                |""".stripMargin
          ) shouldBe (Value.Str("Mark Twain"))
        }

        "accepts Value.TransactionTime" in {
          evalOk(
            auth,
            s"""|let mt = Author.create({ name: 'Mark Twain' })
                |at (mt.ts) {
                |  Author.byId(mt.id)['name']
                |}
                |""".stripMargin
          ) shouldBe (Value.Str("Mark Twain"))
        }

        "rejects bad types" in {
          evalErr(auth, """at (0) 0""") shouldBe QueryCheckFailure(
            Seq(
              TypeError(
                "Type `Int` is not a subtype of `TransactionTime | Time`",
                Span(4, 5, Src.Query("")))))

          evalErr(
            auth,
            """at (0) 0""",
            typecheck = false) shouldBe (QueryRuntimeFailure
            .InvalidType(
              ValueType.TimeType,
              Value.Number(0),
              FQLInterpreter.StackTrace(Seq(Span(4, 5, Src.Query(""))))))
        }

        "returns a sensible error for future reads and MVT violations" in {
          val idF = evalOk(auth, "Author.create({ name: 'ShakespeareGPT' })")
            .to[Value.Doc]
            .id
            .subID
            .toLong
          evalErr(
            auth,
            s"at (Time('2050-01-01T00:00:00.000Z')) Author.byId('$idF')") shouldBe (QueryRuntimeFailure
            .InvalidArgument(
              "at_time",
              "cannot evaluate `at` in the future",
              FQLInterpreter.StackTrace(Seq(Span(4, 36, Src.Query(""))))))

          val idP = evalOk(auth, "Author.create({ name: 'Sophocles' })")
            .to[Value.Doc]
            .id
            .subID
            .toLong
          // The default RepoContext uses the default MVTProvider,
          // so this doesn't fail properly (yet).
          pendingUntilFixed {
            // TODO: Check that there's a good error.
            evalErr(
              auth,
              s"at (Time('2000-01-01T00:00:00.000Z')) Author.byId('$idP')")
          }
        }
      }
    }
  }

  "ts things" - {
    val auth = newAuth
    mkColl(auth, "Author")

    "ts is not equal to TransactionTime companion" in {
      evalOk(
        auth,
        s"""Author.create({}).ts != TransactionTime""") shouldBe Value.True
    }
    "ts is equal to TransactionTime()" in {
      evalOk(
        auth,
        s"""Author.create({}).ts == TransactionTime()""") shouldBe Value.True
    }
  }

  "cannot write in at()" in {
    val auth = newAuth
    mkColl(auth, "Author")

    evalErr(
      auth,
      "at (Time.now().subtract(1, 'day')) { Author.create({}) }") should matchPattern {
      case QueryRuntimeFailure.Simple(
            "invalid_write",
            "Cannot write at a snapshot time in the past.",
            _,
            _) =>
    }

    // at(TransactionTime()) is allowed
    evalOk(auth, "at (TransactionTime()) { Author.create({}) }")
  }

  "transaction time" in {
    val auth = newAuth
    mkColl(auth, "User")

    val res0 = evalRes(auth, "User.create({ a: TransactionTime() })")
    (ctx ! Store.getUnmigrated(auth.scopeID, res0.value.as[DocID])).get.data.fields
      .get(List("data", "a")) shouldBe Some(TimeV(res0.ts))

    val res1 = evalRes(auth, "User.create({ nested: { foo: TransactionTime() } })")
    (ctx ! Store.getUnmigrated(auth.scopeID, res1.value.as[DocID])).get.data.fields
      .get(List("data", "nested", "foo")) shouldBe Some(TimeV(res1.ts))

    val res2 = evalRes(
      auth,
      "User.create({ array: [{ foo: TransactionTime() }, TransactionTime()] })")
    val arr =
      (ctx ! Store.getUnmigrated(auth.scopeID, res2.value.as[DocID])).get.data.fields
        .get(List("data", "array"))
        .get
        .asInstanceOf[ArrayV]

    arr.elems(0).asInstanceOf[MapV].get(List("foo")) shouldBe Some(TimeV(res2.ts))
    arr.elems(1) shouldBe TimeV(res2.ts)
  }

  "create with id works" in {
    val auth = newDB
    mkColl(auth, "User")

    evalOk(auth, "User.create({ id: 1 })")
    evalOk(auth, "User.create({ id: '2' })")
    evalOk(auth, "User.create({ id: ID('3') })")

    evalOk(auth, "User.all().toArray().map(.id)") shouldBe Value.Array(
      Value.ID(1),
      Value.ID(2),
      Value.ID(3))
  }
}
