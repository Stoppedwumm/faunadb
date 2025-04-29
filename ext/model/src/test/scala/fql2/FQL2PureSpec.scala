package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.Effect
import fauna.repo.values.Value
import org.scalactic.source.Position

class FQL2PureSpec extends FQL2Spec {

  var auth: Auth = _

  // All these `Value.Doc`s are used to make sure functions on docs count as reads.
  var namedDoc: Value.Doc = _
  var idDoc: Value.Doc = _
  var nonExistantNamedDoc: Value.Doc = _
  var nonExistantIdDoc: Value.Doc = _

  before {
    auth = newDB.withPermissions(AdminPermissions)

    nonExistantNamedDoc =
      evalOk(auth, "Collection.byName('Bar')").asInstanceOf[Value.Doc]
    namedDoc =
      evalOk(auth, "Collection.create({ name: 'Foo' })").asInstanceOf[Value.Doc]

    nonExistantIdDoc = evalOk(auth, "Foo.byId(555)").asInstanceOf[Value.Doc]
    idDoc =
      evalOk(auth, "Foo.create({ id: 1234, foo: 'bar' })").asInstanceOf[Value.Doc]

    evalOk(auth, "Foo.create({ id: 1235, foo: 'bar' })")
  }

  def checkAllowed(query: String)(implicit pos: Position) =
    evalOk(auth, query, effect = Effect.Pure)

  def checkDisallowed(query: String, func: String, typecheck: Boolean = true)(
    implicit pos: Position) = {
    for {
      doc <- Seq(nonExistantNamedDoc, namedDoc, nonExistantIdDoc, idDoc)
    } {
      val err = evalErr(
        auth,
        // FIXME: Typechecking doesn't know to lookup the type of `Foo` if we
        // pass in a `Value.Doc` referring to the `Foo` collection.
        query = s"Foo; $query",
        typecheck = typecheck,
        globalCtx = Map("doc" -> doc),
        effect = Effect.Pure
      )
      err.code shouldBe "invalid_effect"
      err.failureMessage shouldBe s"$func performs a read, which is not allowed in model tests."
    }
  }

  // This is an example of the full error message you would get from a default field.
  // This code was written before default fields, so this example uses the "model
  // tests" reason.
  //
  // All the other tests produce an error like this, its just less verbose to wrap it
  // up in some helpers.
  "error example" in {
    val err = evalErr(auth, "Collection.all()", effect = Effect.Pure)
    err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
      """|error: `all` performs a read, which is not allowed in model tests.
         |at *query*:1:15
         |  |
         |1 | Collection.all()
         |  |               ^^
         |  |""".stripMargin
    )
  }

  // This is a real example of the error format, just for a read-only context.
  "writes within set functions make nice errors" in {
    val err = evalErr(auth, "[1].toSet().map(_ => Foo.create({})).toArray()")
    val expected =
      """|error: `create` performs a write, which is not allowed in set functions.
         |at *query*:1:32
         |  |
         |1 | [1].toSet().map(_ => Foo.create({})).toArray()
         |  |                                ^^^^
         |  |
         |  |
         |1 | [1].toSet().map(_ => Foo.create({})).toArray()
         |  |                ^^^^^^^^^^^^^^^^^^^^^
         |  |
         |  |
         |1 | [1].toSet().map(_ => Foo.create({})).toArray()
         |  |                                             ^^
         |  |""".stripMargin

    err.errors.head.renderWithSource(Map.empty) shouldBe expected
  }

  "docs" - {

    // This looks weird but `name` and `id` are ref fields so we don't actually
    // read anything here.
    //
    // FIXME: This should be allowed, but is disallowed so that deleting
    // and re-creating a collection doesn't change the behavior of an indexed
    // computed field.
    "byName fields" in pendingUntilFixed {
      checkAllowed("Collection.byName('Foo').name")
    }
    "byId fields" in pendingUntilFixed {
      checkAllowed("Foo.byId(1234).id")
    }

    // FIXME: This tests the current behavior, but should be removed once we
    // fix the computed field indexing issue (see above).
    "byId should read" in {
      checkDisallowed("Collection.byName('Foo')", "`byName`")
      checkDisallowed("Foo.byId(1234)", "`byId`")
    }

    "bang operator" in {
      checkDisallowed("doc!", "`!`")
    }

    "exists" in {
      checkDisallowed("doc.exists()", "`exists`")
    }

    "field access" in {
      checkDisallowed("doc!.data", "`!`")
      checkDisallowed("doc?.data", "Reading a field")
      checkDisallowed("doc.data", "Reading a field", typecheck = false)
    }

    "projection" in {
      checkDisallowed("doc { data }", "Projection")
    }

    "??" in {
      checkDisallowed("doc ?? 'hi'", "`??`")
    }

    "toString" in {
      // This checks `ReadBroker.getAllFields`.
      val res = evalOk(
        auth,
        "Foo; Object.toString(doc)",
        globalCtx = Map("doc" -> idDoc),
        effect = Effect.Pure
      )

      // This is a really stupid output, but at least its consistent :P
      res shouldBe Value.Str(
        "[error: Reading a document performs a read, which is not allowed in model tests.]")
    }
  }

  "collections" - {
    "all" in {
      checkDisallowed("Collection.all()", "`all`")
      checkDisallowed("Foo.all()", "`all`")
    }

    "all overloaded with range" in {
      checkDisallowed("Collection.all({ from: doc })", "`all`")
      checkDisallowed("Foo.all({ from: doc })", "`all`")
    }

    "where" in {
      checkDisallowed("Collection.where(.name == 'Foo')", "`where`")
      checkDisallowed("Foo.where(.foo == 'bar')", "`where`")
    }

    "firstWhere" in {
      checkDisallowed("Collection.firstWhere(.name == 'Foo')", "`firstWhere`")
      checkDisallowed("Foo.firstWhere(.foo == 'bar')", "`firstWhere`")
    }

    // This is the only other function left on collections, might as well check it.
    "toString" in {
      checkAllowed("Collection.toString()")
      checkAllowed("Foo.toString()")
    }
  }

  "sets" - {
    "pure sets are allowed" in {
      checkAllowed("[].toSet()")
      checkAllowed("[].toSet().count()")

      checkAllowed("[1, 2].toSet()")
      checkAllowed("[1, 2].toSet().count()")

      val cursor =
        checkAllowed("[1, 2].toSet().paginate(1).after.toString()").as[String]
      checkAllowed(s"Set.paginate('$cursor')")
    }

    "Set.paginate()" in {
      val set = evalOk(auth, "Foo.all().paginate(1).after.toString()").as[String]
      // `Set.paginate` is fine, but that calls `Foo.all()` which is not fine. Not an
      // ideal error, but it works well enough.
      checkDisallowed(s"Set.paginate('$set')", "`all`")
    }
  }

  "tokens" in {
    checkDisallowed("Token.byDocument(doc)", "`byDocument`")
  }

  "credentials" in {
    val cred = evalOk(
      auth,
      """|Credential.create({
         |  document: Foo.byId('1234'),
         |  password: 'hi'
         |})""".stripMargin)
    val err = evalErr(
      auth,
      "cred.verify('hi')",
      globalCtx = Map("cred" -> cred),
      effect = Effect.Pure)

    // Not ideal, because we can't test that the `verify` function checks for reads,
    // but at least it returns an error.
    err.code shouldBe "invalid_effect"
    err.failureMessage shouldBe "Reading a field performs a read, which is not allowed in model tests."
  }

  "databases" in {
    checkDisallowed("Database.all()", "`all`")
  }

  "writes" in {
    val err =
      evalErr(auth, "Collection.create({ name: 'zzz' })", effect = Effect.Pure)
    err.code shouldBe "invalid_effect"
    err.failureMessage shouldBe "`create` performs a write, which is not allowed in model tests."

    // This error kinda sucks but ah well. Ideally it'd say "`update` performs a
    // write".
    checkDisallowed("doc.update({})", "Reading a field")
    checkDisallowed("doc.replace({})", "Reading a field")
    checkDisallowed("doc.delete()", "Reading a field")
  }

  "Time.now()" in {
    val err =
      evalErr(auth, "Time.now()", effect = Effect.Pure)
    err.code shouldBe "invalid_effect"
    err.failureMessage shouldBe "`now` performs an observation, which is not allowed in model tests."
  }

  "newId()" in {
    val err =
      evalErr(auth, "newId()", effect = Effect.Pure)
    err.code shouldBe "invalid_effect"
    err.failureMessage shouldBe "`newId` performs an observation, which is not allowed in model tests."
  }
}
