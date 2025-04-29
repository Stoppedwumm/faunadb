package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.fql2.QueryCheckFailure
import fauna.repo.values.Value
import scala.concurrent.duration._

class FQL2ComputedFieldsSpec extends FQL2Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "computed fields" - {
    "definition" - {
      "can be declared" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => doc.baz"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(auth, "Foo.definition.computed_fields.bar!.body")
          .as[String] shouldEqual "doc => doc.baz"
      }

      "can be removed" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: { body: "doc => doc.baz" }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(auth, "Foo.definition.computed_fields == null")
          .as[Boolean] shouldEqual false

        evalOk(
          auth,
          """|Collection.byName('Foo')!.replace({
             |  name: 'Foo'
             |})
             |""".stripMargin
        )

        evalOk(auth, "Foo.definition.computed_fields == null")
          .as[Boolean] shouldEqual true
      }

      "reject invalid field name" in {
        val err = evalErr(
          auth,
          """|Collection.create({
               |  name: 'Foo',
               |  computed_fields: {
               |    '1234': {
               |      body: "x => x"
               |    }
               |  }
               |})
               |""".stripMargin
        )

        err.code shouldBe "constraint_failure"
        err.errors.head.message should include("Invalid identifier")
      }

      "reject reserved field name" in {
        val err0 = evalErr(
          auth,
          """|Collection.create({
               |  name: 'Foo',
               |  computed_fields: {
               |    update: {
               |      body: "x => x"
               |    }
               |  }
               |})
               |""".stripMargin
        )

        val err1 = evalErr(
          auth,
          """|Collection.create({
               |  name: 'Foo',
               |  computed_fields: {
               |    ttl: {
               |      body: "x => x"
               |    }
               |  }
               |})
               |""".stripMargin
        )

        val errs = Seq(err0, err1)
        all(errs map { _.code }) shouldBe "constraint_failure"
        all(errs map { _.errors.head.message }) should
          include regex "The name '\\w+' is reserved"
      }

      "reject names duplicating defined fields" in {
        val err = evalErr(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    a: { signature: "Number" }
             |  },
             |  computed_fields: {
             |    a: { body: " _ => 0" }
             |  }
             |})""".stripMargin
        )
        err.errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Failed to create Collection.
             |constraint failures:
             |  computed_fields: Computed field 'a' collides with a defined field
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: "Foo",
             |3 | |   fields: {
             |4 | |     a: { signature: "Number" }
             |5 | |   },
             |6 | |   computed_fields: {
             |7 | |     a: { body: " _ => 0" }
             |8 | |   }
             |9 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "reject invalid function arity" in {
        val err = evalErr(
          auth,
          """|Collection.create({
               |  name: 'Foo',
               |  computed_fields: {
               |    bar: {
               |      body: "() => 42"
               |    }
               |  }
               |})
               |""".stripMargin
        )

        err.code shouldBe "constraint_failure"
        err.errors.head.message should include(
          "Expected exactly 1 argument, but the function was defined with 0 arguments")
      }

      "can have a type signature" in {
        evalOk(
          auth,
          """|Collection.create({
               |  name: 'Foo',
               |  computed_fields: {
               |    bar: {
               |      body: "doc => doc.baz",
               |      signature: "String"
               |    }
               |  }
               |})
               |""".stripMargin
        )

        evalOk(auth, "Foo.definition.computed_fields.bar!.signature")
          .as[String] shouldEqual "String"
      }

      "rejects invalid signature" in {
        val err = evalErr(
          auth,
          """|Collection.create({
               |  name: 'Foo',
               |  computed_fields: {
               |    bar: {
               |      body: "doc => doc.baz",
               |      signature: "^.^"
               |    }
               |  }
               |})
               |""".stripMargin
        )

        err.code shouldBe "constraint_failure"
        err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
          """|error: Failed to create Collection.
             |constraint failures:
             |  computed_fields.bar.signature: Failed parsing user-provided signature.
             |      error: Expected type
             |      at *signature*:1:1
             |        |
             |      1 | ^.^
             |        | ^
             |        |
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: 'Foo',
             |3 | |   computed_fields: {
             |4 | |     bar: {
             |5 | |       body: "doc => doc.baz",
             |6 | |       signature: "^.^"
             |7 | |     }
             |8 | |   }
             |9 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "rejects signature with generics" in {
        val err = evalErr(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => doc.baz",
             |      signature: "A => A"
             |    }
             |  }
             |})""".stripMargin
        )

        err.code shouldBe "invalid_schema"
        err.errors.head.message should be(
          """|Invalid database schema update.
             |    error: Unknown type `A`
             |    at main.fsl:5:16
             |      |
             |    5 |   compute bar: A => A = (doc) => doc.baz
             |      |                ^
             |      |
             |    error: Unknown type `A`
             |    at main.fsl:5:21
             |      |
             |    5 |   compute bar: A => A = (doc) => doc.baz
             |      |                     ^
             |      |
             |    error: Type `Foo` does not have field `baz`
             |    at main.fsl:5:38
             |      |
             |    5 |   compute bar: A => A = (doc) => doc.baz
             |      |                                      ^^^
             |      |""".stripMargin
        )
      }

      "should infer computed field types" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "x => 1",
             |    }
             |  }
             |})
             |Function.create({
             |  name: 'Bar',
             |  body: "() => Foo.byId('0')!.bar"
             |})
             |""".stripMargin
        )

        // Inferred type of Bar should include the inferred type of the computed
        // field.
        evalRes(auth, "Bar").typeStr shouldBe "UserFunction<() => 1>"
      }

      "inferred type should be persisted and show up in queries" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    foo: {
             |      body: "x => 1",
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalRes(auth, "Foo.byId('0')").typeStr shouldBe "Ref<Foo>"
        evalRes(auth, "Foo.byId('0')?.foo").typeStr shouldBe "1 | Null"
        evalRes(
          auth,
          "Foo.byId('0') { foo, bar }").typeStr shouldBe "{ foo: 1, bar: Any } | Null"
      }

      "should infer more complex computed field types" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    foo: { body: "x => 2" },
             |    bar: { body: "x => x.foo + 3" },
             |    baz: { body: "x => x.bar + 4" },
             |  }
             |})
             |Function.create({
             |  name: 'Bar',
             |  body: "() => Foo.byId('0')!.baz"
             |})
             |""".stripMargin
        )

        evalRes(auth, "Bar").typeStr shouldBe "UserFunction<() => Number>"
      }

      "index types use computed field types" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => 3"
             |    }
             |  },
             |  indexes: {
             |    byBar: {
             |      terms: [{ field: ".bar" }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalRes(auth, "Foo.byBar").typeStr should be(
          "((term1: 3) => Set<Foo>) & ((term1: 3, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Foo>)"
        )

        val err = evalErr(auth, "Foo.byBar('hi')")
        err.isInstanceOf[QueryCheckFailure] shouldBe true
        err.errors.size shouldEqual 1
        err.errors.head.message shouldEqual """`"hi"` is not the value `3`"""
      }

      "index types work for nested fields" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => { a: 4 }"
             |    }
             |  },
             |  indexes: {
             |    byBar: {
             |      terms: [{ field: ".bar" }, { field: ".bar.a" }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalRes(auth, "Foo.byBar").typeStr should be(
          "((term1: { a: 4 }, term2: 4) => Set<Foo>) & ((term1: { a: 4 }, term2: 4, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Foo>)"
        )
      }

      "index types use signature over inferred type" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => { a: 4 }",
             |      signature: "{ a: Number }"
             |    }
             |  },
             |  indexes: {
             |    byBar: {
             |      terms: [{ field: ".bar.a" }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalRes(auth, "Foo.byBar").typeStr should be(
          "((term1: Number) => Set<Foo>) & ((term1: Number, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Foo>)"
        )
      }

      "index types fallback to Any for unknown fields" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  indexes: {
             |    byBar: {
             |      terms: [{ field: ".a.b.c" }]
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalRes(auth, "Foo.byBar").typeStr should be(
          "((term1: Any) => Set<Foo>) & ((term1: Any, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Foo>)"
        )
      }

      "checks computed field signatures" in {
        evalOk(auth, "Collection.create({ name: 'Foo' })")

        def checkComputedFieldOk(body: String, signature: String) = {
          evalOk(
            auth,
            s"""|Collection.byName('Foo')!.update({
                |  computed_fields: {
                |    bar: {
                |      body: "$body",
                |      signature: "$signature",
                |    }
                |  }
                |})
                |""".stripMargin
          )
        }
        def checkComputedFieldErr(body: String, signature: String) = {
          evalErr(
            auth,
            s"""|Collection.byName('Foo')!.update({
                |  computed_fields: {
                |    bar: {
                |      body: "$body",
                |      signature: "$signature",
                |    }
                |  }
                |})
                |""".stripMargin
          )
        }

        // The `doc` is `{ *: Any }`.
        checkComputedFieldOk("doc => doc.foo", "Number")
        checkComputedFieldOk("doc => doc.foo * 2", "Number")
        val err0 = checkComputedFieldErr("doc => doc * 2", "Number")
        err0.code shouldBe "invalid_schema"
        // FIXME: This should underline the `doc` value (not the entire
        // function), and should show the type `Foo` (not the alias).
        err0.errors.head.message shouldBe (
          """|Invalid database schema update.
             |    error: Type `{ id: ID, ts: Time, ttl: Time | Null, bar: Number, *: Any }` is not a subtype of `Number`
             |    at main.fsl:5:25
             |      |
             |    5 |   compute bar: Number = (doc) => doc * 2
             |      |                         ^^^^^^^^^^^^^^^^
             |      |""".stripMargin
        )

        checkComputedFieldOk("doc => 'foo'", "String")
        val err1 = checkComputedFieldErr("doc => 'foo'", "Number")
        err1.code shouldBe "invalid_schema"
        err1.errors.head.message shouldBe (
          """|Invalid database schema update.
             |    error: Type `String` is not a subtype of `Number`
             |    at main.fsl:5:34
             |      |
             |    5 |   compute bar: Number = (doc) => "foo"
             |      |                                  ^^^^^
             |      |""".stripMargin
        )
      }
    }

    "definition with short lambda" - {
      "works" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "(.baz)"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        evalOk(auth, "Foo.create({ baz: 3 }).bar").as[Int] shouldBe 3
      }

      "requires parenthesis" in {
        val err = evalErr(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: ".baz"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        err.code shouldBe "constraint_failure"
        err.errors.head.renderWithSource(Map.empty) shouldEqual
          """|error: Failed to create Collection.
             |constraint failures:
             |  computed_fields.bar.body: Unable to parse FQL source code.
             |      error: Invalid anonymous field access
             |      at *body*:1:1
             |        |
             |      1 | .baz
             |        | ^^^^
             |        |
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: 'Foo',
             |3 | |   computed_fields: {
             |4 | |     bar: {
             |5 | |       body: ".baz"
             |6 | |     }
             |7 | |   }
             |8 | | })
             |  | |__^
             |  |""".stripMargin
      }
    }

    "projection" - {
      "can project pure computed fields" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "_ => 42"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val value =
          evalOk(
            auth,
            """|let foo = Foo.create({})
               |foo.bar
               |""".stripMargin
          )

        value.to[Value.Int].value shouldBe 42
      }

      "can project on doc fields" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => doc.baz"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val value =
          evalOk(
            auth,
            """|let foo = Foo.create({ baz: 42 })
               |foo.bar
               |""".stripMargin
          )

        value.to[Value.Int].value shouldBe 42
      }

      "computed fields can read" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "doc => doc.baz"
             |    }
             |  }
             |})
             |
             |Collection.create({
             |  name: 'Fizz'
             |})
             |""".stripMargin
        )

        val value =
          evalOk(
            auth,
            """|let fizz = Fizz.create({ buzz: 42 })
               |let foo = Foo.create({ baz: fizz })
               |foo.bar.buzz
               |""".stripMargin
          )

        value.to[Value.Int].value shouldBe 42
      }

      "materialization returns computed fields" in {
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: 'Foo',
              |  computed_fields: {
              |    bar: {
              |      body: '_ => 42'
              |    }
              |  }
              |})
              |""".stripMargin
        ).to[Value.Doc]

        // Note that computed fields take precedence.
        val doc =
          evalOk(
            auth,
            """|Foo.createData({
               |  bar: "I'm in data",
               |  baz: "Something else"
               |})
               |""".stripMargin
          ).to[Value.Doc]

        val fields = getDocFields(auth, doc).to[Value.Struct]
        (fields / "bar") shouldBe Value.Int(42)
        (fields / "baz") shouldBe Value.Str("Something else")

        val dataBar =
          evalOk(
            auth,
            """|Foo.all().first()!.data.bar
               |""".stripMargin
          ).to[Value.Str]

        dataBar shouldBe Value.Str("I'm in data")
      }

      "fail on writes" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "_ => Foo.create({})"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val err =
          evalErr(
            auth,
            """|let foo = Foo.create({})
               |foo.bar!
               |""".stripMargin
          )

        err.code shouldBe "invalid_effect"
        err.errors.head.renderWithSource(Map.empty) shouldEqual
          """|error: `create` performs a write, which is not allowed in computed fields.
             |at *computed_field:Foo:bar*:1:16
             |  |
             |1 | _ => Foo.create({})
             |  |                ^^^^
             |  |
             |at *query*:2:5
             |  |
             |2 | foo.bar!
             |  |     ^^^
             |  |""".stripMargin
      }

      "can fail at runtime" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "_ => 10 / 0"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val err =
          evalErr(
            auth,
            """|let foo = Foo.create({})
               |foo.bar!
               |""".stripMargin
          )

        err.code shouldBe "divide_by_zero"
        err.errors.head.renderWithSource(Map.empty) shouldEqual
          """|error: Attempted integer division by zero.
             |at *computed_field:Foo:bar*:1:11
             |  |
             |1 | _ => 10 / 0
             |  |           ^
             |  |
             |at *query*:2:5
             |  |
             |2 | foo.bar!
             |  |     ^^^
             |  |""".stripMargin
      }

      "recursive projection fails cleanly" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: { body: "doc => doc.bar" }
             |  }
             |})
             |""".stripMargin
        )

        val err = evalErr(auth, "Foo.create({}).bar")
        err.code shouldBe "stack_overflow"
        err.failureMessage should include("number of stack frames exceeded limit")
      }
    }

    "writing" - {
      "cannot be written to except under data" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: 'Foo',
             |  computed_fields: {
             |    bar: {
             |      body: "_ => 0"
             |    },
             |    baaz: {
             |      body: "_ => 1"
             |    }
             |  }
             |})
             |""".stripMargin
        )

        // Create.
        val cerr = evalErr(auth, "Foo.create({ bar: 1 })")
        cerr.code shouldBe "constraint_failure"
        cerr.errors.size shouldEqual 1
        cerr.errors.head.renderWithSource(Map.empty) shouldEqual
          """|error: Failed to create document in collection `Foo`.
             |constraint failures:
             |  bar: Cannot write to a computed field
             |at *query*:1:11
             |  |
             |1 | Foo.create({ bar: 1 })
             |  |           ^^^^^^^^^^^^
             |  |""".stripMargin

        evalOk(auth, "Foo.createData({ bar: 1 })")
        evalOk(auth, "Foo.all().first()!.bar") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.all().first()!.data.bar") shouldEqual Value.Int(1)

        // Update.
        val id = evalOk(auth, "Foo.all().first()!.id") match {
          case Value.ID(id) => id
          case _            => fail("id was not an id?")
        }
        val doc = s"Foo.byId('$id')!"
        val uerr = evalErr(auth, s"$doc.update({ bar: 2 })")
        uerr.code shouldBe "constraint_failure"
        uerr.errors.size shouldEqual 1
        uerr.errors.head.renderWithSource(Map.empty) shouldEqual
          s"""|error: Failed to update document with id $id in collection `Foo`.
              |constraint failures:
              |  bar: Cannot write to a computed field
              |at *query*:1:39
              |  |
              |1 | $doc.update({ bar: 2 })
              |  |                                       ^^^^^^^^^^^^
              |  |""".stripMargin

        evalOk(auth, s"$doc.updateData({ bar: 2 })")
        evalOk(auth, s"$doc.bar") shouldEqual Value.Int(0)
        evalOk(auth, s"$doc.data.bar") shouldEqual Value.Int(2)

        // Replace.
        val rerr = evalErr(auth, s"$doc.replace({ bar: 3 })")
        rerr.code shouldBe "constraint_failure"
        rerr.errors.size shouldEqual 1
        rerr.errors.head.renderWithSource(Map.empty) shouldEqual
          s"""|error: Failed to update document with id $id in collection `Foo`.
              |constraint failures:
              |  bar: Cannot write to a computed field
              |at *query*:1:40
              |  |
              |1 | $doc.replace({ bar: 3 })
              |  |                                        ^^^^^^^^^^^^
              |  |""".stripMargin

        evalOk(auth, s"$doc.replaceData({ bar: 3 })")
        evalOk(auth, s"$doc.bar") shouldEqual Value.Int(0)
        evalOk(auth, s"$doc.data.bar") shouldEqual Value.Int(3)

        // Multiple violations.
        val merr = evalErr(auth, "Foo.create({ bar: 1, baaz: 0 })")
        merr.code shouldBe "constraint_failure"
        merr.errors.size shouldEqual 1
        merr.errors.head.renderWithSource(Map.empty) shouldEqual
          """|error: Failed to create document in collection `Foo`.
             |constraint failures:
             |  bar: Cannot write to a computed field
             |  baaz: Cannot write to a computed field
             |at *query*:1:11
             |  |
             |1 | Foo.create({ bar: 1, baaz: 0 })
             |  |           ^^^^^^^^^^^^^^^^^^^^^
             |  |""".stripMargin
      }
    }

    "indexing" - {
      "works as terms and values" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    b: {
             |      body: "doc => 2 * doc.a"
             |    },
             |    c: {
             |      body: "doc => 1 + doc.d"
             |    }
             |  },
             |  indexes: {
             |    byAB: {
             |      terms: [{ field: ".a" }, { field: ".b" }],
             |      values: [{ field: ".c", order: "desc" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(
          auth,
          """|Foo.create({ a: 0, d: 1 })
             |Foo.create({ a: 1, d: 1 })
             |Foo.create({ a: 2, d: 0 })
             |Foo.create({ a: 2, d: 1 })""".stripMargin
        )

        def actual(a: Int, b: Int) =
          evalOk(auth, s"Foo.byAB($a, $b).map(x => x { a, b, c, d }).toArray()")
        def entry(a: Int, b: Int, c: Int, d: Int) = {
          val vi = Value.Int // Why, yes, I am this lazy.
          Value.Struct("a" -> vi(a), "b" -> vi(b), "c" -> vi(c), "d" -> vi(d))
        }
        actual(0, 0) shouldEqual Value.Array(entry(0, 0, 2, 1))
        actual(1, 2) shouldEqual Value.Array(entry(1, 2, 2, 1))
        // The order proves the index sees the values.
        actual(2, 4) shouldEqual Value.Array(entry(2, 4, 2, 1), entry(2, 4, 1, 0))
        actual(3, 6) shouldEqual Value.Array.empty
      }

      "works for members of computed objects and arrays" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "doc => { b : 1, c : 2 }"
             |    },
             |    b: {
             |      body: "doc => [1, 2]"
             |    }
             |  },
             |  indexes: {
             |    byADotB: {
             |      terms: [{ field: ".a.b" }]
             |    },
             |    byFirstInB: {
             |      terms: [{ field: ".b[0]" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})".stripMargin)

        evalOk(auth, s"Foo.byADotB(1).count()") shouldEqual Value.Int(1)
        evalOk(auth, s"Foo.byFirstInB(1).count()") shouldEqual Value.Int(1)
      }

      "works for members of computed objects and arrays (special field syntax)" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "doc => { 'b c': [1, 2] }"
             |    }
             |  },
             |  indexes: {
             |    byFirstInA: {
             |      terms: [{ field: '.["a"]["b c"][0]' }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})".stripMargin)

        evalOk(auth, s"Foo.byFirstInA(1).count()") shouldEqual Value.Int(1)
      }

      "plays nicely with MVA and non-MVA terms" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    c: {
             |      body: "doc => [doc.a, doc.b]",
             |      signature: "Any"
             |    }
             |  },
             |  indexes: {
             |    mva_on: {
             |      terms: [{ field: ".c", mva: true }]
             |    },
             |    mva_off: {
             |      terms: [{ field: ".c", mva: false }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(
          auth,
          """|Foo.create({ a: 0, b: 0 })
             |Foo.create({ a: 0, b: 1 })""".stripMargin
        )

        evalOk(auth, "Foo.mva_on(0).count()") shouldEqual Value.Int(2)
        evalOk(auth, "Foo.mva_on(1).count()") shouldEqual Value.Int(1)
        evalOk(auth, "Foo.mva_on([0, 0]).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.mva_on([0, 1]).count()") shouldEqual Value.Int(0)

        evalOk(auth, "Foo.mva_off(0).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.mva_off(1).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.mva_off([0, 0]).count()") shouldEqual Value.Int(1)
        evalOk(auth, "Foo.mva_off([0, 1]).count()") shouldEqual Value.Int(1)
      }

      "works for nested field paths with MVA" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "doc => { b : 1, c : [{ a: 1, b: [0, [1, 2]] }] }",
             |      signature: "Any"
             |    },
             |  },
             |  indexes: {
             |    byYikes: {
             |      terms: [{ field: ".a.c[0].b", mva: true }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})".stripMargin)

        evalOk(auth, s"Foo.byYikes(0).count()") shouldEqual Value.Int(1)
        evalOk(auth, s"Foo.byYikes(1).count()") shouldEqual Value.Int(1)
        evalOk(auth, s"Foo.byYikes(2).count()") shouldEqual Value.Int(1)
      }

      "typechecks computed fields for index signatures" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    c: {
             |      body: "doc => [doc.a, doc.b]"
             |    }
             |  },
             |  indexes: {
             |    mva_on: {
             |      terms: [{ field: ".c", mva: true }]
             |    },
             |    mva_off: {
             |      terms: [{ field: ".c", mva: false }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalRes(auth, "(x) => Foo.mva_on(x)").typeStr shouldBe "Any => Set<Foo>"
        evalRes(
          auth,
          "(x) => Foo.mva_off(x)").typeStr shouldBe "[Any, Any] => Set<Foo>"

        val err1 = evalErr(auth, "Foo.mva_off(0).count()")
        err1.isInstanceOf[QueryCheckFailure] shouldBe true
        err1.errors.size shouldEqual 1
        err1.errors.head.message shouldEqual """Type `Int` is not a subtype of `[Any, Any]`"""
        val err2 = evalErr(auth, "Foo.mva_off('hi').count()")
        err2.isInstanceOf[QueryCheckFailure] shouldBe true
        err2.errors.size shouldEqual 1
        err2.errors.head.message shouldEqual """Type `String` is not a subtype of `[Any, Any]`"""
        evalOk(auth, "Foo.mva_off([0, 0]).count()")
        evalOk(auth, "Foo.mva_off(['hi', 1]).count()")
      }

      "works as a unique constraint" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    c: {
             |      body: "doc => doc.a + doc.b"
             |    }
             |  },
             |  constraints: [
             |    { unique: [".c"] }
             |  ]
             |})""".stripMargin
        )

        evalOk(
          auth,
          """|Foo.create({ a: 0, b: 1 })
             |Foo.create({ a: 1, b: 1 })""".stripMargin
        )

        evalErr(
          auth,
          "Foo.create({ a: 1, b: 0 })").failureMessage shouldBe "Failed unique constraint."
        evalErr(
          auth,
          "Foo.create({ a: 0, b: 2 })").failureMessage shouldBe "Failed unique constraint."
      }

      "handles updates and deletes" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "doc => doc.x"
             |    },
             |    b: {
             |      body: "doc => doc.y"
             |    }
             |  },
             |  indexes: {
             |    byA: {
             |      terms: [{ field: ".a" }],
             |      values: [{ field: ".b" }]
             |    },
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ x: 0, y: 0 })")
        evalOk(auth, "Foo.byA(0).count()") shouldBe Value.Int(1)

        evalOk(auth, "Foo.byA(0).first()!.update({ x : 1 })")
        evalOk(auth, "Foo.byA(0).count()") shouldBe Value.Int(0)
        evalOk(auth, "Foo.byA(1).count()") shouldBe Value.Int(1)

        evalOk(auth, "Foo.byA(1).first()!.delete()")
        evalOk(auth, "Foo.byA(1).count()") shouldBe Value.Int(0)

        evalOk(
          auth,
          """|Foo.create({ x: 0, y: 0 })
             |Foo.create({ x: 0, y: 1 })""".stripMargin
        )
        evalOk(auth, "Foo.byA(0).map(x => x.b).toArray()") shouldBe Value.Array(
          Value.Int(0),
          Value.Int(1))
        evalOk(auth, "Foo.byA(0).first()!.update({ y: 2 })")
        evalOk(auth, "Foo.byA(0).map(x => x.b).toArray()") shouldBe Value.Array(
          Value.Int(1),
          Value.Int(2))
      }

      "plays nice with data field homonyms" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "_ => 1",
             |      signature: "Number"
             |    },
             |    b: {
             |      body: "doc => -1 * doc.data.b",
             |      signature: "Number"
             |    }
             |  },
             |  indexes: {
             |    byA: {
             |      terms: [{ field: ".a" }],
             |      values: [{ field: "data.b", order: "desc" }]
             |    },
             |    byDataA: {
             |      terms: [{ field: ".data.a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.createData({ a: 0, b: 0 })")

        evalOk(auth, "Foo.byA(0).count()") shouldBe Value.Int(0)
        evalOk(auth, "Foo.byA(1).count()") shouldBe Value.Int(1)
        evalOk(auth, "Foo.byDataA(0).count()") shouldBe Value.Int(1)
        evalOk(auth, "Foo.byDataA(1).count()") shouldBe Value.Int(0)

        evalOk(auth, "Foo.createData({ b: 1 })".stripMargin)

        // The order proves the index sees data.b, not the computed field b.
        evalOk(auth, "Foo.byA(1).map(x => x.data.b).toArray()") shouldBe Value
          .Array(Value.Int(1), Value.Int(0))
      }

      "doesn't confuse computed and persisted values in the cache" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "_ => 1",
             |    }
             |  },
             |  indexes: {
             |    ofA: {
             |      values: [{ field: "a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.createData({ a: 0 })")

        evalOk(auth, "Foo.ofA().first()!.data.a") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.ofA().first()!.a") shouldEqual Value.Int(1)
      }

      "doesn't cache null values" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "_ => Time.now()",
             |    }
             |  },
             |  indexes: {
             |    ofA: {
             |      values: [{ field: "a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ id: 0 })")

        // The covered index value is null because reads aren't allowed
        // in CF indexing, so the value should be recomputed here.
        evalOk(auth, "Foo.ofA().first()!.a") shouldBe a[Value.Time]
      }

      "invalidates cached indexed computed field values" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "doc => doc.x + 1",
             |    }
             |  },
             |  indexes: {
             |    ofA: {
             |      values: [{ field: "a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ id: 0, x: 0 })")

        evalOk(
          auth,
          """|let old = Foo.ofA().first()!.a
             |Foo.byId(0)!.update({ x: 1 }).a""".stripMargin
        ) shouldEqual Value.Int(2)
      }

      "doesn't hide evaluation errors with cached nulls" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "_ => 1/0",
             |    }
             |  },
             |  indexes: {
             |    ofA: {
             |      values: [{ field: "a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ id: 0 })")

        // The covered index value is null, which is not the value of the
        // computed field, so we shouldn't return null from the cache.
        evalErr(auth, "Foo.ofA().first()!.a").code shouldEqual "divide_by_zero"
      }

      "cannot call UDFs (typecheck)" - {
        // Note that, due to a bug in model tests, these queries will
        // still create the schema objects. New DB every test!
        "when the UDF is used in a term" in {
          val auth = newDB

          val err = evalErr(
            auth,
            """|Function.create({
               |  name: "one",
               |  body: "_ => 1"
               |})
               |
               |Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: {
               |      body: "_ => 1 + one(0)"
               |    }
               |  },
               |  indexes: {
               |     byA: {
               |      terms: [{ field: ".a" }]
               |    }
               |  }
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot call UDFs
               |    at main.fsl:5:26
               |      |
               |    5 |   compute a = (_) => 1 + one(0)
               |      |                          ^^^
               |      |""".stripMargin
        }

        "erors display correctly when using square bracket paths" in {
          val auth = newDB

          val err = evalErr(
            auth,
            """|Function.create({
               |  name: "one",
               |  body: "_ => { b: 0 }"
               |})
               |
               |Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: {
               |      body: "_ => one(0)"
               |    }
               |  },
               |  indexes: {
               |     byA: {
               |      terms: [{ field: ".['a']['b']" }]
               |    }
               |  }
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          // The name `a` should be pulled out even though the path is weird.
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot call UDFs
               |    at main.fsl:5:22
               |      |
               |    5 |   compute a = (_) => one(0)
               |      |                      ^^^
               |      |""".stripMargin
        }

        "when the UDF is used in a value" in {
          val auth = newDB

          // We'll catch you even if you don't call it directly.
          val err = evalErr(
            auth,
            """|Function.create({
               |  name: "one",
               |  body: "_ => 1"
               |})
               |
               |Collection.create({
               |  name: "FooValues",
               |  computed_fields: {
               |    a: {
               |      body: "_ => { let two = one; 1 + two(0) }"
               |    }
               |  },
               |  indexes: {
               |    byA: {
               |      terms: [{ field: ".x" }],
               |      values: [{ field: ".a" }]
               |    }
               |  }
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot call UDFs
               |    at main.fsl:6:15
               |      |
               |    6 |     let two = one
               |      |               ^^^
               |      |""".stripMargin
        }

        "when the UDF is used in a unique constraint" in {
          val auth = newDB

          val err = evalErr(
            auth,
            """|Function.create({
               |  name: "one",
               |  body: "_ => 1"
               |})
               |
               |Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: {
               |      body: "_ => 1 + one(0)"
               |    }
               |  },
               |  constraints: [
               |    { unique: [".a"] }
               |  ]
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot call UDFs
               |    at main.fsl:5:26
               |      |
               |    5 |   compute a = (_) => 1 + one(0)
               |      |                          ^^^
               |      |""".stripMargin
        }

        "on update" in {
          val auth = newDB

          evalOk(
            auth,
            """|Function.create({
               |  name: "one",
               |  body: "_ => 1"
               |})
               |
               |Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: {
               |      body: "_ => 1 + one(0)"
               |    }
               |  }
               |})""".stripMargin
          )

          evalErr(
            auth,
            """|Collection.byName('Foo')!.update({
               |  indexes: {
               |    byA: {
               |      terms: [{ field: ".a" }]
               |    }
               |  }
               |})""".stripMargin
          ).code shouldEqual "invalid_schema"
        }
      }

      "cannot call UDFs (run-time)" in {
        val auth = newDB(typeChecked = false)
        evalOk(
          auth,
          """|Function.create({
             |  name: "one",
             |  body: "_ => 1"
             |})
             |
             |Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    b: {
             |      body: "doc => if (doc.a) one(1) else 0"
             |    },
             |  },
             |  indexes: {
             |    byB: {
             |      terms: [{ field: ".b" }],
             |    },
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ a: true })")
        evalOk(auth, "Foo.create({ a: false })")
        evalOk(auth, "Foo.byB(0).count()") shouldBe Value.Int(1)
        evalOk(auth, "Foo.byB(1).count()") shouldBe Value.Int(0)
      }

      "cannot access collections (typecheck)" - {
        // Note that, due to a bug in model tests, these queries will
        // still create the schema objects. New DB every test!

        def checkDisallowed(name: String, field: String) = {
          name in {
            val auth = newDB

            val err = evalErr(
              auth,
              s"""|Collection.create({
                  |  name: "Bar",
                  |  indexes: {
                  |    byName: {
                  |      terms: [{ field: ".name" }]
                  |    }
                  |  }
                  |})
                  |
                  |Collection.create({
                  |  name: "Foo",
                  |  computed_fields: {
                  |    a: {
                  |      body: "_ => $field"
                  |    }
                  |  },
                  |  indexes: {
                  |     byA: {
                  |      terms: [{ field: ".a" }]
                  |    }
                  |  }
                  |})""".stripMargin
            )
            err.code shouldEqual "invalid_schema"
          }
        }

        checkDisallowed("create sets", "Bar.all()")
        checkDisallowed("access sets", "Bar.all().count()")
        checkDisallowed("create docs", "Bar.create({})")
        checkDisallowed("lookup definition", "Bar.definition")
        checkDisallowed("call where()", "Bar.where(.foo > 3)")
        checkDisallowed("call firstWhere()", "Bar.firstWhere(.foo > 3)")
        checkDisallowed("call index", "Bar.byName('foo')")

        // This should be allowed eventually, but for now we need to disallow it.
        checkDisallowed("call byId", "Bar.byId(3)")
      }

      "cannot access collections (runtime)" in {
        val auth = newDB(typeChecked = false)
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Bar",
             |})
             |
             |Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: {
             |      body: "_ => Bar.all().count()"
             |    }
             |  },
             |  indexes: {
             |     byA: {
             |      terms: [{ field: ".a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})")

        evalOk(auth, s"Foo.byA(0).count()") shouldEqual Value.Int(0)
      }

      "cannot use computed fields (typecheck)" - {
        "in terms" in {
          val auth = newDB

          val err = evalErr(
            auth,
            """|Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: { body: "_ => 1" },
               |    b: { body: "doc => doc.a" }
               |  },
               |  indexes: {
               |    byB: {
               |      terms: [{ field: ".b" }]
               |    }
               |  }
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot use computed fields
               |    at main.fsl:6:28
               |      |
               |    6 |   compute b = (doc) => doc.a
               |      |                            ^
               |      |""".stripMargin
        }

        "in values" in {
          val auth = newDB

          // Can't sneak around the restriction.
          val err = evalErr(
            auth,
            """|Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: { body: "_ => { x: 0, y: 1 }" },
               |    b: { body: "doc => { let f = doc; 1 + f.a.x }" }
               |  },
               |  indexes: {
               |    byB: {
               |      values: [{ field: ".b" }]
               |    }
               |  }
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot use computed fields
               |    at main.fsl:11:11
               |       |
               |    11 |     1 + f.a.x
               |       |           ^
               |       |""".stripMargin
        }

        "in unique constraints" in {
          val auth = newDB

          evalOk(
            auth,
            """|Collection.create({
               |  name: "Bar",
               |  computed_fields: {
               |    x: { body: "_ => 0" }
               |  }
               |})""".stripMargin
          )

          // Also applies to computed fields from other collections.
          // Of course, the read is also invalid in an indexed CF.
          val err = evalErr(
            auth,
            """|Collection.create({
               |  name: "Foo",
               |  computed_fields: {
               |    a: { body: "_ => Bar.byId(0)!.x" },
               |  },
               |  constraints: [
               |    { unique: [".a" ] }
               |  ]
               |})""".stripMargin
          )
          err.code shouldEqual "invalid_schema"
          err.errors.head.message shouldEqual
            """|Invalid database schema update.
               |    error: Computed fields used in indexes or constraints cannot access collections
               |    at main.fsl:9:22
               |      |
               |    9 |   compute a = (_) => Bar.byId(0)!.x
               |      |                      ^^^
               |      |
               |    error: Computed fields used in indexes or constraints cannot use computed fields
               |    at main.fsl:9:35
               |      |
               |    9 |   compute a = (_) => Bar.byId(0)!.x
               |      |                                   ^
               |      |""".stripMargin
        }
      }

      "cannot use computed fields (run-time)" in {
        val auth = newDB(typeChecked = false)
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    b: {
             |      body: "doc => 2 * doc.a"
             |    },
             |    c: {
             |      body: "doc => 2 * doc.b"
             |    }
             |  },
             |  indexes: {
             |    byC: {
             |      terms: [{ field: ".c" }],
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ a: 1 })")

        evalOk(auth, s"Foo.byC(4).count()") shouldEqual Value.Int(0)
      }

      "re-indexes when an indexed computed field changes" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  indexes: {
             |    byB: {
             |      terms: [{ field: ".b" }]
             |    },
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({ b: 0 })")

        evalOk(auth, "Foo.byB(0).count()") shouldEqual Value.Int(1)

        // Add a computed field. This should reindex based on the computed
        // field, not the fixed field.
        evalOk(
          auth,
          """|Collection.byName('Foo')!.update({
             |  computed_fields: {
             |    b: {
             |      body: "_ => 1",
             |      signature: "Number"
             |    }
             |  }
             |})""".stripMargin
        )

        // The index may need a moment to rebuild.
        eventually(timeout(5.seconds))(
          evalOk(auth, "Foo.byB(0).count()") shouldEqual Value.Int(0))
        evalOk(auth, "Foo.byB(1).count()") shouldEqual Value.Int(1)

        // Change the definition of the computed field. This should
        // reindex.
        evalOk(
          auth,
          """|Collection.byName('Foo')!.update({
             |  computed_fields: {
             |    b: {
             |      body: "_ => 2"
             |    }
             |  }
             |})""".stripMargin
        )

        eventually(timeout(5.seconds))(
          evalOk(auth, "Foo.byB(0).count()") shouldEqual Value.Int(0))
        evalOk(auth, "Foo.byB(1).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.byB(2).count()") shouldEqual Value.Int(1)

        // Remove the computed field. The index should revert to indexing
        // on the fixed field.
        evalOk(auth, "Collection.byName('Foo')!.update({ computed_fields: null })")

        eventually(timeout(5.seconds))(
          evalOk(auth, "Foo.byB(0).count()") shouldEqual Value.Int(1))
        evalOk(auth, "Foo.byB(1).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.byB(2).count()") shouldEqual Value.Int(0)
      }

      "re-indexes when an indexed computed field changes (special field syntax)" in {
        // Defined field term.
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  indexes: {
             |    byABC0: {
             |      terms: [{ field: ".['a']['b c'][0]" }]
             |    },
             |  }
             |})""".stripMargin
        )
        evalOk(auth, "Foo.create({ a: { 'b c': [0, 1] } })")
        evalOk(auth, "Foo.byABC0(0).count()") shouldEqual Value.Int(1)

        // Changes to a computed field.
        evalOk(
          auth,
          """|Collection.byName('Foo')!.update({
             |  computed_fields: {
             |    a: {
             |      body: "_ => { 'b c': [1, 0] }",
             |      signature: "Any"
             |    }
             |  }
             |})""".stripMargin
        )
        eventually(timeout(5.seconds))(
          evalOk(auth, "Foo.byABC0(0).count()") shouldEqual Value.Int(0))
        evalOk(auth, "Foo.byABC0(1).count()") shouldEqual Value.Int(1)

        // Definition of the computed field changes.
        evalOk(
          auth,
          """|Collection.byName('Foo')!.update({
             |  computed_fields: {
             |    a: {
             |      body: "_ => { 'b c': [2, 0] }"
             |    }
             |  }
             |})""".stripMargin
        )
        eventually(timeout(5.seconds))(
          evalOk(auth, "Foo.byABC0(0).count()") shouldEqual Value.Int(0))
        evalOk(auth, "Foo.byABC0(1).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.byABC0(2).count()") shouldEqual Value.Int(1)

        // Computed field removed; back to defined field.
        evalOk(auth, "Collection.byName('Foo')!.update({ computed_fields: null })")
        eventually(timeout(5.seconds))(
          evalOk(auth, "Foo.byABC0(0).count()") shouldEqual Value.Int(1))
        evalOk(auth, "Foo.byABC0(1).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.byABC0(2).count()") shouldEqual Value.Int(0)
      }

      "cannot use Time.now() because it's a read (runtime)" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    m: {
             |      body: "_ => Time.now().month"
             |    },
             |  },
             |  indexes: {
             |    byMonth: {
             |      terms: [{ field: ".m" }],
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})")

        (1 to 12) foreach { m =>
          evalOk(auth, s"Foo.byMonth($m).count()") shouldEqual Value.Int(0)
        }
      }

      "cannot use newId() because it's a read (runtime)" in {
        // Ignore the actual ID, and just return zero so that we can test that the
        // value didn't get indexed.
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    custom_id: {
             |      body: "_ => { newId(); 0 }"
             |    },
             |  },
             |  indexes: {
             |    byCustomID: {
             |      terms: [{ field: ".custom_id" }],
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})")

        evalOk(auth, s"Foo.byCustomID(0).count()") shouldEqual Value.Int(0)
      }

      "doesn't allow indexing if the ts field is used" in {
        val err = evalErr(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: { body: "doc => doc.ts" }
             |  },
             |  indexes: {
             |    byA: {
             |      terms: [{ field: ".a" }]
             |    }
             |  }
             |})""".stripMargin
        )
        err.code shouldEqual "invalid_schema"
        err.errors.head.message shouldEqual
          """|Invalid database schema update.
             |    error: Computed fields used in indexes or constraints cannot use a doc's ts field
             |    at main.fsl:5:28
             |      |
             |    5 |   compute a = (doc) => doc.ts
             |      |                            ^^
             |      |""".stripMargin
      }

      // Ensure the check tested above isn't too strict.
      "allowed indexing if an object's ts field is used" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: { body: "_ => { ts: 0 }.ts" }
             |  },
             |  indexes: {
             |    byA: {
             |      terms: [{ field: ".a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.create({})")
        evalOk(auth, "Foo.byA(0).count()") shouldEqual Value.Int(1)
      }

      "allowed indexing if the data.ts field is used" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: { body: "doc => doc.data.ts" }
             |  },
             |  indexes: {
             |    byA: {
             |      terms: [{ field: ".a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        evalOk(auth, "Foo.createData({ ts: 0 })")
        evalOk(auth, "Foo.byA(0).count()") shouldEqual Value.Int(1)
      }

      "doesn't allow indexing if the ts field is used (runtime)" in {
        val auth = newDB(typeChecked = false)
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: { body: "doc => doc.ts" }
             |  }
             |})""".stripMargin
        )

        // Pre-index: will have a resolved ts at build time.
        evalOk(auth, "Foo.create({ id: 0 })")

        evalOk(
          auth,
          """|Collection.byName("Foo")!.update({
             |  indexes: {
             |    byA: {
             |      terms: [{ field: ".a" }]
             |    }
             |  }
             |})""".stripMargin
        )

        // Post-index: will have an unresolved ts at build time.
        evalOk(auth, "Foo.create({ id : 1 })")

        evalOk(auth, "Foo.byA(Foo.byId(0)!.ts).count()") shouldEqual Value.Int(0)
        evalOk(auth, "Foo.byA(Foo.byId(1)!.ts).count()") shouldEqual Value.Int(0)
      }
    }

    "uses computed field signature when given" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  computed_fields: {
           |    foo: {
           |      body: "_ => 2",
           |      signature: "Number"
           |    }
           |  },
           |  indexes: {
           |    byFoo: {
           |      terms: [{ field: ".foo" }]
           |    }
           |  }
           |})""".stripMargin
      )

      evalOk(
        auth,
        """|Collection.create({
           |  name: "Bar",
           |  computed_fields: {
           |    foo: {
           |      body: "_ => 2"
           |    }
           |  },
           |  indexes: {
           |    byFoo: {
           |      terms: [{ field: ".foo" }]
           |    }
           |  }
           |})""".stripMargin
      )

      // Foo has an explicit signature which should widen the type.
      evalRes(auth, "Foo.create({}).foo").typeStr shouldBe "Number"
      evalRes(auth, "(x) => Foo.byFoo(x)").typeStr shouldBe "Number => Set<Foo>"

      // Bar does not, so the inferred type should be used instead.
      evalRes(auth, "Bar.create({}).foo").typeStr shouldBe "2"
      evalRes(auth, "(x) => Bar.byFoo(x)").typeStr shouldBe "2 => Set<Bar>"

      evalOk(auth, "Foo.byFoo(2)")
      evalOk(auth, "Foo.byFoo(0)")
      evalErr(auth, "Foo.byFoo('hi')").errors.map(
        _.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Type `String` is not a subtype of `Number`
           |at *query*:1:11
           |  |
           |1 | Foo.byFoo('hi')
           |  |           ^^^^
           |  |""".stripMargin
      )

      evalOk(auth, "Bar.byFoo(2)")
      evalErr(auth, "Bar.byFoo(0)").errors.map(
        _.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: `0` is not the value `2`
           |at *query*:1:11
           |  |
           |1 | Bar.byFoo(0)
           |  |           ^
           |  |""".stripMargin
      )
      evalErr(auth, "Bar.byFoo('hi')").errors.map(
        _.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: `"hi"` is not the value `2`
           |at *query*:1:11
           |  |
           |1 | Bar.byFoo('hi')
           |  |           ^^^^
           |  |""".stripMargin
      )
    }

    // We cache the result of parsing the computed field, so this is just to make
    // sure renaming a collection doesn't cause problems.
    "collection can be renamed" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  computed_fields: {
           |    bar: {
           |      body: "doc => abort('hi')"
           |    }
           |  }
           |})
           |""".stripMargin
      )

      // `; null` is there to avoid rendering the field `bar`. This doesn't matter in
      // model, but in an actual query we can't return the new doc.
      evalOk(auth, "Foo.create({ id: 1234 }); null")

      val err = evalErr(
        auth,
        """|Collection.byName('Foo')!.update({
           |  name: 'Bar',
           |})
           |
           |Foo.byId(1234)!.bar
           |""".stripMargin
      )
      err.code shouldEqual "abort"

      // Note the span still includes `Foo` even though the collection just got
      // renamed.
      err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Query aborted.
           |at *computed_field:Foo:bar*:1:13
           |  |
           |1 | doc => abort('hi')
           |  |             ^^^^^^
           |  |
           |at *query*:5:17
           |  |
           |5 | Foo.byId(1234)!.bar
           |  |                 ^^^
           |  |""".stripMargin
      )
    }

    "mva'ed terms should have the correct inferred type" in {
      def inferIndexType(sig: String) = {
        val auth = newDB
        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "User",
              |  computed_fields: {
              |    name: {
              |      body: "doc => abort(0)",
              |      signature: "$sig",
              |    }
              |  },
              |  indexes: {
              |    byName: {
              |      terms: [{ field: ".name", mva: true }]
              |    }
              |  }
              |})""".stripMargin
        )

        // We only care about the first overload, so use this lambda to avoid the
        // range arguments.
        val ty = evalRes(auth, "(x) => User.byName(x)").typeStr
        // The actual signature is `<inffered> => Set<User>`, and we only want
        // the inferred bit.
        ty.stripSuffix(" => Set<User>")
      }

      // Simple types should be unchanged
      inferIndexType("1") shouldBe "1"
      inferIndexType("1 | 2") shouldBe "1 | 2"

      // Arrays should get simplified by one layer
      inferIndexType("Array<1>") shouldBe "1"
      inferIndexType("Array<1 | 2>") shouldBe "1 | 2"
      inferIndexType("Array<1> | Array<2>") shouldBe "1 | 2"
      inferIndexType("Array<Array<1>>") shouldBe "Array<1>"
      inferIndexType("Array<1> | 2") shouldBe "2 | 1"

      // Tuples should be the union of all their elements, to only one layer
      inferIndexType("[1, 2]") shouldBe "1 | 2"
      inferIndexType("[1, [2, 3]]") shouldBe "1 | [2, 3]"
      inferIndexType("[1, 2] | [3, 4]") shouldBe "1 | 3 | 2 | 4"

      // Unions should be recursive
      inferIndexType("(1 | 2) | 3") shouldBe "1 | 2 | 3"
    }

    "mva'ed terms enforce the inferred type correctly" in {
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "User",
            |  computed_fields: {
            |    name: {
            |      body: "doc => [1, 2]",
            |    }
            |  },
            |  indexes: {
            |    byName: {
            |      terms: [{ field: ".name", mva: true }]
            |    }
            |  }
            |})""".stripMargin
      )

      evalOk(auth, "User.create({})")

      evalOk(auth, "User.byName(1).count()") shouldBe Value.Int(1)
      evalOk(auth, "User.byName(2).count()") shouldBe Value.Int(1)
      val err = evalErr(auth, "User.byName(3)")
      err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Type `3` is not a subtype of `1 | 2`
           |at *query*:1:13
           |  |
           |1 | User.byName(3)
           |  |             ^
           |  |
           |cause: `3` is not the value `1`
           |  |
           |1 | User.byName(3)
           |  |             ^
           |  |
           |cause: `3` is not the value `2`
           |  |
           |1 | User.byName(3)
           |  |             ^
           |  |""".stripMargin
      )
    }

    "overlaps with data are handled nicely" in {
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "User",
            |  computed_fields: {
            |    foo: {
            |      body: "doc => 3",
            |    }
            |  }
            |})""".stripMargin
      )

      val obj = evalOk(auth, "Object.assign({}, User.createData({ foo: 5 }))")
      obj / "foo" shouldBe Value.Int(3)
      obj / "data" / "foo" shouldBe Value.Int(5)
    }
  }
}
