package fql.test

import fql.analyzer.{ CompletionItem, CompletionKind, SchemaContext }
import fql.ast.Span

class SchemaCompletionSpec extends AnalyzerSpec {
  val ctx = SchemaContext(globals, shapes)

  val Keywords = Seq("false", "null", "true", "at", "if", "let")
  val Modules =
    Seq(
      "Collection",
      "Function",
      "Math",
      "Set",
      "String",
      "Time",
      "User",
      "desc",
      "timeOrStr")
  val QueryTopLevel = Modules ++ Keywords

  val Types = Seq(
    "Any",
    "Array",
    "Boolean",
    "CollectionCollection",
    "FunctionCollection",
    "MathModule",
    "Never",
    "Number",
    "Set",
    "String",
    "Time",
    "TimeModule",
    "User",
    "UserCollection",
    "false",
    "null",
    "true"
  )

  val TopLevel = Seq("collection", "function", "role", "access provider")

  def completeItems(srcWithCursor: String) = {
    val cursor = srcWithCursor.indexOf("|")
    val src = srcWithCursor.slice(0, cursor) + srcWithCursor.slice(
      cursor + 1,
      srcWithCursor.length)

    ctx.update(src)
    ctx.completions(cursor)
  }
  def complete(srcWithCursor: String) =
    completeItems(srcWithCursor).map(_.label)

  "A context" should "complete functions" in {
    complete("|function foo(): Number { a }") shouldBe TopLevel
    complete("func|tion foo(): Number { a }") shouldBe TopLevel
    complete("func|") shouldBe TopLevel
    complete("function |foo(): Number { a }") shouldBe Seq.empty
    complete("function foo(|): Number { a }") shouldBe Seq.empty
    complete("function foo(): |Number { a }") shouldBe Types
    complete("function foo(): Number| { a }") shouldBe Types
    complete("function foo(): Number {| a }") shouldBe QueryTopLevel
    complete("function foo(): Number { |a }") shouldBe QueryTopLevel
    complete("function foo(): Number { a |}") shouldBe QueryTopLevel
    complete("function foo(): Number { a }|") shouldBe QueryTopLevel
    complete("function foo(): Number { a } |") shouldBe TopLevel

    complete("function foo(): Number { | }") shouldBe QueryTopLevel
  }

  it should "complete function arguments with no arguments yet" in {
    complete("function foo(|): Number { a }") shouldBe Seq.empty
  }

  it should "complete function arguments" in {
    complete("function foo(|a: Number): Number { a }") shouldBe Seq.empty
    complete("function foo(a|: Number): Number { a }") shouldBe Seq.empty
    complete("function foo(a:| Number): Number { a }") shouldBe Types
    complete("function foo(a: |Number): Number { a }") shouldBe Types
    complete("function foo(a: Number|): Number { a }") shouldBe Types

    complete(
      "function foo(a: Number, ...|b: Number): Number { a }") shouldBe Seq.empty
    complete("function foo(a: Number, ...b:| Number): Number { a }") shouldBe Types
    complete("function foo(a: Number, ...b: |Number): Number { a }") shouldBe Types
  }

  it should "know about function arguments in function bodies" in {
    complete("function foo(myTime: Time): Time { my| }") shouldBe Seq(
      "myTime") ++ QueryTopLevel
    complete("function foo(myTime: Time): Time { myTime.| }") shouldBe Seq(
      "add",
      "month",
      "toString",
      "year")
    complete("function foo(myTime: Time): Time { myTime.|foo }") shouldBe Seq(
      "add",
      "month",
      "toString",
      "year")

    complete("function foo(myTime: Time): Time { let a = myTime.| }") shouldBe Seq(
      "add",
      "month",
      "toString",
      "year")
  }

  it should "complete collections" in {
    val collMembers =
      Seq(
        "index",
        "unique",
        "check",
        "compute",
        "history_days",
        "ttl_days",
        "migrations")

    complete("col|") shouldBe TopLevel
    complete("collection Foo { | }") shouldBe collMembers

    complete("collection Foo { i| }") shouldBe collMembers
    complete("collection Foo { in| }") shouldBe collMembers
    complete("collection Foo { ttl_| }") shouldBe collMembers
    complete("collection Foo { index; | }") shouldBe collMembers
  }

  it should "complete index members" in {
    complete("collection Foo { index bar { | } }") shouldBe Seq("terms", "values")
    complete("collection Foo { index bar { te| } }") shouldBe Seq("terms", "values")
    complete("collection Foo { index bar { terms []; | } }") shouldBe Seq("values")

    // TODO: Complete based off of fields in the collection here
    complete("collection Foo { index bar { terms [.|] } }") shouldBe Seq.empty
  }

  it should "complete check constraint members" in {
    complete("collection Foo { check | }") shouldBe Seq.empty
    complete("collection Foo { check it_works (x => |) }") shouldBe (
      Seq("x") ++ QueryTopLevel
    )
  }

  it should "complete computed fields on doc types" in {
    complete(
      "collection Foo { compute foo = _ => 3; check it_works (x => x.|) }"
    ) shouldBe (
      Seq("foo")
    )
    complete(
      "collection Foo { compute foo = (_ => 3); check it_works (.|) }"
    ) shouldBe (
      Seq("foo")
    )
    complete(
      "collection Foo { compute foo = _ => 3; compute bar = (x => x.|) }"
    ) shouldBe (
      Seq("bar", "foo")
    )
    complete(
      "collection Foo { compute foo = _ => 3; compute bar = x => x.| }"
    ) shouldBe (
      Seq("bar", "foo")
    )

    // It should show the type of the field.
    completeItems(
      "collection Foo { compute foo: Int = (_ => 3); check it_works (.|) }"
    ) shouldBe Seq(
      CompletionItem(
        label = "foo",
        detail = "Int",
        kind = CompletionKind.Field,
        span = Span(63, 63, ctx.src),
        newCursor = 66,
        replaceText = "foo",
        retrigger = false,
        snippet = None
      )
    )
    completeItems(
      "collection Foo { compute foo = (_ => 3); check it_works (.|) }"
    ) shouldBe Seq(
      CompletionItem(
        label = "foo",
        detail = "3",
        kind = CompletionKind.Field,
        span = Span(58, 58, ctx.src),
        newCursor = 61,
        replaceText = "foo",
        retrigger = false,
        snippet = None
      )
    )
  }

  it should "complete computed fields for unique constraints and indexes" in pendingUntilFixed {
    complete("collection Foo { unique [.|] }") shouldBe Seq.empty
    // TODO: These should use the doc shape for completions.
    complete("collection Foo { compute foo = 3; unique [.|] }") shouldBe Seq("foo")
    complete(
      "collection Foo { compute foo = 3; index foo { terms [.|] } }"
    ) shouldBe (
      Seq("foo")
    )
    ()
  }

  it should "complete migration blocks" in {
    complete("collection Foo { migrations { | } }") shouldBe Seq(
      "add",
      "backfill",
      "drop",
      "split",
      "move",
      "move_conflicts",
      "move_wildcard")
  }

  it should "complete backfill" in {
    completeItems(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    backfill .| = 3
         |  }
         |}""".stripMargin
    ) shouldBe Seq(
      CompletionItem(
        label = "foo",
        detail = "Int",
        kind = CompletionKind.Field,
        span = Span(72, 72, ctx.src),
        newCursor = 75,
        replaceText = "foo",
        retrigger = false,
        snippet = None
      ),
      CompletionItem(
        label = "bar",
        detail = "String",
        kind = CompletionKind.Field,
        span = Span(72, 72, ctx.src),
        newCursor = 75,
        replaceText = "bar",
        retrigger = false,
        snippet = None
      )
    )

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    backfill .| = 3
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    backfill .foo .| = 3
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    backfill .foo = |
         |  }
         |}""".stripMargin
    ) shouldBe QueryTopLevel
  }

  Seq("add", "drop", "move_conflicts", "move_wildcard").foreach { migration =>
    it should s"complete $migration" in {
      complete(
        s"""|collection Foo {
            |  foo: Int
            |  bar: String
            |
            |  migrations {
            |    $migration .|
            |  }
            |}""".stripMargin
      ) shouldBe Seq("foo", "bar")
    }
  }

  it should "complete split" in {
    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    split .foo -> .|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    split .foo -> .|bar
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    split .bar -> .|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |  qux: Boolean
         |
         |  migrations {
         |    split .foo -> .bar, .z|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "qux")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |  qux: Boolean
         |
         |  migrations {
         |    split .|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar", "qux")
  }

  it should "complete move" in {
    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    move .|foo -> .bar
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    move .foo -> .|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("bar")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    move .bar -> .|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    move .| -> .bar
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo")

    complete(
      """|collection Foo {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    move .|
         |  }
         |}""".stripMargin
    ) shouldBe Seq("foo", "bar")
  }

  it should "complete roles" in {
    complete("ro|") shouldBe TopLevel
    complete("role Bar { | }") shouldBe Seq("privileges", "membership")
    complete("role Bar { pri| }") shouldBe Seq("privileges", "membership")
  }

  it should "complete access providers" in {
    complete("acc|") shouldBe TopLevel
    complete("access provider Bar { issuer \"|foo\"; }") shouldBe Seq(
      "issuer",
      "jwks_uri",
      "role")

    // While this is correct, IDEs probably won't handle this correctly, with the
    // space existing.
    complete("access pro|") shouldBe TopLevel
  }

  it should "complete role actions" in {
    val privilegeActions = Seq(
      "read",
      "history_read",
      "write",
      "create",
      "create_with_id",
      "delete",
      "call")

    complete("role MyRole { privileges User { | } }") shouldBe privilegeActions
    complete("role MyRole { privileges User { rea| } }") shouldBe privilegeActions
    complete(
      "role MyRole { privileges User { read; | } }") shouldBe (privilegeActions diff Seq(
      "read"))
    complete(
      "role MyRole { privileges User { read | } }") shouldBe (privilegeActions diff Seq(
      "read"))
  }

  it should "complete role action predicates" in {
    complete("role MyRole { privileges User { read { | } } }") shouldBe Seq(
      "predicate")
    complete("role MyRole { privileges User { read { pre| } } }") shouldBe Seq(
      "predicate")
  }

  it should "complete membership predicates" in {
    complete("role MyRole { membership User { | } }") shouldBe Seq("predicate")
    complete("role MyRole { membership User { pr| } }") shouldBe Seq("predicate")
  }
}
