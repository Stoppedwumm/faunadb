package fql.test.parser

import fql.{ Result, TextUtil }
import fql.ast._
import fql.ast.display._
import fql.parser._
import fql.test.Spec
import org.scalactic.source.Position

class FSLParserSpec extends Spec {

  "Collection" should "parse minimal" in {
    roundtrip(
      """|collection Foo {
         |}
         |""".stripMargin
    )
  }

  it should "parse with an alias" in {
    roundtrip(
      """|@alias(Bar)
         |collection Foo {
         |}
         |""".stripMargin
    )
  }

  it should "parse history_days" in {
    roundtrip(
      """|collection Foo {
         |  history_days 32
         |}
         |""".stripMargin
    )
  }

  it should "parse ttl_days" in {
    roundtrip(
      """|collection Foo {
         |  ttl_days 33
         |}
         |""".stripMargin
    )
  }

  it should "parse document_ttls" in {
    roundtrip(
      """|collection Foo {
         |  document_ttls true
         |}
         |""".stripMargin
    )

    roundtrip(
      """|collection Foo {
         |  document_ttls false
         |}
         |""".stripMargin
    )
  }

  it should "parse an index" in {
    roundtrip(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar, mva(.baz)]
         |    values [desc(mva(.buz))]
         |  }
         |}
         |""".stripMargin
    )
    parseAndRender(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar, mva(.baz)]
         |    values [asc(mva(.buz))]
         |  }
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar, mva(.baz)]
         |    values [mva(.buz)]
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "parse a unique constraint" in {
    roundtrip(
      """|collection Foo {
         |  unique [.bar, mva(.baz)]
         |}
         |""".stripMargin
    )
  }

  it should "parse a computed field definition" in {
    roundtrip(
      """|collection Foo {
         |  compute bar: String = (doc) => doc.baaz + "quux"
         |}
         |""".stripMargin
    )
  }

  it should "parse a field definition" in {
    roundtrip(
      """|collection Foo {
         |  bar: Int = 3
         |}
         |""".stripMargin
    )

    roundtrip(
      """|collection Foo {
         |  foo: Int
         |  bar: Int = 2 + 3
         |  baz: Int = Time.now().month + 3
         |  multiline: Int = {
         |    let a = 2
         |    let b = 3
         |    a + b
         |  }
         |}
         |""".stripMargin
    )

    // This looks terrible but it's valid.
    parseAndRender(
      """|collection Foo {short:Int=3}
         |""".stripMargin
    )(
      """|collection Foo {
         |  short: Int = 3
         |}
         |""".stripMargin
    )
  }

  it should "parse a field with a keyword name" in {
    roundtrip(
      """|collection Foo {
         |  privileges: Int
         |}
         |""".stripMargin
    )

    roundtrip(
      """|collection Foo {
         |  function: Int
         |}
         |""".stripMargin
    )
  }

  it should "handle nl correctly after type exprs" in {
    parseAndRender(
      """|collection Foo {
         |  bar: (Int, String)
         |  baz: (
         |    Int,
         |    String
         |  )
         |  opt: Int?
         |  many: Int? | String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  bar: [Int, String]
         |  baz: [Int, String]
         |  opt: Int?
         |  many: Int? | String
         |}
         |""".stripMargin
    )
  }

  it should "allow whitespace before colon type in field definition" in {
    parseOk("collection Foo { bar : Int = 2 + 3 }")
  }

  it should "parse a wildcard definition" in {
    roundtrip(
      """|collection Foo {
         |  *: Any
         |}
         |""".stripMargin
    )
  }

  it should "parse a check constraint" in {
    roundtrip("""|collection Foo {
                 |  check a ((doc) => doc.ok)
                 |  check b (() => 1)
                 |  check c ((x, y) => {
                 |    let z = x + y
                 |    z
                 |  })
                 |}
                 |""".stripMargin)
  }

  it should "reject invalid check constraints" in {
    parseErr(
      """|collection Foo {
         |  check ((doc) => doc.ok)
         |}
         |""".stripMargin
    )(
      """|error: Missing required name
         |at *fsl*:2:3
         |  |
         |2 |   check ((doc) => doc.ok)
         |  |   ^^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  check a ((doc) => doc.ok)
         |  check a ((doc) => !doc.ok)
         |}
         |""".stripMargin
    )(
      """|error: Duplicate check constraint name `a`
         |at *fsl*:3:9
         |  |
         |3 |   check a ((doc) => !doc.ok)
         |  |         ^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:9
         |  |
         |2 |   check a ((doc) => doc.ok)
         |  |         ^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  check a true
         |}
         |""".stripMargin
    )(
      """|error: Expected lambda check predicate
         |at *fsl*:2:11
         |  |
         |2 |   check a true
         |  |           ^^^^
         |  |""".stripMargin
    )
  }

  it should "parse special names" in {
    roundtrip(
      """|collection "0Foo$" {
         |  index byBar {
         |    terms [.[0], .["0baz"]]
         |  }
         |}
         |""".stripMargin
    )
    parseAndRender(
      """|collection '0Foo$' {
         |  index byBar {
         |    terms [.[0], .['0baz']]
         |  }
         |}
         |""".stripMargin
    )(
      """|collection "0Foo$" {
         |  index byBar {
         |    terms [.[0], .["0baz"]]
         |  }
         |}
         |""".stripMargin
    )

    roundtrip(
      """|collection Foo {
         |  index byBar {
         |    terms [.a["a b"][0], .["a b"]]
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "reject invalid index names" in {
    parseErr(
      """|collection '0Foo$' {
         |  index "0byBar.$" {}
         |}
         |""".stripMargin
    )(
      """|error: Invalid identifier `0byBar.$`
         |at *fsl*:2:9
         |  |
         |2 |   index "0byBar.$" {}
         |  |         ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicated names" in {
    parseErr(
      """|collection Foo {}
         |collection Foo {}
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `Foo`
         |at *fsl*:2:12
         |  |
         |2 | collection Foo {}
         |  |            ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:12
         |  |
         |1 | collection Foo {}
         |  |            ^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  index byBar { terms [.bar] }
         |  index byBar { terms [.bar] }
         |}
         |""".stripMargin
    )(
      """|error: Duplicate index name `byBar`
         |at *fsl*:3:9
         |  |
         |3 |   index byBar { terms [.bar] }
         |  |         ^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:9
         |  |
         |2 |   index byBar { terms [.bar] }
         |  |         ^^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  history_days 42
         |  history_days 24
         |}
         |""".stripMargin
    )(
      """|error: Duplicate history days
         |at *fsl*:3:3
         |  |
         |3 |   history_days 24
         |  |   ^^^^^^^^^^^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:3
         |  |
         |2 |   history_days 42
         |  |   ^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  ttl_days 42
         |  ttl_days 24
         |}
         |""".stripMargin
    )(
      """|error: Duplicate ttl days
         |at *fsl*:3:3
         |  |
         |3 |   ttl_days 24
         |  |   ^^^^^^^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:3
         |  |
         |2 |   ttl_days 42
         |  |   ^^^^^^^^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  document_ttls false
         |  document_ttls true
         |}
         |""".stripMargin
    )(
      """|error: Duplicate document TTLs
         |at *fsl*:3:3
         |  |
         |3 |   document_ttls true
         |  |   ^^^^^^^^^^^^^^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:3
         |  |
         |2 |   document_ttls false
         |  |   ^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicated annotation" in {
    parseErr(
      """|@alias(Bar)
         |@alias(Bar)
         |collection Foo {}
         |""".stripMargin
    )(
      """|error: Duplicate annotation `alias`
         |at *fsl*:2:1
         |  |
         |2 | @alias(Bar)
         |  | ^^^^^^^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:1
         |  |
         |1 | @alias(Bar)
         |  | ^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicate unique constraints" in {
    parseErr(
      """|collection Foo {
         |  unique [.name]
         |  unique [.name]
         |}
         |""".stripMargin
    )(
      """|error: Duplicate unique constraint.
         |at *fsl*:3:10
         |  |
         |3 |   unique [.name]
         |  |          ^^^^^^^
         |  |
         |hint: This unique constraint covers the same fields.
         |at *fsl*:2:10
         |  |
         |2 |   unique [.name]
         |  |          ^^^^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection Foo {
         |  unique [.name]
         |  unique [.["name"]]
         |}
         |""".stripMargin
    )(
      """|error: Duplicate unique constraint.
         |at *fsl*:3:10
         |  |
         |3 |   unique [.["name"]]
         |  |          ^^^^^^^^^^^
         |  |
         |hint: This unique constraint covers the same fields.
         |at *fsl*:2:10
         |  |
         |2 |   unique [.name]
         |  |          ^^^^^^^
         |  |""".stripMargin
    )

    // mva/non-mva is allowed
    roundtrip(
      """|collection Foo {
         |  unique [.name]
         |  unique [mva(.name)]
         |}
         |""".stripMargin
    )

    // ordering doesn't matter
    parseErr(
      """|collection Foo {
         |  unique [.foo, .bar]
         |  unique [.bar, .foo]
         |}
         |""".stripMargin
    )(
      """|error: Duplicate unique constraint.
         |at *fsl*:3:10
         |  |
         |3 |   unique [.bar, .foo]
         |  |          ^^^^^^^^^^^^
         |  |
         |hint: This unique constraint covers the same fields.
         |at *fsl*:2:10
         |  |
         |2 |   unique [.foo, .bar]
         |  |          ^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicate privileges" in {
    parseErr(
      """|role Bar {
         |  privileges Foo { create }
         |  privileges Foo { create }
         |}
         |""".stripMargin
    )(
      """|error: Duplicate privileges `Foo`
         |at *fsl*:3:14
         |  |
         |3 |   privileges Foo { create }
         |  |              ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:14
         |  |
         |2 |   privileges Foo { create }
         |  |              ^^^
         |  |""".stripMargin
    )
  }

  it should "reject role annotation" in {
    parseErr(
      """|@role(Bar)
         |collection Foo {}
         |""".stripMargin
    )(
      """|error: Annotation not supported here
         |at *fsl*:1:1
         |  |
         |1 | @role(Bar)
         |  | ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "parse complex definition" in {
    roundtrip(
      """|@alias(Bar)
         |collection Foo {
         |  index byBar {
         |    terms [.bar, mva(.baz)]
         |    values [desc(mva(.buz))]
         |  }
         |  unique [.bar, mva(.baz)]
         |  history_days 32
         |  ttl_days 33
         |  document_ttls true
         |}
         |""".stripMargin
    )
  }

  it should "parse migrations" in {
    roundtrip(
      """|collection Foo {
         |  migrations {
         |    backfill .c = 3
         |    drop .bar
         |    move .a -> .b
         |    split .a -> .b, .c, .d, .e, .f, .g, .h
         |    add .a
         |  }
         |}
         |""".stripMargin
    )

    // At most one migration block allowed.
    parseErr(
      """|collection Foo {
         |  migrations {
         |    backfill .c = 3
         |  }
         |  migrations {
         |    drop .bar
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Duplicate migration block
         |at *fsl*:5:3
         |  |
         |5 |     migrations {
         |  |  ___^
         |6 | |     drop .bar
         |7 | |   }
         |  | |___^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:3
         |  |
         |2 |     migrations {
         |  |  ___^
         |3 | |     backfill .c = 3
         |4 | |   }
         |  | |___^
         |  |""".stripMargin
    )

    // No names allowed.
    parseErr(
      """|collection Foo {
         |  migrations "i am named" {
         |    backfill .c = 3
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Name is not allowed here
         |at *fsl*:2:14
         |  |
         |2 |   migrations "i am named" {
         |  |              ^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // Empty block not allowed.
    parseErr(
      """|collection Foo {
         |  migrations {
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Empty migration blocks are not allowed
         |at *fsl*:2:3
         |  |
         |2 |     migrations {
         |  |  ___^
         |3 | |   }
         |  | |___^
         |  |""".stripMargin
    )
  }

  "Role" should "parse minimal" in {
    roundtrip(
      """|role Foo {
         |}
         |""".stripMargin
    )
  }

  it should "parse privileges" in {
    roundtrip(
      """|role Foo {
         |  privileges Bar {
         |    create
         |    create_with_id
         |    delete
         |    read
         |    write
         |    history_read
         |    history_write
         |    unrestricted_read
         |    call
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "parse privileges in one line" in {
    parseAndRender(
      """|role Foo {
         |  privileges Bar {
         |    create; delete; read; write; history_read;
         |    history_write; unrestricted_read; call
         |  }
         |}
         |""".stripMargin
    )(
      """|role Foo {
         |  privileges Bar {
         |    create
         |    delete
         |    read
         |    write
         |    history_read
         |    history_write
         |    unrestricted_read
         |    call
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "require valid action names" in {
    parseErr(
      """|role Foo {
         |  privileges Bar {
         |    invalid
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Invalid action `invalid`
         |at *fsl*:3:5
         |  |
         |3 |     invalid
         |  |     ^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "parse privilege predicate" in {
    roundtrip(
      """|role Foo {
         |  privileges Bar {
         |    create {
         |      predicate ((x) => x.bar)
         |    }
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "reject invalid privilege predicate" in {
    parseErr(
      """|role Foo {
         |  privileges Bar {
         |    create {
         |      predicate 42
         |    }
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Expected lambda predicate
         |at *fsl*:4:17
         |  |
         |4 |       predicate 42
         |  |                 ^^
         |  |""".stripMargin
    )
  }

  it should "parse membership" in {
    roundtrip(
      """|role Foo {
         |  membership Bar
         |}
         |""".stripMargin
    )
  }

  it should "parse membership predicate" in {
    roundtrip(
      """|role Foo {
         |  membership Bar {
         |    predicate ((x) => x.bar)
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "reject wrong membership predicate" in {
    parseErr(
      """|role Foo {
         |  membership Bar {
         |    predicate 42
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Expected lambda predicate
         |at *fsl*:3:15
         |  |
         |3 |     predicate 42
         |  |               ^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicated names" in {
    parseErr(
      """|role Foo {}
         |role Foo {}
         |""".stripMargin
    )(
      """|error: Duplicate role `Foo`
         |at *fsl*:2:6
         |  |
         |2 | role Foo {}
         |  |      ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:6
         |  |
         |1 | role Foo {}
         |  |      ^^^
         |  |""".stripMargin
    )

    parseErr(
      """|role Foo {
         |  privileges A {}
         |  privileges A {}
         |}
         |""".stripMargin
    )(
      """|error: Duplicate privileges `A`
         |at *fsl*:3:14
         |  |
         |3 |   privileges A {}
         |  |              ^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:14
         |  |
         |2 |   privileges A {}
         |  |              ^
         |  |""".stripMargin
    )

    parseErr(
      """|role Foo {
         |  privileges A {
         |    read
         |    read
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Duplicate action `read`
         |at *fsl*:4:5
         |  |
         |4 |     read
         |  |     ^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:3:5
         |  |
         |3 |     read
         |  |     ^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|role Foo {
         |  membership A
         |  membership A
         |}
         |""".stripMargin
    )(
      """|error: Duplicate membership `A`
         |at *fsl*:3:14
         |  |
         |3 |   membership A
         |  |              ^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:14
         |  |
         |2 |   membership A
         |  |              ^
         |  |""".stripMargin
    )
  }

  it should "reject annotations" in {
    parseErr(
      """|@alias(Bar) role Foo {}
         |""".stripMargin
    )(
      """|error: Annotations are not allowed here
         |at *fsl*:1:1
         |  |
         |1 | @alias(Bar) role Foo {}
         |  | ^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "parse complex definition" in {
    roundtrip(
      """|role Foo {
         |  privileges Bar {
         |    create {
         |      predicate ((x) => {
         |        x.isVip
         |      })
         |    }
         |    read
         |  }
         |  privileges Baz {
         |    read
         |    write
         |  }
         |  membership Fiz
         |  membership Buz {
         |    predicate ((x) => {
         |      x.canLogin
         |    })
         |  }
         |}
         |""".stripMargin
    )
  }

  "AccessProvider" should "parse minimal" in {
    roundtrip(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |}
         |""".stripMargin
    )
  }

  it should "parse APs config in one line" in {
    parseAndRender(
      """|access provider Foo {
         |  issuer "bar";  jwks_uri "baz"
         |}
         |""".stripMargin
    )(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |}
         |""".stripMargin
    )
  }

  it should "parse roles" in {
    roundtrip(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |  role Fizz
         |}
         |""".stripMargin
    )
  }

  it should "parse role predicates" in {
    roundtrip(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |  role Fizz {
         |    predicate ((x) => x.buzz)
         |  }
         |}
         |""".stripMargin
    )
  }

  it should "reject wrong role predicates" in {
    parseErr(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |  role Fizz {
         |    predicate 42
         |  }
         |}
         |""".stripMargin
    )(
      """|error: Expected lambda predicate
         |at *fsl*:5:15
         |  |
         |5 |     predicate 42
         |  |               ^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicated names" in {
    parseErr(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |}
         |access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |}
         |""".stripMargin
    )(
      """|error: Duplicate access provider `Foo`
         |at *fsl*:5:17
         |  |
         |5 | access provider Foo {
         |  |                 ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:17
         |  |
         |1 | access provider Foo {
         |  |                 ^^^
         |  |""".stripMargin
    )

    parseErr(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |  role Bar
         |  role Bar
         |}
         |""".stripMargin
    )(
      """|error: Duplicate role `Bar`
         |at *fsl*:5:8
         |  |
         |5 |   role Bar
         |  |        ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:4:8
         |  |
         |4 |   role Bar
         |  |        ^^^
         |  |""".stripMargin
    )
  }

  it should "reject annotations" in {
    parseErr(
      """|@alias(Bar) access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |}
         |""".stripMargin
    )(
      """|error: Annotations are not allowed here
         |at *fsl*:1:1
         |  |
         |1 | @alias(Bar) access provider Foo {
         |  | ^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "parse complex definition" in {
    roundtrip(
      """|access provider Foo {
         |  issuer "bar"
         |  jwks_uri "baz"
         |  role Fizz {
         |    predicate ((x) => {
         |      x.buzz
         |    })
         |  }
         |  role AnotherRole
         |}
         |""".stripMargin
    )
  }

  "Function" should "parse minimal" in {
    roundtrip(
      """|function foo() {
         |  42
         |}
         |""".stripMargin
    )
  }

  it should "parse arguments" in {
    roundtrip(
      """|function foo(a) {
         |  42
         |}
         |""".stripMargin
    )
    roundtrip(
      """|function foo(a, b) {
         |  42
         |}
         |""".stripMargin
    )
    roundtrip(
      """|function foo(a, b, ...c) {
         |  42
         |}
         |""".stripMargin
    )
  }

  it should "parse with types" in {
    roundtrip(
      """|function foo(a: Int): Int {
         |  42
         |}
         |""".stripMargin
    )
    roundtrip(
      """|function foo(a: Int, b: String): Int {
         |  42
         |}
         |""".stripMargin
    )
    roundtrip(
      """|function foo(a: Int, b: String, ...c: Char): Int {
         |  42
         |}
         |""".stripMargin
    )
  }

  it should "parse with an alias" in {
    roundtrip(
      """|@alias(bar)
         |function foo(a: Int): Int {
         |  42
         |}
         |""".stripMargin
    )
  }

  it should "parse with a role" in {
    roundtrip(
      """|@role(admin)
         |function foo(a: Int): Int {
         |  42
         |}
         |""".stripMargin
    )
  }

  it should "reject duplicated annotations" in {
    parseErr(
      """|@alias(Bar)
         |@alias(Baz)
         |function foo(a) { 42 }
         |""".stripMargin
    )(
      """|error: Duplicate annotation `alias`
         |at *fsl*:2:1
         |  |
         |2 | @alias(Baz)
         |  | ^^^^^^^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:1
         |  |
         |1 | @alias(Bar)
         |  | ^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "parse with annotations in one line" in {
    parseAndRender(
      """|@alias(bar) @role(admin)
         |function foo(a: Int): Int { 42 }
         |""".stripMargin
    )(
      """|@alias(bar)
         |@role(admin)
         |function foo(a: Int): Int {
         |  42
         |}
         |""".stripMargin
    )
  }

  it should "reject partial type annotations" in {
    parseErr(
      "function f(x): Int { x }"
    )("""|error: Return type is not allowed when arguments do not have a type
         |at *fsl*:1:16
         |  |
         |1 | function f(x): Int { x }
         |  |                ^^^
         |  |
         |hint: Expecting no types due to this argument not having a type
         |  |
         |1 | function f(x): Int { x }
         |  |            ^
         |  |""".stripMargin)

    parseErr(
      "function f(x: Int) { x }"
    )("""|error: Return type is required when arguments have a type
         |at *fsl*:1:18
         |  |
         |1 | function f(x: Int) { x }
         |  |                  ^
         |  |
         |hint: Expecting types due to this type annotation
         |  |
         |1 | function f(x: Int) { x }
         |  |               ^^^
         |  |""".stripMargin)

    parseErr(
      "function f(x: Int, ...y): Int { x }"
    )(
      """|error: All or no arguments must have a type
         |at *fsl*:1:23
         |  |
         |1 | function f(x: Int, ...y): Int { x }
         |  |                       ^
         |  |
         |hint: Expecting types due to this type annotation
         |  |
         |1 | function f(x: Int, ...y): Int { x }
         |  |               ^^^
         |  |""".stripMargin
    )
  }

  it should "reject duplicated names" in {
    parseErr(
      """|function foo() {42}
         |function foo() {42}
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `foo`
         |at *fsl*:2:10
         |  |
         |2 | function foo() {42}
         |  |          ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:10
         |  |
         |1 | function foo() {42}
         |  |          ^^^
         |  |""".stripMargin
    )
  }

  it should "parse complex definition" in {
    roundtrip(
      """|@alias(bar)
         |@role(admin)
         |function foo(a: Int, ...b: String): Int {
         |  if (a < 0) "invalid" else b
         |}
         |""".stripMargin
    )
  }

  it should "disallow conflicting aliases" in {
    parseErr(
      """|@alias(fun1)
         |collection coll1 {}
         |function fun1() { 0 }
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `fun1`
         |at *fsl*:3:10
         |  |
         |3 | function fun1() { 0 }
         |  |          ^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:8
         |  |
         |1 | @alias(fun1)
         |  |        ^^^^
         |  |""".stripMargin
    )

    parseErr(
      """|collection coll1 {}
         |@alias(coll1)
         |function fun1() { 0 }
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `coll1`
         |at *fsl*:2:8
         |  |
         |2 | @alias(coll1)
         |  |        ^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:12
         |  |
         |1 | collection coll1 {}
         |  |            ^^^^^
         |  |""".stripMargin
    )
  }

  it should "handle conflicting names correctly" in {
    parseErr(
      """|collection foo {}
         |function foo() { 0 }
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `foo`
         |at *fsl*:2:10
         |  |
         |2 | function foo() { 0 }
         |  |          ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:1:12
         |  |
         |1 | collection foo {}
         |  |            ^^^
         |  |""".stripMargin
    )

    // Adding an alias to the function makes this allowed.
    //
    // Name resolution will prioritize the collection when looking up `foo`, so this
    // will work as expected.
    roundtrip(
      """|collection foo {
         |}
         |
         |@alias(bar)
         |function foo() {
         |  0
         |}
         |""".stripMargin
    )

    // However, name resolution will always prioritize the collection for conflicting
    // names. So adding an alias to the collection is not enough in this case.
    parseErr(
      """|@alias(bar)
         |collection foo {}
         |function foo() { 0 }
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `foo`
         |at *fsl*:3:10
         |  |
         |3 | function foo() { 0 }
         |  |          ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:12
         |  |
         |2 | collection foo {}
         |  |            ^^^
         |  |""".stripMargin
    )

    // Duplicate names are still always disallowed, even with aliases.
    parseErr(
      """|@alias(bar)
         |function foo() { 0 }
         |@alias(baz)
         |function foo() { 0 }
         |""".stripMargin
    )(
      """|error: Duplicate collection or function `foo`
         |at *fsl*:4:10
         |  |
         |4 | function foo() { 0 }
         |  |          ^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:2:10
         |  |
         |2 | function foo() { 0 }
         |  |          ^^^
         |  |""".stripMargin
    )
  }

  it should "render in UDF form" in {
    val fns =
      parseOk(
        """|function foo(a, b) { 42 }
           |function bar(a: Int, ...b: Int): Int { 42 }
           |""".stripMargin
      ).map {
        _.asInstanceOf[SchemaItem.Function]
      }

    fns should have size 2

    fns(0).displayAsUDFBody shouldEqual
      """|(a, b) => {
         |  42
         |}""".stripMargin
    fns(1).displayAsUDFBody shouldEqual
      """|(a, ...b) => {
         |  42
         |}""".stripMargin

    fns(0).displayAsUDFSig shouldBe empty
    fns(1).displayAsUDFSig shouldBe "(a: Int, b: ...Int) => Int"
    assert(Parser.typeExpr(fns(1).displayAsUDFSig).isOk)
  }

  it should "render a paths in FQL form" in {
    val cols =
      parseOk(
        """|collection Foo {
           |  index byBar {
           |    terms [mva(.baz.bar), .ts]
           |    values [mva(.baz.bar), desc(.fizz), .buzz]
           |  }
           |}
           |""".stripMargin
      ).map {
        _.asInstanceOf[SchemaItem.Collection]
      }

    cols should have size 1
    cols.head.indexes should have size 1

    val index = cols.head.indexes.head
    val indexConfig = index.configValue
    indexConfig.terms shouldNot be(empty)
    indexConfig.values shouldNot be(empty)

    val terms = indexConfig.terms.get.configValue
    terms should have size 2
    terms(0).displayTerm shouldEqual ".baz.bar"
    terms(1).displayTerm shouldEqual ".ts"

    val values = indexConfig.values.get.configValue
    values should have size 3
    values(0).displayValue shouldEqual ".baz.bar"
    values(1).displayValue shouldEqual ".fizz"
    values(2).displayValue shouldEqual ".buzz"
  }

  private def roundtrip(src: String)(implicit pos: Position) =
    parseAndRender(src)(src)

  private def parseAndRender(
    src: String)(expected: String)(implicit pos: Position) = {
    val parsed = parseOk(src)
    val fsl = parsed.view.map { _.display }.mkString("\n")
    if (fsl != expected) {
      fail(
        s"""|** Expected:
            |$expected
            |** Actual:
            |$fsl
            |** Diff:
            |${TextUtil.diff(fsl, expected)}
            |""".stripMargin
      )
    }
  }

  private def parseOk(src: String)(implicit pos: Position) = {
    parseRawOk(src)
    Parser.schemaItems(src) match {
      case Result.Ok(parsed) => parsed
      case Result.Err(errs) =>
        errs foreach { err =>
          info(err.renderWithSource(Map(Src.Id("*fsl*") -> src)))
        }
        fail("failed to parse schema item")
    }
  }

  private def parseErr(src: String)(expected: String)(implicit pos: Position) = {
    parseRawOk(src)
    Parser.schemaItems(src) match {
      case Result.Ok(_) => fail("expected an error but parsing succeeded")
      case Result.Err(errs) =>
        val failures = errs
          .map { err => err.renderWithSource(Map(Src.Id("*fsl*") -> src)) }
          .mkString("\n")
        expected shouldEqual failures
    }
  }

  private def parseRawOk(src: String)(implicit pos: Position) =
    // raw form should never fail
    Parser.fslNodes(src) match {
      case Result.Ok(_) => ()
      case Result.Err(errs) =>
        errs foreach { err =>
          info(err.renderWithSource(Map(Src.Id("*fsl*") -> src)))
        }
        fail("failed to parse schema item in 'raw' form")
    }
}
