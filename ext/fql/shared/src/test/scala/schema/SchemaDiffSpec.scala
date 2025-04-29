package fql.test.schema

import fql.ast._
import fql.color.ColorKind
import fql.env.DatabaseSchema
import fql.env.SchemaTypeValidator
import fql.migration.MigrationValidator
import fql.parser.Parser
import fql.schema.{ Change, Diff, DiffRender, SchemaDiff }
import fql.test.TypeSpec
import fql.Result
import fql.TextUtil
import org.scalactic.source.Position

class SchemaDiffSpec extends SchemaDiffHelperSpec {
  "SchemaDiff" should "detect additions" in {
    diff(before = "")(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|* Adding collection `Foo` to main.fsl:1:1
         |
         |""".stripMargin
    )
  }

  it should "detect removals" in {
    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(after = "")(
      """|* Removing collection `Foo` from main.fsl:1:1
         |
         |""".stripMargin
    )
  }

  it should "detect new annotations" in {
    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|@alias(Bar)
         |collection Foo {
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  + add alias set to Bar
         |
         |""".stripMargin
    )
  }

  it should "detect dropped annotations" in {
    diff(
      """|@alias(Bar)
         |collection Foo {
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  - remove alias set to Bar
         |
         |""".stripMargin
    )
  }

  it should "detect a scalar addition" in {
    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  history_days 42
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  + add history_days set to 42
         |
         |""".stripMargin
    )
  }

  it should "detect a scalar removal" in {
    diff(
      """|collection Foo {
         |  history_days 42
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  - remove history_days set to 42
         |
         |""".stripMargin
    )
  }

  it should "detect a scalar modification" in {
    diff(
      """|collection Foo {
         |  history_days 0
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  history_days 42
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  ~ change history_days from 0 to 42
         |
         |""".stripMargin
    )
  }

  it should "detect seq additions" in {
    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  unique [.bar]
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Constraints:
         |  + add unique constraint on [.bar]
         |
         |""".stripMargin
    )
  }

  it should "detect seq removals" in {
    diff(
      """|collection Foo {
         |  unique [.bar]
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Constraints:
         |  - remove unique constraint on [.bar]
         |
         |""".stripMargin
    )
  }

  it should "detect seq modifications" in {
    diff(
      """|collection Foo {
         |  unique [.bar]
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  unique [.bar, .baz]
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Constraints:
         |  + add unique constraint on [.bar, .baz]
         |  - remove unique constraint on [.bar]
         |
         |""".stripMargin
    )
  }

  it should "detect block additions" in {
    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Indexes:
         |  + add index `byBar`
         |
         |""".stripMargin
    )
  }

  it should "detect block removals" in {
    diff(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Indexes:
         |  - remove index `byBar`
         |
         |""".stripMargin
    )
  }

  it should "detect block modifications" in {
    diff(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  index byBar {
         |    terms [.bar, .baz]
         |    values [.bar, .baz]
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Indexes:
         |  ~ change index `byBar`
         |    ~ change terms from [.bar] to [.bar, .baz]
         |
         |""".stripMargin
    )
  }

  it should "detect optional config addition" in {
    diff(
      """|role Foo {
         |  membership Bar
         |}
         |""".stripMargin
    )(
      """|role Foo {
         |  membership Bar {
         |    predicate ((x) => x.baz)
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying role `Foo` at main.fsl:1:1:
         |  ~ change membership on `Bar` by adding a predicate
         |
         |""".stripMargin
    )
  }

  it should "detect optional config removal" in {
    diff(
      """|role Foo {
         |  membership Bar {
         |    predicate ((x) => x.baz)
         |  }
         |}
         |""".stripMargin
    )(
      """|role Foo {
         |  membership Bar
         |}
         |""".stripMargin
    )(
      """|* Modifying role `Foo` at main.fsl:1:1:
         |  ~ change membership on `Bar` by removing a predicate
         |
         |""".stripMargin
    )
  }

  it should "detect optional config modification" in {
    diff(
      """|role Foo {
         |  membership Bar {
         |    predicate ((x) => x.baz)
         |  }
         |}
         |""".stripMargin
    )(
      """|role Foo {
         |  membership Bar {
         |    predicate ((x) => x.fizz)
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying role `Foo` at main.fsl:1:1:
         |  ~ change membership on `Bar` by modifying its predicate
         |
         |""".stripMargin
    )
  }

  it should "detect modifications to blocks with optional configs" in {
    diff(
      """|role Foo {
         |  privileges Bar {
         |    read
         |    write
         |    create {
         |      predicate ((x) => x.fizz)
         |    }
         |    history_read {
         |      predicate ((x) => x.buzz)
         |    }
         |  }
         |}
         |""".stripMargin
    )(
      """|role Foo {
         |  privileges Bar {
         |    write {
         |      predicate ((x) => x.fizz)
         |    }
         |    create
         |    history_read {
         |      predicate ((x) => x.somethingelse)
         |    }
         |    read
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying role `Foo` at main.fsl:1:1:
         |  ~ change privileges on `Bar`
         |    ~ change write by adding a predicate
         |    ~ change create by removing a predicate
         |    ~ change history_read by modifying its predicate
         |
         |""".stripMargin
    )
  }

  it should "detect function signature changes" in {
    diff(
      """|function foo(a) {
         |  42
         |}
         |""".stripMargin
    )(
      """|function foo(a, b) {
         |  42
         |}
         |""".stripMargin
    )(
      """|* Modifying function `foo` at main.fsl:1:1:
         |  ~ change signature
         |
         |""".stripMargin
    )
  }

  it should "detect function type changes" in {
    diff(
      """|function foo(a: A): A {
         |  42
         |}
         |""".stripMargin
    )(
      """|function foo(a) {
         |  42
         |}
         |""".stripMargin
    )(
      """|* Modifying function `foo` at main.fsl:1:1:
         |  ~ change signature
         |
         |""".stripMargin
    )
  }

  it should "detect function body changes" in {
    diff(
      """|function foo(a) {
         |  42
         |}
         |""".stripMargin
    )(
      """|function foo(a) {
         |  43
         |}
         |""".stripMargin
    )(
      """|* Modifying function `foo` at main.fsl:1:1:
         |  ~ change body
         |
         |""".stripMargin
    )
  }

  it should "detect multiple changes" in {
    diff(
      """|collection Foo {
         |  history_days 10
         |  index byBar {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |  check ok ((doc) => doc.ok)
         |  check cool ((doc) => doc.colo)
         |}
         |
         |function double(n) {
         |  2 * n
         |}
         |""".stripMargin
    )(
      """|@alias(Bar)
         |collection Foo {
         |  compute floof: Int = ((doc) => !doc.notfloof)
         |  index byBuz {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |  unique [mva(.bar)]
         |  check notok ((doc) => !doc.ok)
         |  check cool ((doc) => doc.cool)
         |  history_days 0
         |  ttl_days 10
         |}
         |
         |function double(a) {
         |  a * 2
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Computed fields:
         |  + add computed field `floof`
         |
         |  * Indexes:
         |  + add index `byBuz`
         |  - remove index `byBar`
         |
         |  * Constraints:
         |  + add unique constraint on [mva(.bar)]
         |  + add check constraint `notok`
         |  - remove check constraint `ok`
         |  ~ change check constraint `cool` body
         |
         |  * Configuration:
         |  + add alias set to Bar
         |  + add ttl_days set to 10
         |  ~ change history_days from 10 to 0
         |
         |* Modifying function `double` at main.fsl:15:1:
         |  ~ change signature
         |  ~ change body
         |
         |""".stripMargin
    )
  }

  it should "display semantic adds correctly" in {
    diff(
      ""
    )(
      """|@alias(Bar)
         |collection Foo {
         |  compute floof: Int = ((doc) => !doc.notfloof)
         |  index byBuz {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |  unique [mva(.bar)]
         |  check notok ((doc) => !doc.ok)
         |  check cool ((doc) => doc.cool)
         |  history_days 0
         |  ttl_days 10
         |}
         |
         |function double(a) { a * 2 }
         |function tripple(a) { a * 3 }
         |""".stripMargin
    )(
      """|* Adding collection `Foo` to main.fsl:1:1:
         |  * Computed fields:
         |  + add computed field `floof`
         |
         |  * Indexes:
         |  + add index `byBuz`
         |
         |  * Constraints:
         |  + add unique constraint on [mva(.bar)]
         |  + add check constraint `notok`
         |  + add check constraint `cool`
         |
         |  * Configuration:
         |  + add alias set to Bar
         |  + add ttl_days set to 10
         |
         |* Adding function `double` to main.fsl:15:1
         |
         |* Adding function `tripple` to main.fsl:16:1
         |
         |""".stripMargin
    )
  }

  it should "display semantic removes correctly" in {
    diff(
      """|@alias(Bar)
         |collection Foo {
         |  compute floof: Int = ((doc) => !doc.notfloof)
         |  index byBuz {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |  unique [mva(.bar)]
         |  check notok ((doc) => !doc.ok)
         |  check cool ((doc) => doc.cool)
         |  history_days 0
         |  ttl_days 10
         |}
         |
         |function double(a) { a * 2 }
         |function tripple(a) { a * 3 }
         |""".stripMargin
    )(
      ""
    )(
      """|* Removing collection `Foo` from main.fsl:1:1:
         |  * Computed fields:
         |  - remove computed field `floof`
         |
         |  * Indexes:
         |  - remove index `byBuz`
         |
         |  * Constraints:
         |  - remove unique constraint on [mva(.bar)]
         |  - remove check constraint `notok`
         |  - remove check constraint `cool`
         |
         |  * Configuration:
         |  - remove alias set to Bar
         |  - remove ttl_days set to 10
         |
         |* Removing function `double` from main.fsl:15:1
         |
         |* Removing function `tripple` from main.fsl:16:1
         |
         |""".stripMargin
    )
  }

  it should "not detect diffs from spans changing" in {
    diff(
      """|collection Foo {
         |  compute x = (doc => doc.x)
         |  foo: Int = 3
         |}
         |role myRole {
         |  privileges Foo {
         |    read {
         |      predicate (doc => doc.foo)
         |    }
         |  }
         |}
         |function foo(x: Int): Int { x }
         |""".stripMargin
    )("""|collection Foo {
         |  compute x = (doc =>  doc.x )
         |  foo :  Int  =  3
         |}
         |role myRole {
         |  privileges Foo {
         |    read {
         |      predicate (  doc =>  doc.foo )
         |    }
         |  }
         |}
         |function foo( x:  Int ): Int {   x  }
         |""".stripMargin)("")
  }

  it should "detect fields being added" in pendingUntilFixed {
    diff(
      """|collection Foo {}
         |""".stripMargin
    )(
      """|collection Foo {
         |  foo: Int = 3
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    + add field `foo`
         |    - remove implicit wildcard
         |
         |""".stripMargin)
  }

  it should "detect fields being removed" in pendingUntilFixed {
    diff(
      """|collection Foo {
         |  foo: Int = 3
         |}
         |""".stripMargin
    )(
      """|collection Foo {}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    + add implicit wildcard
         |    - remove field `foo`
         |
         |""".stripMargin)
  }

  it should "detect fields being change to computed" in pendingUntilFixed {
    diff(
      """|collection Foo {
         |  foo: Int = 3
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  compute foo = (doc => doc.foo)
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    + add computed field `foo`
         |    + add implicit wildcard
         |    - remove field `foo`
         |
         |""".stripMargin)
  }

  it should "detect field signature changes" in pendingUntilFixed {
    diff(
      """|collection Foo {
         |  a: Int
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: Number
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    ~ change type of field `a`
         |
         |""".stripMargin)
  }

  // NB: This is unobservable at the top-level when using real schema because we
  //     restrict the top-level wildcard to have type Any.
  it should "detect wildcard changes" in pendingUntilFixed {
    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  *: Null
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    ~ change wildcard type
         |
         |""".stripMargin)

    diff(
      """|collection Foo {
         |  *: Any
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  *: Null
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    ~ change wildcard type
         |
         |""".stripMargin)

    diff(
      """|collection Foo {
         |  *: Null
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    ~ change wildcard type from `Null` to `Any`
         |
         |""".stripMargin)

    diff(
      """|collection Foo {
         |  a: Number
         |  *: Any
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: Number
         |}
         |""".stripMargin
    )("""|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Summary:
         |    - remove wildcard
         |
         |""".stripMargin)
  }

  it should "not detect implicit => explicit wildcard changes" in {
    diff("collection Foo {}")("collection Foo { *: Any }")("")
  }

  it should "detect items being moved" in {
    diff(
      "collection Foo {}",
      Src.SourceFile("main.fsl")
    )(
      """|collection Foo {
         |  history_days 10
         |}
         |""".stripMargin,
      Src.SourceFile("collections.fsl")
    )(
      """|* Modifying collection `Foo` at collections.fsl:1:1 (previously defined at main.fsl:1:1):
         |  * Configuration:
         |  + add history_days set to 10
         |
         |""".stripMargin
    )
  }

  it should "detect renames" in {
    diff(
      """|collection Foo {}
         |function bar() {42}
         |""".stripMargin
    )(
      """|collection NewFoo {}
         |function newBar() {42}
         |""".stripMargin,
      renames = Map(
        (SchemaItem.Kind.Collection, "NewFoo") -> "Foo",
        (SchemaItem.Kind.Function, "newBar") -> "bar")
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  ~ rename to `NewFoo`
         |
         |* Modifying function `bar` at main.fsl:2:1:
         |  ~ rename to `newBar`
         |
         |""".stripMargin
    )
  }

  it should "not consider history_days 0 a semantic change" in {
    diff(
      """|collection Foo {
         |  history_days 0
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      ""
    )

    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  history_days 0
         |}
         |""".stripMargin
    )(
      ""
    )
  }

  it should "consider new migrations a semantic change" in {
    diffMigrations(
      """|collection Foo {
         |  _no_wildcard: Null
         |  migrations {
         |    add .foo
         |  }
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  _no_wildcard: Null
         |  bar: Int = 3
         |  migrations {
         |    add .foo
         |    add .bar
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Defined fields:
         |  + add field `.bar` with backfill `3`
         |
         |""".stripMargin
    )

    diffMigrations(
      """|collection Foo {
         |  migrations {
         |    backfill .foo = 3
         |    backfill .bar = 3
         |  }
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  migrations {
         |    backfill .bar = 3
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  No semantic changes.
         |
         |""".stripMargin
    )

    diffMigrations(
      """|collection Foo {
         |  _no_wildcard: Null
         |  migrations {
         |    add .foo
         |    backfill .foo = 3
         |  }
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  _no_wildcard: Null
         |  foo: Int
         |  migrations {
         |    add .foo
         |    backfill .foo = 5
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Defined fields:
         |  + add field `.foo` with backfill `5`
         |
         |""".stripMargin
    )
  }

  it should "renderSemantic works" in {
    diffMigrationsSemantic(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin
    )(
      """|collection Foo {
         |  b: Int
         |  c: String = "hello"
         |
         |  migrations {
         |    move .a -> .b
         |    add .c
         |  }
         |}""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Defined fields:
         |  ~ move field `.a` to field `.b`
         |  + add field `.c` with backfill `"hello"`
         |
         |""".stripMargin
    )
  }

  it should "render migrations nicely" in {
    diffMigrations(
      """|collection Foo {
         |  migrations {
         |    add .foo
         |    drop .foo
         |  }
         |}""".stripMargin
    )(
      """|collection Foo {
         |  a: Int
         |  b: String = "hello"
         |  c: String?
         |  d: String
         |  dump: { *: Any }?
         |
         |  migrations {
         |    add .foo
         |    drop .foo
         |    add .bye
         |    drop .bye
         |    add .a
         |    backfill .a = 11
         |    add .b
         |    add .c
         |    split .c -> .c, .d
         |    backfill .d = "split!"
         |    add .dump
         |    move_conflicts .dump
         |    move_wildcard .dump
         |  }
         |}""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Defined fields:
         |  - drop field `.bye`
         |  + add field `.a` with backfill `11`
         |  + add field `.b` with backfill `"hello"`
         |  ~ split field `.c` into fields `.c`,`.d`
         |    - if the value matches type `String | Null`, the value stays in field `.c`
         |    - else if the value matches type `String`, the value moves to field `.d`
         |  + backfill field `.d` with value `"split!"`
         |  ~ move values with type conflicts into `.dump`
         |  ~ move all fields into the field `.dump` except fields in the list `[.d, .dump, .c, .b, .a]`
         |
         |""".stripMargin
    )

    // Cover the split cases in a separate test...
    diffMigrations(
      """|collection Foo {
         |  letters: "A" | "B" | "C" | "D"
         |  migrations {
         |    add .letters
         |  }
         |}""".stripMargin
    )(
      """|collection Foo {
         |  a: "A"?
         |  b: "B" = "B"
         |  c: "C"
         |  d: "D"
         |
         |  migrations {
         |    split .letters -> .a, .b, .c, .d
         |    backfill .c = "C"
         |    backfill .d = "D"
         |  }
         |}""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Defined fields:
         |  ~ split field `.letters` into fields `.a`,`.b`,`.c`,`.d`
         |    - if the value matches type `"A" | Null`, the value moves to field `.a`
         |    - else if the value matches type `"B"`, the value moves to field `.b`
         |    - else if the value matches type `"C"`, the value moves to field `.c`
         |    - else if the value matches type `"D"`, the value moves to field `.d`
         |  + backfill field `.b` with value `"B"`
         |  + backfill field `.c` with value `"C"`
         |  + backfill field `.d` with value `"D"`
         |
         |""".stripMargin
    )
  }

  it should "detect removing defaults" in {
    diffMigrations(
      """|collection Foo {
         |  foo: String = ""
         |}""".stripMargin
    )(
      """|collection Foo {
         |  foo: String
         |}""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1:
         |  * Configuration:
         |  - remove default value from field `.foo`
         |
         |""".stripMargin
    )
  }

  it should "consistently order diffs" in pendingUntilFixed {
    (1 to 10) foreach { _ =>
      diff(
        """|collection Foo {
           |  a: Number
           |  b: Number
           |  c: String
           |  d: String
           |}""".stripMargin
      )(
        """|collection Foo {
           |  c: Number
           |  d: Number
           |  e: String
           |  f: String
           |}""".stripMargin
      )(
        """|* Modifying collection `Foo` at main.fsl:1:1:
           |  * Summary:
           |    + add field `e`
           |    + add field `f`
           |    - remove field `a`
           |    - remove field `b`
           |    ~ change type of field `c`
           |    ~ change type of field `d`
           |
           |""".stripMargin
      )
    }
  }

  it should "generate correct diff for duplicated items" in {
    val src = Src.SourceFile("main")

    val content =
      """
        |role foo {
        |  privileges Agents { create }
        |}
        |""".stripMargin

    val before = Parser.schemaItems(content, src) getOrElse Seq.empty

    val privilegeAfter = Member.Named(
      Name("Agents", Span.Null),
      Member.Typed(
        Member.Kind.Privileges,
        SchemaItem.Role.PrivilegeConfig(
          Seq(Member(Member.Kind.Create, Config.Opt(None), Span.Null)),
          Span.Null),
        Span.Null
      )
    )

    val diffs = SchemaDiff.diffItems(
      before,
      List(
        SchemaItem.Role(
          Name("foo", Span.Null),
          Seq(privilegeAfter, privilegeAfter),
          span = Span.Null)),
      Map.empty)

    val Seq(Diff.Modify(_, _, changes, _)) = diffs
    val Seq(SchemaItem.Role(_, Seq(privilegeBefore), _, _, _)) = before

    changes shouldBe Seq(
      Change.Add(privilegeAfter),
      Change.Add(privilegeAfter),
      Change.Remove(privilegeBefore))
  }

  def diff(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl"),
    renames: SchemaDiff.Renames = Map.empty
  )(
    expectedDiff: String
  )(implicit pos: Position) = {
    val beforeItems = Parser.schemaItems(before, srcBefore) match {
      case Result.Ok(v)  => v
      case Result.Err(e) => fail(s"failed to parse: $e")
    }
    val afterItems = Parser.schemaItems(after, srcAfter) match {
      case Result.Ok(v)  => v
      case Result.Err(e) => fail(s"failed to parse: $e")
    }
    val diffs = SchemaDiff.diffItems(beforeItems, afterItems, renames)

    val diffText =
      DiffRender.renderSemantic(
        Map(srcBefore -> before),
        Map(srcAfter -> after),
        diffs,
        ColorKind.None
      )

    if (diffText != expectedDiff) {
      withClue(TextUtil.diff(diffText, expectedDiff)) {
        fail("generated and expected diffs do not match")
      }
    }
  }

  def diffMigrations(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl"),
    renames: SchemaDiff.Renames = Map.empty
  )(
    expectedDiff: String
  )(implicit pos: Position) =
    diffMigrations0(before, srcBefore)(after, srcAfter, renames)(expectedDiff)(
      DiffRender.renderSemantic(_, _, _, ColorKind.None))

  def diffMigrationsSemantic(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl"),
    renames: SchemaDiff.Renames = Map.empty
  )(
    expectedDiff: String
  )(implicit pos: Position) =
    diffMigrations0(before, srcBefore)(after, srcAfter, renames)(expectedDiff)(
      DiffRender.renderSemantic(_, _, _, ColorKind.None))
}

class SchemaDiffHelperSpec extends TypeSpec {
  def diffVeryShort(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl"),
    renames: SchemaDiff.Renames = Map.empty
  )(
    expectedDiff: String
  )(implicit pos: Position) =
    diffMigrations0(before, srcBefore)(after, srcAfter, renames)(expectedDiff)(
      DiffRender.renderSummary(_, _, _, ColorKind.None))

  // Like `diff`, but also runs the full migration validation and typechecking.
  def diffMigrations0(before: String, srcBefore: Src.Id)(
    after: String,
    srcAfter: Src.Id,
    renames: SchemaDiff.Renames
  )(
    expectedDiff: String
  )(renderer: (Map[Src.Id, String], Map[Src.Id, String], Seq[Diff]) => String)(
    implicit pos: Position) = {
    val beforeItems = Parser.schemaItems(before, srcBefore) match {
      case Result.Ok(v)  => v
      case Result.Err(e) => fail(s"failed to parse: $e")
    }
    val afterItems = Parser.schemaItems(after, srcAfter) match {
      case Result.Ok(v)  => v
      case Result.Err(e) => fail(s"failed to parse: $e")
    }
    val diffs0 = SchemaDiff.diffItems(beforeItems, afterItems, renames)

    val db =
      DatabaseSchema.fromItems(afterItems, v4Funcs = Seq.empty, v4Roles = Seq.empty)

    val typer = (SchemaTypeValidator
      .validate(StdlibEnv(), db, isEnvTypechecked = false)
      .map(_.environment) match {
      case Result.Ok(v) => v
      case Result.Err(e) =>
        fail(
          s"failed to typecheck:\n${e.map(_.renderWithSource(Map(srcAfter -> after))).mkString("\n\n")}")
    }).newTyper()

    // The schema manager does this in model, but checks for empty collections first.
    // For the purpose of these unit tests, assume all collections have docs.
    val diffs = diffs0.map {
      case diff @ Diff.Modify(
            from: SchemaItem.Collection,
            to: SchemaItem.Collection,
            _,
            _) =>
        val migrations = MigrationValidator
          .validate(typer, from, to) match {
          case Result.Ok(v) => v
          case Result.Err(e) =>
            fail(
              s"failed to validate migrations:\n${e.map(_.renderWithSource(Map(srcAfter -> after))).mkString("\n\n")}")
        }
        diff.copy(migrations = migrations)

      case diff => diff
    }

    val diffText =
      renderer(
        Map(srcBefore -> before),
        Map(srcAfter -> after),
        diffs
      )

    if (diffText != expectedDiff) {
      println(diffText)
      withClue(TextUtil.diff(diffText, expectedDiff)) {
        fail("generated and expected diffs do not match")
      }
    }
  }
}
