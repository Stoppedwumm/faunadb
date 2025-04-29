package fql.test.schema

import fql.{ Result, TextUtil }
import fql.ast._
import fql.parser.Parser
import fql.schema.{ SchemaDiff, SchemaPatch }
import fql.test.Spec
import org.scalactic.source.Position

class SchemaPatchSpec extends Spec {

  private var renames: SchemaDiff.Renames = _

  before { renames = Map.empty }

  "Schema items" should "be added to main file if src is unknown" in {
    patch(
      Src.SourceFile("main.fsl") -> ""
    )(
      Src.Null -> "collection Foo {}",
      Src.Null -> "collection Bar {}"
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |}
           |
           |collection Bar {
           |}
           |""".stripMargin
    )
  }

  it should "be added to declared src if known" in {
    patch(
      Src.SourceFile("main.fsl") -> "// start of main.fsl"
    )(
      Src.SourceFile("main.fsl") -> "collection Foo {}",
      Src.SourceFile("cols.fsl") -> "collection Bar {}"
    )(
      Src.SourceFile("main.fsl") ->
        """|// start of main.fsl
           |
           |collection Foo {
           |}
           |""".stripMargin,
      Src.SourceFile("cols.fsl") ->
        """|collection Bar {
           |}
           |""".stripMargin
    )
  }

  it should "adds extra nl when adding a new schema item" in {
    patch(
      Src.SourceFile("main.fsl") ->
        "collection Foo {}"
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |collection Bar {}""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |
           |collection Bar {
           |}
           |""".stripMargin
    )
  }

  it should "preserve existing comments when adding new items" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {} // after close brace
           |// after Foo
           |
           |// end of main.fsl
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |collection Bar {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {} // after close brace
           |// after Foo
           |
           |// end of main.fsl
           |
           |collection Bar {
           |}
           |""".stripMargin
    )

    patch(
      Src.SourceFile("main.fsl") ->
        "// Comment without nl at the end"
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// Comment without nl at the end
           |
           |collection Foo {
           |}
           |""".stripMargin
    )

    patch(
      Src.SourceFile("main.fsl") ->
        """|// Comment with nl at the end
           |// Comment without nl at the end""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// Comment with nl at the end
           |// Comment without nl at the end
           |
           |collection Foo {
           |}
           |""".stripMargin
    )
  }

  it should "remove an item" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// start of main.fsl
           |// comment not associated with Foo
           |
           |// before collection Foo
           |// comment associated with Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before index */ index byBar {
           |    // inside index
           |    terms [.bar]
           |    values [.bar, .ts]
           |  } // after index
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") -> ""
    )(
      // NB. Only top level comments preceeding an item is associated with it.
      Src.SourceFile("main.fsl") ->
        """|// start of main.fsl
           |// comment not associated with Foo
           |
           |// after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "preserve comments when removing an item" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// start of main.fsl
           |// comment not associated with Foo
           |
           |// before collection Foo
           |// comment associated with Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before index */ index byBar {
           |    // inside index
           |    terms [.bar]
           |    values [.bar, .ts]
           |  } // after index
           |} // after Foo's close brace
           |// after collection Foo
           |
           |// before collection Bar
           |// comment associated with Bar
           |collection Bar { // after Bar's open brace
           |  // inside collection Bar
           |  /* before index */ index byBaz {
           |    // inside index
           |    terms [.baz]
           |    values [.baz, .ts]
           |  } // after index
           |} // after Bar's close brace
           |// after collection Bar
           |
           |// end of main.fsl
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Bar {
           |  index byBaz {
           |    terms [.baz]
           |    values [.baz, .ts]
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// start of main.fsl
           |// comment not associated with Foo
           |
           |// after Foo's close brace
           |// after collection Foo
           |
           |// before collection Bar
           |// comment associated with Bar
           |collection Bar { // after Bar's open brace
           |  // inside collection Bar
           |  /* before index */ index byBaz {
           |    // inside index
           |    terms [.baz]
           |    values [.baz, .ts]
           |  } // after index
           |} // after Bar's close brace
           |// after collection Bar
           |
           |// end of main.fsl
           |""".stripMargin
    )
  }

  it should "support renaming an item" in {
    renames = Map((SchemaItem.Kind.Collection -> "Bar") -> "Foo")
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection /* before */ Foo /* after */ {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Bar {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection /* before */ Bar /* after */ {}
           |""".stripMargin
    )
  }

  "Members" should "patch a config addition" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10
           |}
           |""".stripMargin
    )
  }
  it should "preserve comments when adding a new config" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  history_days 10
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch multiple config additions" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10
           |  ttl_days 5
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  history_days 10
           |  ttl_days 5
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch a config removal" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  history_days 10
           |  ttl_days 5
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10
           |}
           |""".stripMargin
    )(
      // NB. Comments are no associated with item members.
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  history_days 10
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "preserve comments when dropping config" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before history_days */ history_days 10 /* after history_days */
           |  /* before ttl_days */ ttl_days 5 /* after ttl_days */
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10
           |}
           |""".stripMargin
    )(
      // NB. Comments are no associated with item members.
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before history_days */ history_days 10 /* after history_days */
           |  /* before ttl_days */ /* after ttl_days */
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch multiple config removals" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before history_days */ history_days 10 /* after history_days */
           |  /* before ttl_days */ ttl_days 5 /* after ttl_days */
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before history_days */ /* after history_days */
           |  /* before ttl_days */ /* after ttl_days */
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch a block config addition" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  index byBar {
           |    terms [.bar]
           |    values [.bar, .ts]
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  index byBar {
           |    terms [.bar]
           |    values [.bar, .ts]
           |  }
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch a block config removal" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before index */ index byBar {
           |    // inside index
           |    terms [.bar]
           |    values [.bar, .ts]
           |  } // after index
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before index */ // after index
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch a simple config change" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before history_days */ history_days 10 /* after history_days */
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 42
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before history_days */ history_days 42 /* after history_days */
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch nested configs additions" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  index byBar {}
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  index byBar {
           |    terms [.bar]
           |    values [.bar, .ts]
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  index byBar {
           |    values [.bar, .ts]
           |    terms [.bar]
           |  }
           |}
           |""".stripMargin
    )
  }

  it should "patch preserve comments on nested configs additions" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |// comment associated with Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before index */ index byBar {
           |    // inside index
           |  } // after index
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  index byBar {
           |    terms [.bar]
           |    values [.bar, .ts]
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// before collection Foo
           |// comment associated with Foo
           |collection Foo { // after Foo's open brace
           |  // inside collection Foo
           |  /* before index */ index byBar {
           |    // inside index
           |    values [.bar, .ts]
           |    terms [.bar]
           |  } // after index
           |} // after Foo's close brace
           |// after collection Foo
           |""".stripMargin
    )
  }

  it should "patch predicate function addition" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate (x => x.baz)
           |    }
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.baz)
           |    }
           |  }
           |}
           |""".stripMargin
    )
  }

  it should "patch predicate function removal" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate (x => x.baz)
           |    }
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |  }
           |}
           |""".stripMargin
    )
  }

  it should "patch predicate function modification" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.baz)
           |    }
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.fizz)
           |    }
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.fizz)
           |    }
           |  }
           |}
           |""".stripMargin
    )
  }

  it should "optional config addition" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.fizz)
           |    }
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.fizz)
           |    }
           |  }
           |}
           |""".stripMargin
    )
  }

  it should "optional config removal" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read {
           |      predicate ((x) => x.fizz)
           |    }
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read
           |  }
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|role Foo {
           |  privileges Bar {
           |    read
           |  }
           |}
           |""".stripMargin
    )
  }

  "Annotations" should "be added to schema items" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(Bar) collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(Bar)
           |collection Foo {}
           |""".stripMargin
    )
  }

  it should "preserve doc comments when adding annotations" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|// This is the collection foo
           |/* before collection Foo */ collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(Bar) collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|// This is the collection foo
           |/* before collection Foo */
           |@alias(Bar)
           |collection Foo {}
           |""".stripMargin
    )
  }

  it should "add multiple annotations" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(bar) @role(baz) function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@role(baz)
           |@alias(bar)
           |function foo() { 42 }
           |""".stripMargin
    )
  }

  it should "patch annotation config value" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|@alias(Bar) collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(Baz) collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(Baz) collection Foo {}
           |""".stripMargin
    )
  }

  it should "preserve comments when pating annotation config value" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|/* Before ann */ @alias(Bar) /* After ann */
           |collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@alias(Baz) collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|/* Before ann */ @alias(Baz) /* After ann */
           |collection Foo {}
           |""".stripMargin
    )
  }

  it should "remove an annotation" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|@alias(Bar) collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )
  }

  it should "preserve comments when removing an annotation" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|/* Before ann */ @alias(Bar) /* After ann */
           |collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|/* Before ann */ /* After ann */
           |collection Foo {}
           |""".stripMargin
    )
  }

  "Functions" should "be added to main file if src is unknown" in {
    patch(
      Src.SourceFile("main.fsl") -> ""
    )(
      Src.Null ->
        """|function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() {
           |  42
           |}
           |""".stripMargin
    )
  }

  it should "be added to declared src if known" in {
    patch(
      Src.SourceFile("main.fsl") -> ""
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() {
           |  42
           |}
           |""".stripMargin
    )
  }

  it should "patch annotation addition" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@role(Bar) function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|@role(Bar)
           |function foo() { 42 }
           |""".stripMargin
    )
  }

  it should "patch signature changes" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo(x: Int): Int { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo(x: Int): Int { 42 }
           |""".stripMargin
    )

    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo(x: Int): Int { x }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo(x) { x }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo(x) { x }
           |""".stripMargin
    )
  }

  it should "preserve comments when patching signature changes" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo /* before sig */ () /* after sig */ { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo(x: Int): Int { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo /* before sig */ (x: Int): Int /* after sig */ { 42 }
           |""".stripMargin
    )
  }

  it should "patch body changes" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 44 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() {
           |  44
           |}
           |""".stripMargin
    )
  }

  it should "preserve comments when patching body changes" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|function foo() /* before body */ { 42 } /* after body */
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() { 44 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function foo() /* before body */ {
           |  44
           |} /* after body */
           |""".stripMargin
    )
  }

  it should "support renaming a function" in {
    renames = Map((SchemaItem.Kind.Function -> "bar") -> "foo")
    patch(
      Src.SourceFile("main.fsl") ->
        """|function /* before */ foo /* after */ () { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function bar() { 42 }
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|function /* before */ bar /* after */ () { 42 }
           |""".stripMargin
    )
  }

  it should "patch semicolons correctly" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10;
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") -> "collection Foo {}"
    )(
      // Note that the trailing semicolon was removed.
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |}
           |""".stripMargin
    )

    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  history_days 10 /* inline comment */; /*
           |    I am a block comment
           |  */; // semicolon 1
           |  ; // semicolon 2
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") -> "collection Foo {}"
    )(
      // Block comments are considered part of the schema item, and line comments are
      // not, resulting in the following change:
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  // semicolon 1
           |  ; // semicolon 2
           |}
           |""".stripMargin
    )
  }

  it should "patch removing a default from a field" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  foo: String = ""
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  foo: String
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Foo {
           |  foo: String
           |}
           |""".stripMargin
    )
  }

  it should "patch generate duplicate items" in {
    val src = Src.SourceFile("main")

    val content =
      """
        |role foo {
        |  privileges Agents { create }
        |}
        |""".stripMargin

    val before = Parser.schemaItems(content, src) getOrElse Seq.empty

    val privilege = Member.Named(
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
      List(SchemaItem
        .Role(Name("foo", Span.Null), Seq(privilege, privilege), span = Span.Null)),
      Map.empty)

    val patched = SchemaPatch.applyTo(src, Map(src -> content), diffs)

    patched(src) shouldBe
      """
        |role foo {
        |  privileges Agents {
        |    create
        |  }
        |  privileges Agents {
        |    create
        |  }
        |}
        |""".stripMargin
  }

  "Complex schema" should "patch multiple changes at once" in {
    patch(
      Src.SourceFile("main.fsl") ->
        """|/** Foo */
           |@alias(Buzz) collection Foo {
           |  ttl_days 5
           |}
           |
           |/** Baz */
           |collection Baz {}
           |
           |/** Bar */
           |collection Bar {}
           |""".stripMargin,
      Src.SourceFile("security.fsl") ->
        """|role User {
           |  privileges Foo {
           |    read
           |  }
           |}
           |
           |/** securityCheck */
           |function securityCheck(x) {
           |  true
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|collection Fizz {}
           |
           |@alias(Baz) collection Bar {
           |  history_days 44
           |  ttl_days 5
           |}
           |
           |collection Foo {
           |  history_days 42
           |}
           |""".stripMargin,
      Src.SourceFile("security.fsl") ->
        """|access provider AP {
           |  issuer "issuer"
           |  jwks_uri "url"
           |}
           |
           |role User {
           |  privileges Foo {
           |    write {
           |      predicate ((x) => check(x))
           |    }
           |    read
           |  }
           |}
           |
           |@alias(check) @role(server)
           |function securityCheck(x: Any): Boolean {
           |  x.isUser
           |}
           |""".stripMargin
    )(
      Src.SourceFile("main.fsl") ->
        """|/** Foo */
           |collection Foo {
           |  history_days 42
           |}
           |
           |/** Bar */
           |@alias(Baz)
           |collection Bar {
           |  history_days 44
           |  ttl_days 5
           |}
           |
           |collection Fizz {
           |}
           |""".stripMargin,
      Src.SourceFile("security.fsl") ->
        """|role User {
           |  privileges Foo {
           |    read
           |    write {
           |      predicate ((x) => check(x))
           |    }
           |  }
           |}
           |
           |/** securityCheck */
           |@role(server)
           |@alias(check)
           |function securityCheck(x: Any): Boolean {
           |  x.isUser
           |}
           |
           |access provider AP {
           |  issuer "issuer"
           |  jwks_uri "url"
           |}
           |""".stripMargin
    )
  }

  private def patch(before: (Src.Id, String)*)(after: (Src.Id, String)*)(
    expected: (Src, String)*)(implicit pos: Position) = {

    def parse(srcs: Seq[(Src.Id, String)]) = {
      val items = Seq.newBuilder[SchemaItem]
      srcs foreach { case (src, fsl) =>
        Parser.schemaItems(fsl, src) match {
          case Result.Ok(it) => items ++= it
          case Result.Err(errs) =>
            errs foreach { err =>
              info(err.renderWithSource(Map(src -> fsl)))
            }
            fail(s"fail to parser $srcs")
        }
      }
      items.result()
    }

    val beforeItems = parse(before)
    val afterItems = parse(after)
    val diffs = SchemaDiff.diffItems(beforeItems, afterItems, renames)

    val patched =
      SchemaPatch.applyTo(
        Src.SourceFile("main.fsl"),
        before.toMap,
        diffs
      )

    val expectedMap = expected.toMap
    patched.keySet should contain theSameElementsAs expectedMap.keySet

    patched foreachEntry { (src, patchedSrc) =>
      val expectedSrc = expectedMap(src)
      if (patchedSrc != expectedSrc) {
        info(
          s"""|* Src: $src
              |* Expected:
              |$expectedSrc
              |
              |* Actual:
              |$patchedSrc
              |
              |* Diff:
              |${TextUtil.diff(patchedSrc, expectedSrc)}
              |""".stripMargin
        )
        fail("expected src is different than patched source")
      }
    }

    // Ensure patched srcs are valid.
    parse(
      patched.view.map {
        case (src: Src.Id, fsl) => (src, fsl)
        case (src, _)           => fail(s"expected Src.Id: $src")
      }.toSeq
    )
  }
}
