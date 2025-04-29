package fql.test

import fql.analyzer.{ CompletionItem, CompletionKind, QueryContext }
import fql.ast.Span

class QueryCompletionSpec extends AnalyzerSpec {
  val ctx = QueryContext(globals, shapes)

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
  val TopLevel = Modules ++ Keywords

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

  // this test validates the actual spans and such. its much more complete, but much
  // more verbose, so there will only be a couple of these.
  "A context" should "complete and set replaceText/retrigger correctly" in {
    def KeywordItems(span: Span) = Keywords.map { kw =>
      CompletionItem(
        label = kw,
        detail = "A builtin keyword",
        kind = CompletionKind.Keyword,
        span = span,
        replaceText = kw,
        newCursor = kw.length,
        retrigger = false
      )
    }
    def ModuleItems(span: Span) = Modules.map { module =>
      CompletionItem(
        label = module,
        detail = module match {
          case "Time"       => "TimeModule"
          case "Math"       => "MathModule"
          case "User"       => "UserCollection"
          case "desc"       => "(A => Any) => { desc: A => Any }"
          case "String"     => "StringModule"
          case "Set"        => "SetModule"
          case "timeOrStr"  => "Time | String"
          case "Function"   => "FunctionCollection"
          case "Collection" => "CollectionCollection"
        },
        kind = CompletionKind.Module,
        span = span,
        replaceText = if (module == "desc") module + "()" else module,
        newCursor = if (module == "desc") module.length + 1 else module.length,
        retrigger = module == "desc",
        snippet = if (module == "desc") {
          Some(s"desc($${1:arg0} => $${2:value})$${0}")
        } else {
          None
        }
      )
    }
    def TopLevel(span: Span): Seq[CompletionItem] =
      ModuleItems(span) ++ KeywordItems(span)

    completeItems("|") shouldBe TopLevel(Span(0, 0, ctx.src))
    completeItems("T|") shouldBe TopLevel(Span(0, 1, ctx.src))
    completeItems("T|ime") shouldBe TopLevel(Span(0, 4, ctx.src))

    completeItems("Time.n|").find(_.label == "now").get shouldBe CompletionItem(
      label = "now",
      detail = "() => Time",
      kind = CompletionKind.Function,
      // span should cover the entire identifier
      span = Span(5, 6, ctx.src),
      replaceText = "now()",
      // cursor should be put inside the ()
      newCursor = 9,
      retrigger = true,
      snippet = Some(s"now()$${0}")
    )
  }

  it should "complete top level" in {
    complete("|") shouldBe TopLevel

    complete("|T") shouldBe TopLevel
    complete("T|") shouldBe TopLevel

    complete("|Set") shouldBe TopLevel
    complete("S|et") shouldBe TopLevel
    complete("Se|t") shouldBe TopLevel
    complete("Set|") shouldBe TopLevel
  }

  it should "complete fields" in {
    complete("Time|.now") shouldBe TopLevel
    complete("Time.|now") shouldBe Seq("fromString", "now")
    complete("Time.n|ow") shouldBe Seq("fromString", "now")
    complete("Time.no|w") shouldBe Seq("fromString", "now")
    complete("Time.now|") shouldBe Seq("fromString", "now")
  }

  it should "complete builtin functions" in {
    completeItems("des|") should contain(
      CompletionItem(
        label = "desc",
        detail = "(A => Any) => { desc: A => Any }",
        kind = CompletionKind.Module,
        // span should cover the entire identifier
        span = Span(0, 3, ctx.src),
        replaceText = "desc()",
        // cursor should be put inside the ()
        newCursor = 5,
        retrigger = true,
        snippet = Some(s"desc($${1:arg0} => $${2:value})$${0}")
      ))
  }

  it should "complete calls" in {
    complete("|Time()") shouldBe TopLevel
    complete("T|ime()") shouldBe TopLevel
    complete("Ti|me()") shouldBe TopLevel
    complete("Tim|e()") shouldBe TopLevel
    complete("Time|()") shouldBe TopLevel
    complete("Time(|)") shouldBe TopLevel

    complete("Time|.now()") shouldBe TopLevel
    complete("Time.|now()") shouldBe Seq("fromString", "now")
    complete("Time.n|ow()") shouldBe Seq("fromString", "now")
    complete("Time.no|w()") shouldBe Seq("fromString", "now")
    complete("Time.now|()") shouldBe Seq("fromString", "now")
    complete("Time.now(|)") shouldBe TopLevel
  }

  it should "complete fields on result of calls" in {
    complete("Time.now(|).year") shouldBe TopLevel
    complete("Time.now()|.year") shouldBe Seq.empty
    complete("Time.now().|year") shouldBe Seq("add", "month", "toString", "year")
    complete("Time.now().y|ear") shouldBe Seq("add", "month", "toString", "year")
    complete("Time.now().ye|ar") shouldBe Seq("add", "month", "toString", "year")
  }

  it should "complete with bang" in {
    complete("|Time!()") shouldBe TopLevel
    complete("T|ime!()") shouldBe TopLevel
    complete("Ti|me!()") shouldBe TopLevel
    complete("Tim|e!()") shouldBe TopLevel
    complete("Time|!()") shouldBe TopLevel
    complete("Time!|()") shouldBe Seq.empty
    complete("Time!(|)") shouldBe TopLevel

    complete("Time|.now!()") shouldBe TopLevel
    complete("Time.|now!()") shouldBe Seq("fromString", "now")
    complete("Time.n|ow!()") shouldBe Seq("fromString", "now")
    complete("Time.no|w!()") shouldBe Seq("fromString", "now")
    complete("Time.now|!()") shouldBe Seq("fromString", "now")
    complete("Time.now!|()") shouldBe Seq.empty
    complete("Time.now!(|)") shouldBe TopLevel
  }

  it should "complete in parens" in {
    complete("(|Time.now())") shouldBe TopLevel
    complete("(T|ime.now())") shouldBe TopLevel
    complete("(Ti|me.now())") shouldBe TopLevel

    complete("(3,| Time.now())") shouldBe TopLevel
    complete("(3, |Time.now())") shouldBe TopLevel
    complete("(3, T|ime.now())") shouldBe TopLevel
    complete("(3, Ti|me.now())") shouldBe TopLevel
  }

  it should "complete in literals" in {
    complete("|true") shouldBe TopLevel
    complete("t|rue") shouldBe TopLevel
    complete("tr|ue") shouldBe TopLevel
    complete("tru|e") shouldBe TopLevel
    complete("true|") shouldBe TopLevel

    complete("|false") shouldBe TopLevel
    complete("f|alse") shouldBe TopLevel
    complete("fa|lse") shouldBe TopLevel
    complete("fal|se") shouldBe TopLevel
    complete("fals|e") shouldBe TopLevel
    complete("false|") shouldBe TopLevel

    complete("|null") shouldBe TopLevel
    complete("n|ull") shouldBe TopLevel
    complete("nu|ll") shouldBe TopLevel
    complete("nul|l") shouldBe TopLevel
    complete("null|") shouldBe TopLevel
  }

  it should "complete if statements" in {
    val boolPrioritized =
      Seq("false", "true") ++ (TopLevel diff Seq("false", "true", "null")) ++ Seq(
        "null")

    complete("|if (true) Time.now()") shouldBe TopLevel
    complete("i|f (true) Time.now()") shouldBe TopLevel
    complete("if| (true) Time.now()") shouldBe TopLevel
    complete("if |(true) Time.now()") shouldBe TopLevel
    // true/false get prioritized here
    complete("if (|true) Time.now()") shouldBe boolPrioritized
    complete("if (t|rue) Time.now()") shouldBe boolPrioritized
    complete("if (tr|ue) Time.now()") shouldBe boolPrioritized
    complete("if (tru|e) Time.now()") shouldBe boolPrioritized
    complete("if (true|) Time.now()") shouldBe boolPrioritized
    complete("if (true)| Time.now()") shouldBe TopLevel
    complete("if (true) |Time.now()") shouldBe TopLevel
    complete("if (true) T|ime.now()") shouldBe TopLevel
    complete("if (true) Ti|me.now()") shouldBe TopLevel
  }

  it should "complete if else statements" in {
    val topLevelWithElse =
      (TopLevel diff Seq("if", "let")) ++ Seq("else", "if", "let")
    complete("if (true) Time.now() |") shouldBe topLevelWithElse

    complete("if (true) Time.now()| else Time.now()") shouldBe Seq.empty
    complete("if (true) Time.now() |else Time.now()") shouldBe topLevelWithElse
    complete("if (true) Time.now() e|lse Time.now()") shouldBe topLevelWithElse
    complete("if (true) Time.now() el|se Time.now()") shouldBe topLevelWithElse
    complete("if (true) Time.now() els|e Time.now()") shouldBe topLevelWithElse
    complete("if (true) Time.now() else| Time.now()") shouldBe topLevelWithElse
    complete("if (true) Time.now() else |Time.now()") shouldBe TopLevel
    complete("if (true) Time.now() else T|ime.now()") shouldBe TopLevel
  }

  it should "complete fields of literals" in {
    complete("2.|") shouldBe Seq("toString")
    complete("2.0.|") shouldBe Seq("toString")

    complete("null.|") shouldBe Seq.empty
    complete("true.|") shouldBe Seq("toString")
    complete("false.|") shouldBe Seq("toString")

    complete("[1, 2].|") shouldBe Seq("indexOf", "take")

    complete("'hello'.|") shouldBe Seq("parseInt", "toString")
    complete("\"hello #{2 + 3}\".|") shouldBe Seq("parseInt", "toString")
  }

  it should "complete within string templates" in {
    complete("\"hello| #{T}\"") shouldBe Seq.empty
    complete("\"hello |#{T}\"") shouldBe Seq.empty
    complete("\"hello #|{T}\"") shouldBe Seq.empty
    complete("\"hello #{|T}\"") shouldBe TopLevel
    complete("\"hello #{T|}\"") shouldBe TopLevel
    complete("\"hello #{T}|\"") shouldBe Seq.empty

    complete("\"hello #{Time.|}\"") shouldBe Seq("fromString", "now")
  }

  it should "complete in blocks outside of span" in {
    complete("Time. |") shouldBe Seq("fromString", "now")
    // this is a weird one. top level is better than nothing I guess
    complete("Time.n |") shouldBe TopLevel

    completeItems("Time. |") shouldBe Seq(
      CompletionItem(
        label = "fromString",
        detail = "String => Time",
        kind = CompletionKind.Function,
        span = Span(6, 6, ctx.src),
        newCursor = 17,
        replaceText = "fromString()",
        retrigger = true,
        snippet = Some(s"fromString($${1:arg0})$${0}")
      ),
      CompletionItem(
        label = "now",
        detail = "() => Time",
        kind = CompletionKind.Function,
        span = Span(6, 6, ctx.src),
        newCursor = 10,
        replaceText = "now()",
        retrigger = true,
        snippet = Some(s"now()$${0}")
      )
    )
  }

  it should "complete local variables" in {
    complete("let foo = 2; |foo") shouldBe Seq("foo") ++ TopLevel
    complete("let foo = 2; f|oo") shouldBe Seq("foo") ++ TopLevel
    complete("let foo = 2; fo|o") shouldBe Seq("foo") ++ TopLevel
    complete("let foo = 2; foo|") shouldBe Seq("foo") ++ TopLevel

    complete("let foo = Time.now(); foo.|") shouldBe Seq(
      "add",
      "month",
      "toString",
      "year")
  }

  it should "complete local variable static types" in {
    completeItems(
      "let foo: (myArg: Number) => {} = (a) => {}; foo|").head shouldBe CompletionItem(
      label = "foo",
      detail = "(myArg: Number) => {}",
      kind = CompletionKind.Variable,
      span = Span(44, 47, ctx.src),
      newCursor = 48,
      replaceText = "foo()",
      retrigger = true,
      snippet = Some(s"foo($${1:myArg})$${0}")
    )
  }

  it should "handles nested local variables correctly" in {
    complete("{ let foo = 2 }; f|") shouldBe TopLevel
    complete("let foo = Time.now(); 2; f|") shouldBe Seq("foo") ++ TopLevel
    complete("let foo = Time.now(); let foo = 2; f|") shouldBe Seq("foo") ++ TopLevel
    complete("let foo = Time.now(); { let foo = 2; 0 }; f|") shouldBe Seq(
      "foo") ++ TopLevel
    complete("let bar = Time.now(); { let baz = 2; 0 }; b|") shouldBe Seq(
      "bar") ++ TopLevel

    complete("let bar = Time.now(); { let baz = 2; b| }; bar") shouldBe Seq(
      "bar",
      "baz") ++ TopLevel
  }

  it should "allow let statements at the end of a block" in {
    complete("let foo = Time.|") shouldBe Seq("fromString", "now")
  }

  it should "complete within object literal" in {
    // this could be an empty block, so we want top level completions.
    complete("{|}") shouldBe TopLevel
    complete("{ | }") shouldBe TopLevel

    // this is not parsable, so fallback to top level.
    // FIXME: parser should not fail in non-strict mode here.
    complete("{| a: }") shouldBe TopLevel
    complete("{ |a: }") shouldBe TopLevel
    complete("{ a|: }") shouldBe TopLevel
    complete("{ a:| }") shouldBe TopLevel
    complete("{ a: |}") shouldBe TopLevel

    // this is parsed as a block, so we shouldn't complete fields.
    complete("{| a: 2 }") shouldBe Seq.empty
    complete("{ |a: 2 }") shouldBe Seq.empty
    complete("{ a|: 2 }") shouldBe Seq.empty
    complete("{ a:| 2 }") shouldBe TopLevel
    complete("{ a: |2 }") shouldBe Seq.empty
    complete("{ a: 2| }") shouldBe Seq.empty
    complete("{ a: 2 |}") shouldBe Seq.empty

    complete("{| a: Time.now() }") shouldBe Seq.empty
    complete("{ |a: Time.now() }") shouldBe Seq.empty
    complete("{ a|: Time.now() }") shouldBe Seq.empty
    complete("{ a:| Time.now() }") shouldBe TopLevel
    complete("{ a: |Time.now() }") shouldBe TopLevel
    complete("{ a: T|ime.now() }") shouldBe TopLevel
  }

  it should "complete projections" in {
    // projection only works on objects
    complete("Time.now() { | }") shouldBe Seq.empty

    complete("{ a: 2 } { | }") shouldBe Seq("a")
    complete("{ a: 2, b: 3 } { | }") shouldBe Seq("a", "b")

    complete("{ a: 2, b: 3 } { |a: .a }") shouldBe Seq("a", "b")
    complete("{ a: 2, b: 3 } { a|: .a }") shouldBe Seq("a", "b")
    complete("{ a: 2, b: 3 } { a:| .a }") shouldBe TopLevel
    complete("{ a: 2, b: 3 } { a: |.a }") shouldBe TopLevel
    complete("{ a: 2, b: 3 } { a: .|a }") shouldBe Seq("a", "b")
    complete("{ a: 2, b: 3 } { a: .a| }") shouldBe Seq("a", "b")

    complete("{ a: 2, b: 3 } { a: |Time.now() }") shouldBe TopLevel
    complete("{ a: 2, b: 3 } { a: T|ime.now() }") shouldBe TopLevel
    complete("{ a: 2, b: 3 } { a: Ti|me.now() }") shouldBe TopLevel

    complete("({ a: 2, b: 3 } { a, c: 3 }) { | }") shouldBe Seq("a", "c")
  }

  it should "complete object fields" in {
    completeItems("{ a: 3 }.|") shouldBe Seq(
      CompletionItem(
        label = "a",
        detail = "3",
        kind = CompletionKind.Field,
        span = Span(9, 9, ctx.src),
        newCursor = 10,
        replaceText = "a",
        retrigger = false
      ))

    completeItems("let foo = { a: 3 }; foo.|") shouldBe Seq(
      CompletionItem(
        label = "a",
        detail = "3",
        kind = CompletionKind.Field,
        span = Span(24, 24, ctx.src),
        newCursor = 25,
        replaceText = "a",
        retrigger = false
      ))
  }

  it should "complete lambdas" in {
    complete("() => |") shouldBe TopLevel
    complete("a => |") shouldBe Seq("a") ++ TopLevel
    complete("(a, b) => |") shouldBe Seq("a", "b") ++ TopLevel
    complete("(a, _) => |") shouldBe Seq("a") ++ TopLevel

    // a is Never
    complete("a => { a.| }") shouldBe Seq.empty
    // a is { foo: Any, ... }. But, because those are use bounds, we don't provide
    // any completions. This is both due to the fact that `foo` was defined as a
    // field implicitly, and that auto-completing fields here is somewhat difficult
    // to implement. If we did show completions, `faunaRulezz` would also pop up
    // here, which we definitely don't want.
    //
    // Once we're able to put types into lambda arguments (like `(a: Number) =>
    // ...`), this will be possible to auto-complete.
    complete("a => { a.foo; a.| }") shouldBe Seq.empty
  }

  it should "not complete in lambda args" in {
    complete("|a => a") shouldBe Seq.empty
    complete("a| => a") shouldBe Seq.empty
  }

  it should "complete with invalid syntax" in {
    complete("|Time.") shouldBe TopLevel
    complete("T|ime.") shouldBe TopLevel
    complete("Ti|me.") shouldBe TopLevel
    complete("Tim|e.") shouldBe TopLevel
    complete("Time|.") shouldBe TopLevel

    // This is the intellij trick: we insert the string `faunaRulezz` at the cursor,
    // which makes this valid syntax, and therefore we're able to auto-complete this.
    complete("Time.|") shouldBe Seq("fromString", "now")
  }

  it should "complete with generics and overloads" in {
    completeItems("[1, 2].|") shouldBe Seq(
      CompletionItem(
        label = "indexOf",
        // this signature should get instantiated
        detail = "(1 | 2 => Number | Null) & ((1 | 2, Number) => Number | Null)",
        kind = CompletionKind.Function,
        span = Span(7, 7, ctx.src),
        newCursor = 15,
        replaceText = "indexOf()",
        retrigger = true,
        snippet = Some(s"indexOf()$${0}")
      ),
      CompletionItem(
        label = "take",
        // this signature should get instantiated
        detail = "(amount: Number) => Array<1 | 2>",
        kind = CompletionKind.Function,
        span = Span(7, 7, ctx.src),
        newCursor = 12,
        replaceText = "take()",
        retrigger = true,
        snippet = Some(s"take($${1:amount})$${0}")
      )
    )
  }

  it should "complete args of generic functions" in pendingUntilFixed {
    // FIXME: Typeschems don't instantiate correctly, as the `indexOf` doesn't
    // know that its type param is `1 | 2`
    complete("[1, 2].indexOf(|)") shouldBe Seq.empty
    ()
  }

  it should "complete args of overloaded functions" in {
    // integer and string should get prioritized
    complete(s"""|let a = true
                 |let bb = 'hi'
                 |let cc = 3
                 |Math.gimmeIntOrStrStr(|)
                 |""".stripMargin).slice(0, 3) shouldBe Seq("bb", "cc", "a")

    // string should get prioritized
    complete(s"""|let a = true
                 |let bb = 'hi'
                 |let cc = 3
                 |Math.gimmeIntOrStrStr(3, |)
                 |""".stripMargin).slice(0, 3) shouldBe Seq("bb", "a", "cc")
  }

  it should "complete unions generics" in {
    // Complete all variants of the union, not just the fields that apply to both.
    complete("timeOrStr.|") shouldBe Seq(
      "add",
      "month",
      "parseInt",
      "toString",
      "year")
  }

  it should "complete lets" in {
    complete("|let a = 2; a") shouldBe TopLevel
    complete("l|et a = 2; a") shouldBe TopLevel
    complete("le|t a = 2; a") shouldBe TopLevel
    complete("let| a = 2; a") shouldBe TopLevel
    complete("let |a = 2; a") shouldBe Seq.empty
    complete("let a| = 2; a") shouldBe Seq.empty
    // FIXME: need the span of the `=` to make this return Seq.empty (which it
    // should)
    complete("let a |= 2; a") shouldBe TopLevel
    complete("let a =| 2; a") shouldBe TopLevel
    complete("let a = |2; a") shouldBe Seq.empty
    complete("let a = 2|; a") shouldBe Seq.empty
    complete("let a = 2;| a") shouldBe TopLevel
    complete("let a = 2; |a") shouldBe Seq("a") ++ TopLevel
    complete("let a = 2; a|") shouldBe Seq("a") ++ TopLevel
  }

  it should "complete lets with a typeexpr" in {
    complete("|let a: Number = 2; a") shouldBe TopLevel
    complete("l|et a: Number = 2; a") shouldBe TopLevel
    complete("le|t a: Number = 2; a") shouldBe TopLevel
    complete("let| a: Number = 2; a") shouldBe TopLevel
    complete("let |a: Number = 2; a") shouldBe Seq.empty
    complete("let a|: Number = 2; a") shouldBe Seq.empty
    complete("let a:| Number = 2; a") shouldBe Types
    complete("let a: |Number = 2; a") shouldBe Types
    complete("let a: N|umber = 2; a") shouldBe Types
    complete("let a: Nu|mber = 2; a") shouldBe Types
    complete("let a: Num|ber = 2; a") shouldBe Types
    complete("let a: Numb|er = 2; a") shouldBe Types
    complete("let a: Numbe|r = 2; a") shouldBe Types
    complete("let a: Number| = 2; a") shouldBe Types
    complete("let a: Number |= 2; a") shouldBe TopLevel
  }

  "Type Exprs" should "complete singletons" in {
    complete("let a: |3 = 2; a") shouldBe Seq.empty
    complete("let a: 3| = 2; a") shouldBe Seq.empty

    complete("let a: |'a' = 2; a") shouldBe Seq.empty
    complete("let a: '|a' = 2; a") shouldBe Seq.empty

    complete("let a: |true = 2; a") shouldBe Types
    complete("let a: t|rue = 2; a") shouldBe Types

    complete("let a: |false = 2; a") shouldBe Types
    complete("let a: f|alse = 2; a") shouldBe Types

    complete("let a: |null = 2; a") shouldBe Types
    complete("let a: n|ull = 2; a") shouldBe Types
  }

  it should "complete cons" in {
    complete("let a: |Array<3> = 2; a") shouldBe Types
    complete("let a: A|rray<3> = 2; a") shouldBe Types
    complete("let a: Ar|ray<3> = 2; a") shouldBe Types
    complete("let a: Arr|ay<3> = 2; a") shouldBe Types
    complete("let a: Arra|y<3> = 2; a") shouldBe Types
    complete("let a: Array|<3> = 2; a") shouldBe Types
    complete("let a: Array<|3> = 2; a") shouldBe Seq.empty
    complete("let a: Array<3|> = 2; a") shouldBe Seq.empty
    complete("let a: Array<3>| = 2; a") shouldBe Types

    complete("let a: Array<2|, 3> = 2; a") shouldBe Seq.empty
    complete("let a: Array<2,| 3> = 2; a") shouldBe Types
    complete("let a: Array<2, |3> = 2; a") shouldBe Seq.empty
    complete("let a: Array<2, 3|> = 2; a") shouldBe Seq.empty
  }

  it should "complete objects" in {
    complete("let a: {|} = 2; a") shouldBe Types
    complete("let a: { a: | } = 2; a") shouldBe Types

    complete("let a: { |a: A } = 2; a") shouldBe Seq.empty
    complete("let a: { a|: A } = 2; a") shouldBe Seq.empty
    complete("let a: { a:| A } = 2; a") shouldBe Types
    complete("let a: { a: |A } = 2; a") shouldBe Types
    complete("let a: { a: A| } = 2; a") shouldBe Types

    // FIXME: we don't have the span of the `*` so we can't do any better than this.
    complete("let a: { |*: A } = 2; a") shouldBe Types
    complete("let a: { *|: A } = 2; a") shouldBe Types
    complete("let a: { *:| A } = 2; a") shouldBe Types
    complete("let a: { *: |A } = 2; a") shouldBe Types
    complete("let a: { *: A| } = 2; a") shouldBe Types

    complete("let a: { a: A|, b: B } = 2; a") shouldBe Types
    complete("let a: { a: A,| b: B } = 2; a") shouldBe Types
    complete("let a: { a: A, |b: B } = 2; a") shouldBe Seq.empty
    complete("let a: { a: A, b|: B } = 2; a") shouldBe Seq.empty
    complete("let a: { a: A, b:| B } = 2; a") shouldBe Types
    complete("let a: { a: A, b: |B } = 2; a") shouldBe Types
    complete("let a: { a: A, b: B| } = 2; a") shouldBe Types
  }

  it should "complete interfaces" in {
    complete("let a: { a: |, ... } = 2; a") shouldBe Types

    complete("let a: { |... } = 2; a") shouldBe Seq.empty
    complete("let a: { .|.. } = 2; a") shouldBe Seq.empty
    complete("let a: { ..|. } = 2; a") shouldBe Seq.empty
    complete("let a: { ...| } = 2; a") shouldBe Seq.empty

    complete("let a: { |a: A, ... } = 2; a") shouldBe Seq.empty
    complete("let a: { a|: A, ... } = 2; a") shouldBe Seq.empty
    complete("let a: { a:| A, ... } = 2; a") shouldBe Types
    complete("let a: { a: |A, ... } = 2; a") shouldBe Types
    complete("let a: { a: A|, ... } = 2; a") shouldBe Types
  }

  it should "complete tuples" in {
    complete("let a: [|] = 2; a") shouldBe Types

    complete("let a: [|A, B] = 2; a") shouldBe Types
    complete("let a: [A|, B] = 2; a") shouldBe Types
    complete("let a: [A,| B] = 2; a") shouldBe Types
    complete("let a: [A, |B] = 2; a") shouldBe Types
    complete("let a: [A, B|] = 2; a") shouldBe Types
  }

  it should "complete lambdas" in {
    complete("let a: (|A, B) => 3 = 2; a") shouldBe Types
    complete("let a: (A|, B) => 3 = 2; a") shouldBe Types
    complete("let a: (A,| B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, |B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, B|) => 3 = 2; a") shouldBe Types
    complete("let a: (A, B)| => 3 = 2; a") shouldBe Types
    complete("let a: (A, B) |=> 3 = 2; a") shouldBe Types
    complete("let a: (A, B) =|> 3 = 2; a") shouldBe Types
    complete("let a: (A, B) =>| 3 = 2; a") shouldBe Types
    complete("let a: (A, B) => |3 = 2; a") shouldBe Seq.empty
    complete("let a: (A, B) => 3| = 2; a") shouldBe Seq.empty

    complete("let a: (|A, ...B) => 3 = 2; a") shouldBe Types
    complete("let a: (A|, ...B) => 3 = 2; a") shouldBe Types
    complete("let a: (A,| ...B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, |...B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, .|..B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, ..|.B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, ...|B) => 3 = 2; a") shouldBe Types
    complete("let a: (A, ...B|) => 3 = 2; a") shouldBe Types
    complete("let a: (A, ...B)| => 3 = 2; a") shouldBe Types
  }

  it should "prioritize completions" in {
    // for some reason the keywords get swapped around when the expected type isn't
    // Type.Any. Not sure why this is.
    val topLevelPrioritized =
      Modules ++ Seq("at", "false", "if", "let", "null", "true")

    complete("let a = 2; let b = 'hi'; |") shouldBe Seq("a", "b") ++ TopLevel
    // b should show up first, because it has the right type.
    complete("let a = 2; let b = 'hi'; Time.fromString(|)") shouldBe Seq(
      "b",
      "a") ++ topLevelPrioritized

    complete("let a = 2; let b = 'hi'; let c = 3; Time.now().add(|)") shouldBe Seq(
      "a",
      "c",
      "b") ++ topLevelPrioritized

    complete(
      "let a = 2; let b = 'hi'; let c = 3; Time.now().add(1, |)") shouldBe Seq(
      "b",
      "a",
      "c") ++ topLevelPrioritized

    complete("Time.now().|") shouldBe Seq("add", "month", "toString", "year")
    // toString should show up first, because it returns the correct type.
    complete("Time.fromString(Time.now().|)") shouldBe Seq(
      "toString",
      "add",
      "month",
      "year")
  }

  it should "prioritize completions in if" in {
    complete("if (|) 2 else 3") shouldBe Seq("false", "true") ++ (TopLevel diff Seq(
      "true",
      "false",
      "null")) ++ Seq("null")
  }

  it should "show fields of expected object" in {
    val createFields =
      Seq("name", "alias", "constraints", "data", "history_days", "indexes")

    // looks like an empty object/block, so normally we'd fall back to block
    // completion
    complete("Collection.create({ | })") shouldBe createFields

    // this is an Expr.Block, so it needs a special case to complete fields
    complete("Collection.create({ n| })") shouldBe createFields

    // "normal" field completion
    complete("Collection.create({ n|: 2 })") shouldBe createFields

    // field names should be completed when typing new fields
    complete("Collection.create({ n: 2, | })") shouldBe createFields
    complete("Collection.create({ n: 2, n| })") shouldBe createFields

    // things should still work with newlines
    complete(
      """|Collection.create({
         |  n: 2,
         |  n|
         |})""".stripMargin
    ) shouldBe createFields

    // fields that are already present should not be suggested
    complete(
      "Collection.create({ name: 2, nam|: 3 })") shouldBe (createFields diff Seq(
      "name"))
    complete("Collection.create({ name: 2, | })") shouldBe (createFields diff Seq(
      "name"))

    // fields you are typing are "present" but should be suggested
    complete("Collection.create({ name|: 2 })") shouldBe createFields

    completeItems("Collection.create({ n| })")
      .find(_.label == "name")
      .get shouldBe CompletionItem(
      label = "name",
      detail = "String",
      span = Span(20, 21, ctx.src),
      replaceText = "name: ",
      newCursor = 26,
      kind = CompletionKind.Property,
      retrigger = true
    )
  }

  it should "not make a snippet for object arguments" in {
    completeItems("Collection.|")
      .find(_.label == "create")
      .get
      .snippet shouldBe Some(s"create($${1:arg0})$${0}")

    completeItems("Function.|")
      .find(_.label == "create")
      .get
      .snippet shouldBe Some(s"create($${1:data})$${0}")
  }

  it should "make a snippet for lambdas" in {
    val item0 = completeItems("Collection.all().fold|")(0)
    item0.snippet shouldBe Some(
      s"fold($${1:seed}, ($${2:arg0}, $${3:arg1}) => $${4:value})$${0}".stripMargin)
  }

  it should "complete aliases in let" in {
    complete("User.byId('0')!.|") shouldBe Seq("exists", "my_computed_field")
  }

  it should "complete nested fields correctly" in {
    complete("User.create({ nested: { | } })") shouldBe Seq("foo")
  }

  it should "complete nested fields with wildcards correctly" in {
    complete("Collection.create({ indexes: { byName: { | } } })") shouldBe Seq(
      "queryable",
      "terms",
      "values")
  }
}
