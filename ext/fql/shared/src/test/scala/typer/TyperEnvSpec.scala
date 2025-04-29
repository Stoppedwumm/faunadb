package fql.test

import fql.ast.{ Span, Src }
import fql.ast.display._
import fql.error.TypeError
import fql.typer.{ Type, TypeScheme, TypeShape, Typer }
import scala.collection.immutable.ArraySeq
import scala.util.Random

class TyperEnvSpec extends TypeSpec {

  "Typer.EnvTyper" should "check environment" in {
    val env = Typer.newEnvBuilder

    val res1 =
      env.addGlobal("plusone", None, Some(parseExpr("x => ident2(succ(ident2(x)))")))
    val res2 =
      env.addGlobal("plustwo", None, Some(parseExpr("x => plusone(plusone(x))")))

    val res3 = env.addGlobal("anInt", None, Some(parseExpr("ident2(succ(2))")))
    val res4 = env.addGlobal("ident2", None, Some(parseExpr("x => ident1(x)")))
    val res5 = env.addGlobal("ident1", None, Some(parseExpr("x => x")))

    val res6 = env.addGlobal("ident3", Some(parseTExpr("b => b")), None)

    TestTyper().typeEnv(env).isOk shouldBe true

    res1.get.display shouldEqual "Int => Int"
    res2.get.display shouldEqual "Int => Int"
    res3.get.display shouldEqual "Int"
    res4.get.display shouldEqual "<A> A => A"
    res5.get.display shouldEqual "<A> A => A"
    res6.get.display shouldEqual "<b> b => b"
  }

  it should "type overloads" in {
    val env = Typer.newEnvBuilder

    val res3 =
      env.addGlobal("myBaz", None, Some(parseExpr("myBar(if (true) 1 else true)")))
    val res2 = env.addGlobal("myBar", None, Some(parseExpr("x => myFoo(x)")))
    val res1 = env.addGlobal("myFoo", None, Some(parseExpr("x => intBool(x)")))

    TestTyper().typeEnv(env).isOk shouldBe true

    res1.get.display shouldEqual "(Int => Int) & (Boolean => Boolean)"
    res2.get.display shouldEqual "(Int => Int) & (Boolean => Boolean)"
    res3.get.display shouldEqual "Int | Boolean"
  }

  it should "check open dependency monster" in {
    val exprs = {
      val b = List.newBuilder[(String, String)]
      b += ("id0" -> "x => x")
      for (i <- 1 to 100) b += (s"id$i" -> s"x => id${i - 1}(x)")
      b.result()
    }

    val env = Typer.newEnvBuilder

    val exprs0 = Random.shuffle(exprs)
    val checks = exprs0.map { case (n, e) =>
      env.addGlobal(n, None, Some(parseExpr(e)))
    }

    TestTyper().typeEnv(env).isOk shouldBe true

    checks.foreach(chk => chk.get.display shouldEqual "<A> A => A")
  }

  // open recursion monster
  {
    val Max = 100
    val exprs = {
      val b = List.newBuilder[(String, String)]
      b += ("id0" -> s"x => if (true) succ(x) else id$Max(x)")
      for (i <- 1 to Max) b += (s"id$i" -> s"x => id${i - 1}(x)")
      b.result()
    }

    val exprs0 = Random.shuffle(exprs)

    it should "reject open recursion monster" in {
      val env = Typer.newEnvBuilder
      val checks = exprs0.map { case (n, e) =>
        n -> env.addGlobal(n, None, Some(parseExpr(e)))
      }

      val errs = TestTyper().typeEnv(env).errOrElse(fail("expected error result"))
      errs.size shouldEqual 1

      errs(0) should matchPattern {
        case TypeError("Recursively defined `id0` needs explicit type.", _, _, _) =>
      }

      checks.foreach { case (n, chk) =>
        val expected = if (n == "id0") "Any" else "Any => Any"
        chk.get.display shouldEqual expected
      }
    }

    it should "check open recursion monster" in pendingUntilFixed {
      val env = Typer.newEnvBuilder
      val checks = exprs0.map { case (n, e) =>
        env.addGlobal(n, None, Some(parseExpr(e)))
      }

      TestTyper().typeEnv(env).getOrElse(fail("did not pass"))

      checks.foreach(chk => chk.get.display shouldEqual "Int => Int")
    }
  }

  it should "report errors" in {
    val env = Typer.newEnvBuilder

    val check1 = env.addGlobal("welp", None, Some(parseExpr("x => oopsie(x)")))
    val check2 = env.addGlobal("oopsie", None, Some(parseExpr("x => succc(x)")))
    val check3 = env.addGlobal(
      "poopsie",
      Some(parseTExpr("String => String")),
      Some(parseExpr("x => succ(x)")))

    val errs = TestTyper().typeEnv(env).errOrElse(fail("expected error result"))

    errs.size shouldEqual 2

    errs(0) should matchPattern {
      case TypeError("Unbound variable `succc`", _, Nil, Nil) =>
    }

    errs(1) should matchPattern {
      case TypeError(
            "Type `Int => Int` is not a subtype of `String => String`",
            _,
            _,
            _) =>
    }

    check1.get.display shouldEqual "Any => Any"
    check2.get.display shouldEqual "Any => Any"
    check3.get.display shouldEqual "String => String"
  }

  it should "run arbitrary checks from addTypeExprCheck" in {
    val env = Typer.newEnvBuilder
    val typer = TestTyper()

    // All these fields are on the collection `User`, which is defined in
    // `TestTyper`.
    def addComputedField(name: String, body: String, signature: String) = {
      val expr =
        parseTExpr(signature, Src.Inline(s"*computed_field:$name*", signature))
      val ty = typer.typeTExprType(expr).getOrElse(fail())

      env.addTypeCheck(
        Type.Function(Type.Named("User") -> ty),
        parseExpr(body, Src.Inline(s"*computed_field:$name*", body))
      )
    }

    env.addGlobal("myFoo", None, Some(parseExpr("1")))
    env.addGlobal("myBar", None, Some(parseExpr("true")))
    env.addGlobal("myBaz", None, Some(parseExpr("if (true) 1 else true")))

    addComputedField("valid1", "doc => doc.name", "String")
    addComputedField("valid2", "doc => doc.name + doc.name", "String")
    addComputedField("valid3", "doc => myFoo", "Int")
    addComputedField("valid4", "doc => myBar", "Boolean")
    addComputedField("valid5", "doc => myBaz", "Int | Boolean")
    addComputedField("invalid1", "doc => doc.name - 2", "Int")
    addComputedField("invalid2", "doc => 3", "String")

    val errs = typer.typeEnv(env).errOrElse(fail())

    assertStringsEqual(
      errs.map(_.renderWithSource(Map.empty)).mkString("\n\n"),
      """|error: Type `String` is not a subtype of `Int`
         |at *computed_field:invalid1*:1:1
         |  |
         |1 | doc => doc.name - 2
         |  | ^^^^^^^^^^^^^^^^^^^
         |  |
         |
         |error: Type `Int` is not a subtype of `String`
         |at *computed_field:invalid2*:1:8
         |  |
         |1 | doc => 3
         |  |        ^
         |  |""".stripMargin
    )
  }

  it should "allow types and type shapes to be passed directly" in {
    val env = Typer.newEnvBuilder

    env.addGlobal("myFoo", Some(parseTExpr("Foo")), None)

    env.addTypeShape(
      "Foo",
      TypeShape(
        self = Type.Named("Foo").typescheme,
        fields = Map(
          "hi" -> Type.Int(Span.Null).typescheme
        )
      )
    )

    env.addTypeCheck(Type.Int, parseExpr("myFoo.hi"))

    TestTyper().typeEnv(env).isOk shouldBe true
  }

  "computed fields" should "work" in {
    val typer = TestTyper()
    val env = Typer.newEnvBuilder

    val v = typer.freshVar(Span.Null)(Type.Level.Zero)
    env.addTypeCheck(v, parseExpr("2 * 3"))

    typer.typeEnv(env).isOk shouldBe true
    val tscheme = typer.toTypeSchemeExpr(TypeScheme.Simple(v))
    tscheme.display shouldEqual "Int"
  }

  it should "work with uses" in {
    val typer = TestTyper()
    val env = Typer.newEnvBuilder

    val v = typer.freshVar(Span.Null)(Type.Level.Zero)
    env.addTypeCheck(v, parseExpr("2 * 3"))

    val myRecord = Type.Record("foo" -> v)
    val scheme = TypeScheme.Simple(myRecord)

    env.addTypeShape(
      "MyRecord",
      TypeShape(self = Type.Int.typescheme, alias = Some(scheme)))
    env.addGlobal("aRecord", Some(parseTExpr("MyRecord")), None)

    env.addGlobal("foo", Some(parseTExpr("Int")), Some(parseExpr("aRecord.foo")))

    typer.typeEnv(env).isOk shouldBe true
    val tscheme = typer.toTypeSchemeExpr(scheme)
    tscheme.display shouldEqual "{ foo: Int }"
  }

  it should "work with overloads" in {
    val typer = TestTyper()
    val env = Typer.newEnvBuilder

    // Need to create at var level one so that annealing will work.
    val v = typer.freshVar(Span.Null)(Type.Level.Zero)
    val thunk1 = env.addTypeCheck(
      Type.Function(ArraySeq(None -> Type.Any), None, v, Span.Null),
      parseExpr("x => 2 + 3"))

    val myRecord = Type.Record("foo" -> v)
    val scheme = TypeScheme.Simple(myRecord)

    env.addTypeShape(
      "MyRecord",
      TypeShape(self = Type.Any.typescheme, alias = Some(scheme)))
    env.addGlobal("aRecord", Some(parseTExpr("MyRecord")), None)

    val thunk2 = env.addGlobal("bar", None, Some(parseExpr("aRecord.foo")))

    typer.typeEnv(env).isOk shouldBe true
    thunk1.get.display shouldEqual "Any => Number"
    thunk2.get.display shouldEqual "Number"
  }
}
