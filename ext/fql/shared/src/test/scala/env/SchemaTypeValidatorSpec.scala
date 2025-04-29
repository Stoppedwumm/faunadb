package fql.test

import fql.ast.Src
import fql.env.{
  CollectionUpdate,
  DatabaseSchema,
  FunctionUpdate,
  InternalUpdate,
  SchemaTypeValidator
}
import fql.parser.Parser
import fql.Result
import org.scalactic.source.Position

class SchemaTypeValidatorSpec extends TypeSpec {
  def validateRes(source: String): Result[Seq[InternalUpdate]] = {
    val src = Src.Inline("main.fsl", source)
    for {
      items <- Parser.schemaItems(source, src)
      schema = DatabaseSchema.fromItems(
        items,
        v4Funcs = Seq.empty,
        v4Roles = Seq.empty)
      env <- SchemaTypeValidator.validate(
        StdlibEnv(),
        schema,
        isEnvTypechecked = true)
    } yield env.updates.toSeq
  }

  def validateOk(source: String)(implicit pos: Position): Seq[InternalUpdate] = {
    validateRes(source) match {
      case Result.Ok(updates) => updates
      case Result.Err(err) =>
        val error =
          err.map(_.renderWithSource(Map.empty)).mkString("\n")
        fail(s"unexpected validation failure:\n$error")
    }
  }

  def validateErr(source: String)(implicit pos: Position): String = {
    validateRes(source) match {
      case Result.Ok(_) => fail(s"expected failure, but source passed:\n$source")
      case Result.Err(err) =>
        err.map(_.renderWithSource(Map.empty)).mkString("\n")
    }
  }

  "SchemaTypeValidator" should "validate schema" in {
    validateOk(
      """|collection User {
         |  first: String
         |  last: String
         |  compute name = doc => doc.first + " " + doc.last
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", Some("{ name: String }"))
    )

    validateErr(
      """|collection User {
         |  compute other = _ => 2
         |  compute name = doc => doc.other
         |  index byName { terms [.name] }
         |}""".stripMargin
    ) shouldBe (
      """|error: Computed fields used in indexes or constraints cannot use computed fields
         |at main.fsl:3:29
         |  |
         |3 |   compute name = doc => doc.other
         |  |                             ^^^^^
         |  |""".stripMargin
    )

    validateErr(
      """|collection User {
         |  compute other = _ => 2
         |  compute name = doc => doc.other
         |  index byName { terms [.name] }
         |}""".stripMargin
    ) shouldBe (
      """|error: Computed fields used in indexes or constraints cannot use computed fields
         |at main.fsl:3:29
         |  |
         |3 |   compute name = doc => doc.other
         |  |                             ^^^^^
         |  |""".stripMargin
    )
  }

  it should "check for simple validation errors" in {
    validateErr(
      """|function one() {
         |  1.foo
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `Int` does not have field `foo`
         |at main.fsl:2:5
         |  |
         |2 |   1.foo
         |  |     ^^^
         |  |""".stripMargin
    )
  }

  it should "infer function signatures" in {
    validateOk(
      """|function one(x) {
         |  x + 2
         |}""".stripMargin
    ) shouldBe Seq(
      FunctionUpdate("one", "Number => Number")
    )
  }

  it should "validate explicit signatures" in {
    validateOk(
      """|function one(x: Int): Int {
         |  x * 2
         |}""".stripMargin
    ) shouldBe Seq(
      FunctionUpdate("one", "(x: Int) => Int")
    )

    validateErr(
      """|function one(x: Int): Int {
         |  x.length
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `Int` does not have field `length`
         |at main.fsl:2:5
         |  |
         |2 |   x.length
         |  |     ^^^^^^
         |  |
         |hint: Type `Int` inferred here
         |at main.fsl:1:17
         |  |
         |1 | function one(x: Int): Int {
         |  |                 ^^^
         |  |""".stripMargin
    )
  }

  it should "check for recursive functions" in {
    validateErr(
      """|function fac(x) {
         |  if (x > 0) {
         |    fac(x - 1) * x
         |  } else {
         |    1
         |  }
         |}""".stripMargin
    ) shouldBe (
      """|error: Recursively defined `fac` needs explicit type.
         |at main.fsl:1:1
         |  |
         |1 |   function fac(x) {
         |  |  _^
         |2 | |   if (x > 0) {
         |3 | |     fac(x - 1) * x
         |4 | |   } else {
         |5 | |     1
         |6 | |   }
         |7 | | }
         |  | |_^
         |  |""".stripMargin
    )

    validateOk(
      """|function fac(x: Int): Int {
         |  if (x > 0) {
         |    fac(x - 1) * x
         |  } else {
         |    1
         |  }
         |}""".stripMargin
    ) shouldBe Seq(
      FunctionUpdate("fac", "(x: Int) => Int")
    )
  }

  it should "validate variadics correctly" in {
    validateOk(
      """|function varargs(a: Int, ...x: Int): Int {
         |  a * x.length
         |}
         |
         |function foo() {
         |  varargs(1, 2, 3)
         |}""".stripMargin
    ) shouldBe Seq(
      FunctionUpdate("varargs", "(a: Int, x: ...Int) => Int"),
      FunctionUpdate("foo", "() => Int")
    )
  }

  it should "handle unnamed arguments correctly" in {
    validateOk(
      """|function no_names(_: Int, _: String): Int {
         |  3
         |}
         |
         |function one_named(_: Int, a: String): Int {
         |  no_names(2, a)
         |}""".stripMargin
    ) shouldBe Seq(
      FunctionUpdate("no_names", "(Int, String) => Int"),
      FunctionUpdate("one_named", "(Int, a: String) => Int")
    )

    validateErr(
      """|function cant_access_underscore(_) {
         |  _
         |}""".stripMargin
    ) shouldBe (
      """|error: Unbound variable `_`
         |at main.fsl:2:3
         |  |
         |2 |   _
         |  |   ^
         |  |""".stripMargin
    )
  }

  // This is validated higher up in the stack, but it should really be validated
  // here.
  it should "disallow args with duplicate names (pending)" in {
    // current (broken) behavior
    validateOk(
      """|function dup_name(a, a) {
         |  a
         |}""".stripMargin
    )

    validateOk(
      """|function dup_name(a: Int, a: Int): Int {
         |  a
         |}""".stripMargin
    )

    pendingUntilFixed {
      validateErr(
        """|function dup_name(a, a) {
           |  a
           |}""".stripMargin
      ) shouldNot equal("")

      ()
    }
  }

  it should "validate check constraints" in {
    validateOk(
      """|collection User {
         |  name: String
         |
         |  check foo (doc => doc.name.length > 3)
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None)
    )

    validateErr(
      """|collection User {
         |  name: String
         |
         |  check foo (_ => "hello".foo)
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `String` does not have field `foo`
         |at main.fsl:4:27
         |  |
         |4 |   check foo (_ => "hello".foo)
         |  |                           ^^^
         |  |""".stripMargin
    )
  }

  it should "validate computed fields" in {
    validateOk(
      """|collection User {
         |  name: String
         |
         |  compute foo = doc => doc.name.length > 3
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", Some("{ foo: Boolean }"))
    )

    validateErr(
      """|collection User {
         |  name: String
         |
         |  compute foo = _ => "hello".foo
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `String` does not have field `foo`
         |at main.fsl:4:30
         |  |
         |4 |   compute foo = _ => "hello".foo
         |  |                              ^^^
         |  |""".stripMargin
    )

    validateErr(
      """|collection User {
         |  compute foo: Array<1, 2> = _ => abort(0)
         |}""".stripMargin
    ) shouldBe (
      """|error: Type constructor `Array<>` accepts 1 parameters, received 2
         |at main.fsl:2:16
         |  |
         |2 |   compute foo: Array<1, 2> = _ => abort(0)
         |  |                ^^^^^
         |  |""".stripMargin
    )
  }

  it should "infer across computed fields and functions" in {
    validateOk(
      """|collection User {
         |  first: String
         |  last: String
         |  compute name = doc => doc.first + " " + doc.last
         |}
         |
         |function myUser() {
         |  User.byId(1)!.name
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", Some("{ name: String }")),
      FunctionUpdate("myUser", "() => String"))
  }

  it should "validate defined field signatures" in {
    validateErr(
      """|collection User {
         |  foo: Array<1, 2>
         |}""".stripMargin
    ) shouldBe (
      """|error: Type constructor `Array<>` accepts 1 parameters, received 2
         |at main.fsl:2:8
         |  |
         |2 |   foo: Array<1, 2>
         |  |        ^^^^^
         |  |""".stripMargin
    )
  }

  it should "validate defined field default values" in {
    validateErr(
      """|collection User {
         |  foo: Int = "hi"
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `String` is not a subtype of `Int`
         |at main.fsl:2:14
         |  |
         |2 |   foo: Int = "hi"
         |  |              ^^^^
         |  |""".stripMargin
    )
  }

  it should "ensure defined fields are persistable" in {
    validateErr(
      """|collection User {
         |  foo: () => Int
         |}""".stripMargin
    ) shouldBe (
      """|error: Field `foo` in collection User is not persistable
         |at main.fsl:2:8
         |  |
         |2 |   foo: () => Int
         |  |        ^^^^^^^^^
         |  |
         |cause: Type () => Int is not persistable
         |  |
         |2 |   foo: () => Int
         |  |        ^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow udfs in indexed computed fields" in {
    validateErr(
      """|function one(_) { 1 }
         |
         |collection Foo {
         |  compute a = _ => 1 + one(0)
         |
         |  index byA { terms [.a] }
         |}""".stripMargin
    ) shouldBe (
      """|error: Computed fields used in indexes or constraints cannot call UDFs
         |at main.fsl:4:24
         |  |
         |4 |   compute a = _ => 1 + one(0)
         |  |                        ^^^
         |  |""".stripMargin
    )
  }

  it should "track paths correctly with square brackets" in {
    validateErr(
      """|function one(_) { 1 }
         |
         |collection Foo {
         |  compute a = _ => 1 + one(0)
         |
         |  index byA { terms [.['a']] }
         |}""".stripMargin
    ) shouldBe (
      """|error: Computed fields used in indexes or constraints cannot call UDFs
         |at main.fsl:4:24
         |  |
         |4 |   compute a = _ => 1 + one(0)
         |  |                        ^^^
         |  |""".stripMargin
    )
  }

  it should "disallow accessing ts field on indexed computed fields" in {
    validateErr(
      """|collection Foo {
         |  compute a = doc => doc.ts
         |
         |  index byA { terms [.a] }
         |}""".stripMargin
    ) shouldBe (
      """|error: Computed fields used in indexes or constraints cannot use a doc's ts field
         |at main.fsl:2:26
         |  |
         |2 |   compute a = doc => doc.ts
         |  |                          ^^
         |  |""".stripMargin
    )
  }

  it should "allow accessing ts field on other objects" in {
    // shadow the `doc` variable just to be sure we're checking types correctly.
    validateOk(
      """|collection Foo {
         |  compute a = doc => {
         |    let doc = { ts: 3 }
         |    doc.ts
         |  }
         |
         |  index byA { terms [.a] }
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("Foo", Some("{ a: 3 }"))
    )
  }

  // This depends on the type `User` existing in the env, which is currently
  // determined by model.
  //
  // So, this test relies heavily on the `User` type being present in the hardcoded
  // test environment. FIXME: Generate user-defined env from schema.
  def checkDisallowed(what: String, source: String): Unit = {
    it should what in {
      val err = validateErr(
        s"""|collection User {
            |  compute a = _ => $source
            |  index byA { terms [.a] }
            |}""".stripMargin
      )
      err should include(
        "Computed fields used in indexes or constraints cannot access collections"
      )
    }
  }

  checkDisallowed("disallow making sets", "User.all()")
  checkDisallowed("disallow accessing sets", "User.all().count()")
  checkDisallowed("disallow creating docs", "User.create({})")
  checkDisallowed("disallow looking up definition", "User.definition")
  checkDisallowed("disallow calling where()", "User.where(.foo > 3)")
  checkDisallowed("disallow calling firstWhere()", "User.firstWhere(.foo > 3)")
  checkDisallowed("disallow calling index", "User.byName('foo')")

  // This should be allowed eventually, but for now we need to disallow it.
  checkDisallowed("disallow calling byId", "User.byId(3)")

  it should "check role privileges" in {
    validateOk(
      """|collection User {
         |  foo: Int
         |}
         |role Admin {
         |  privileges User {
         |    read { predicate ((doc) => doc.foo) }
         |  }
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None)
    )

    validateErr(
      """|collection User {
         |  foo: Int
         |}
         |role Admin {
         |  privileges User {
         |    read { predicate ((doc) => 1 + 'hi') }
         |  }
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `String` is not a subtype of `Number`
         |at main.fsl:6:36
         |  |
         |6 |     read { predicate ((doc) => 1 + 'hi') }
         |  |                                    ^^^^
         |  |""".stripMargin
    )
  }

  it should "check role membership" in {
    validateOk(
      """|collection User {
         |  foo: Int
         |}
         |role Admin {
         |  membership User {
         |    predicate ((doc) => doc.foo)
         |  }
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None)
    )

    validateErr(
      """|collection User {
         |  foo: Int
         |}
         |role Admin {
         |  membership User {
         |    predicate ((doc) => 1 + 'hi')
         |  }
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `String` is not a subtype of `Number`
         |at main.fsl:6:29
         |  |
         |6 |     predicate ((doc) => 1 + 'hi')
         |  |                             ^^^^
         |  |""".stripMargin
    )
  }

  it should "check access provider roles" in {
    validateOk(
      """|collection User {
         |  foo: Int
         |}
         |role Foo {}
         |access provider FooBar {
         |  issuer "me"
         |  jwks_uri "https://example.com"
         |  role Foo {
         |    predicate ((doc) => doc.foo)
         |  }
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None)
    )

    validateErr(
      """|collection User {
         |  foo: Int
         |}
         |role Foo {}
         |access provider FooBar {
         |  issuer "me"
         |  jwks_uri "https://example.com"
         |  role Foo {
         |    predicate ((doc) => 1 + 'hi')
         |  }
         |}""".stripMargin
    ) shouldBe (
      """|error: Type `String` is not a subtype of `Number`
         |at main.fsl:9:29
         |  |
         |9 |     predicate ((doc) => 1 + 'hi')
         |  |                             ^^^^
         |  |""".stripMargin
    )
  }

  it should "typecheck refs correctly" in {
    validateOk(
      """|collection User {
         |  cyclic_ref: User | NullUser
         |  cyclic_ref2: Ref<User>
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None)
    )
  }

  it should "typecheck mva terms correctly" in {
    validateOk(
      """|collection User {
         |  name: Array<String>
         |  index byName {
         |    terms [mva(.name)]
         |  }
         |}
         |
         |function foo() {
         |  User.byName("hello")
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None),
      FunctionUpdate("foo", "() => Set<User>")
    )
  }

  it should "disallow multiple defaults for nested fields" in {
    validateErr(
      """|collection Foo {
         |  a: {
         |    b: Int = 3
         |    c: String
         |  } = { b: 4, c: "hi" }
         |}""".stripMargin
    ) shouldBe (
      """|error: Default values are not allowed here
         |at main.fsl:3:14
         |  |
         |3 |     b: Int = 3
         |  |              ^
         |  |
         |hint: Parent has a conflicting default value
         |at main.fsl:5:7
         |  |
         |5 |   } = { b: 4, c: "hi" }
         |  |       ^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow defaults in nullable fields" in {
    validateErr(
      """|collection Foo {
         |  a: {
         |    b: Int = 3
         |    c: String
         |  }?
         |}""".stripMargin
    ) shouldBe (
      """|error: Default values are not allowed here
         |at main.fsl:3:14
         |  |
         |3 |     b: Int = 3
         |  |              ^
         |  |
         |hint: Parent is nullable
         |at main.fsl:5:4
         |  |
         |5 |   }?
         |  |    ^
         |  |""".stripMargin
    )
  }

  it should "disallow defaults in unions" in {
    validateErr(
      """|collection Foo {
         |  a: {
         |    b: Int = 3
         |    c: String
         |  } | {
         |    b: String
         |  }
         |}""".stripMargin
    ) shouldBe (
      """|error: Default values are not allowed here
         |at main.fsl:3:14
         |  |
         |3 |     b: Int = 3
         |  |              ^
         |  |
         |hint: Parent is a union
         |at main.fsl:2:6
         |  |
         |2 |     a: {
         |  |  ______^
         |3 | |     b: Int = 3
         |4 | |     c: String
         |5 | |   } | {
         |6 | |     b: String
         |7 | |   }
         |  | |___^
         |  |""".stripMargin
    )
  }

  it should "disallow defaults in intersections" in {
    validateErr(
      """|collection Foo {
         |  a: {
         |    b: Int = 3
         |    c: String
         |  } & {
         |    b: String
         |  }
         |}""".stripMargin
    ) shouldBe (
      """|error: Default values are not allowed here
         |at main.fsl:3:14
         |  |
         |3 |     b: Int = 3
         |  |              ^
         |  |
         |hint: Parent is not an object
         |at main.fsl:2:6
         |  |
         |2 |     a: {
         |  |  ______^
         |3 | |     b: Int = 3
         |4 | |     c: String
         |5 | |   } & {
         |6 | |     b: String
         |7 | |   }
         |  | |___^
         |  |""".stripMargin
    )
  }

  it should "typecheck default fields correctly" in {
    validateOk(
      """|collection User {
         |  a: {
         |    foo: Int = 3
         |    bar: String
         |  }
         |}
         |
         |function foo() {
         |  User.create({ a: { bar: "hi" } })
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None),
      FunctionUpdate("foo", "() => User")
    )

    validateOk(
      """|collection User {
         |  a: {
         |    foo: Int = 3
         |    bar: String = "hi"
         |  }
         |}
         |
         |function foo() {
         |  User.create({})
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None),
      FunctionUpdate("foo", "() => User")
    )
  }

  it should "typecheck ref types correctly" in {
    validateOk(
      """|collection User {
         |  address: Ref<Address>
         |}
         |
         |collection Address {
         |  street: String
         |}
         |
         |function foo() {
         |  User.create({ address: Address.create({ street: "hi" }) })
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None),
      CollectionUpdate("Address", None),
      FunctionUpdate("foo", "() => User")
    )
  }

  it should "allow accessing `id` and `ts`" in {
    validateOk(
      """|collection User {
         |  a: Int
         |}
         |
         |function foo() {
         |  User.create({ a: 0 }).id
         |  User.create({ a: 0 }).ts
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None),
      FunctionUpdate("foo", "() => Time")
    )
  }

  it should "allow defaults on the parent field" in {
    validateOk(
      """|collection User {
         |  address: {
         |    street: String
         |    city: String?
         |  } = {
         |    street: "unknown"
         |  }
         |}
         |
         |function foo() {
         |  User.create({}).address
         |}""".stripMargin
    ) shouldBe Seq(
      CollectionUpdate("User", None),
      FunctionUpdate("foo", "() => { street: String, city: String | Null }")
    )
  }

  it should "disallow roles refering to nothing" in {
    validateErr(
      """|role foo {
         |  privileges bar {}
         |}""".stripMargin
    ) shouldBe (
      """|error: Resource `bar` does not exist
         |at main.fsl:2:14
         |  |
         |2 |   privileges bar {}
         |  |              ^^^
         |  |""".stripMargin
    )

    validateErr(
      """|role foo {
         |  membership bar
         |}""".stripMargin
    ) shouldBe (
      """|error: Resource `bar` does not exist
         |at main.fsl:2:14
         |  |
         |2 |   membership bar
         |  |              ^^^
         |  |""".stripMargin
    )
  }

  it should "only allow memberships for collections" in {
    validateOk(
      """|role foo {
         |  membership Collection
         |}""".stripMargin
    )
    validateOk(
      """|collection Foo {}
         |
         |role foo {
         |  membership Foo
         |}""".stripMargin
    )

    validateErr(
      """|function foo() { 0 }
         |
         |role foo {
         |  membership foo
         |}""".stripMargin
    ) shouldBe (
      """|error: Resource `foo` does not exist
         |at main.fsl:4:14
         |  |
         |4 |   membership foo
         |  |              ^^^
         |  |""".stripMargin
    )
  }

  it should "disallow access providers refering to nothing" in {
    validateErr(
      """|access provider bar {
         |  issuer "me"
         |  jwks_uri "https://example.com"
         |  role foo
         |}""".stripMargin
    ) shouldBe (
      """|error: Role `foo` does not exist
         |at main.fsl:4:8
         |  |
         |4 |   role foo
         |  |        ^^^
         |  |""".stripMargin
    )
  }

  it should "disallow builtin roles in access providers" in {
    Seq("admin", "server", "client").foreach { role =>
      validateErr(
        s"""|access provider foo {
            |  issuer "me"
            |  jwks_uri "https://example.com"
            |  role $role
            |}""".stripMargin
      ) shouldBe (
        s"""|error: Builtin role `${role}` is not allowed
            |at main.fsl:4:8
            |  |
            |4 |   role $role
            |  |        ${"^" * role.length}
            |  |""".stripMargin
      )
    }

    validateErr(
      """|access provider foo {
         |  issuer "me"
         |  jwks_uri "https://example.com"
         |  role "server-readonly"
         |}""".stripMargin
    ) shouldBe (
      """|error: Builtin role `server-readonly` is not allowed
         |at main.fsl:4:8
         |  |
         |4 |   role "server-readonly"
         |  |        ^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "allow roles refering to aliases" in {
    validateOk(
      """|@alias(foo2)
         |function foo() { 0 }
         |
         |role bar {
         |  privileges foo2 { read }
         |}""".stripMargin
    )
  }
}
