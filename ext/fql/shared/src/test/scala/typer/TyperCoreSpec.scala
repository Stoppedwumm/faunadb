package fql.test

class TyperCoreSpec extends TypeSpec {

  // TODO: expand these tests based on built-in functions

  "A Typer" should "type literals" in {
    typecheck("123", "123")
    typecheck("'foo'", "\"foo\"")
    typecheck("true", "true")
    typecheck("false", "false")
  }

  it should "typecheck equality operators" in {
    typecheck("3 == 3", "Boolean")
    typecheck("3 != 3", "Boolean")
    typecheck("3 < 3", "Boolean")
    typecheck("3 > 3", "Boolean")
    typecheck("3 <= 3", "Boolean")
    typecheck("3 >= 3", "Boolean")

    typecheck("(if (true) 3 else 'hi') == 3", "Boolean")
    typecheck("(if (true) 3 else 'hi') != 3", "Boolean")
    typecheck("(if (true) 3 else 'hi') < 3", "Boolean")
    typecheck("(if (true) 3 else 'hi') > 3", "Boolean")
    typecheck("(if (true) 3 else 'hi') <= 3", "Boolean")
    typecheck("(if (true) 3 else 'hi') >= 3", "Boolean")
  }

  // FIXME: This should add NullUser to the result, not Null.
  it should "typecheck ?. with call" in {
    typecheck("User.byId('1234')?.name", "String | Null")

    typecheck(
      "User.byId('1234')?.update",
      "((data: { name: String | Null }) => User) | Null")
    typecheck("User.byId('1234')?.update({})", "User | Null")

    // this sets applyOptional
    typecheck("{ foo: null }.foo?.()", "Null")
  }

  it should "typecheck records" in {
    typecheck(
      "gimmeAandB({})",
      """|error: Type `{}` is not a subtype of `{ a: Int, b: String }`
         |at *query*:1:12
         |  |
         |1 | gimmeAandB({})
         |  |            ^^
         |  |
         |cause: Type `{}` does not have field `a`
         |  |
         |1 | gimmeAandB({})
         |  |            ^^
         |  |
         |cause: Type `{}` does not have field `b`
         |  |
         |1 | gimmeAandB({})
         |  |            ^^
         |  |""".stripMargin
    )
    typecheck(
      "gimmeAandB({ a: 3 })",
      """|error: Type `{ a: 3 }` does not have field `b`
         |at *query*:1:12
         |  |
         |1 | gimmeAandB({ a: 3 })
         |  |            ^^^^^^^^
         |  |""".stripMargin
    )

    // The second cause here should be removed.
    typecheck(
      "gimmeAandB({ a: 'hi' })",
      """|error: Type `{ a: "hi" }` is not a subtype of `{ a: Int, b: String }`
         |at *query*:1:12
         |  |
         |1 | gimmeAandB({ a: 'hi' })
         |  |            ^^^^^^^^^^^
         |  |
         |cause: Type `{ a: "hi" }` does not have field `b`
         |  |
         |1 | gimmeAandB({ a: 'hi' })
         |  |            ^^^^^^^^^^^
         |  |
         |cause: Type `String` is not a subtype of `Int`
         |  |
         |1 | gimmeAandB({ a: 'hi' })
         |  |                 ^^^^
         |  |""".stripMargin
    )

    typecheck("gimmeAandB({ a: 3, b: 'hi' })", "Int")

    typecheck(
      "gimmeAandB({ a: 3, b: 'hi', c: 5 })",
      """|error: Type `{ a: 3, b: "hi", c: 5 }` contains extra field `c`
         |at *query*:1:12
         |  |
         |1 | gimmeAandB({ a: 3, b: 'hi', c: 5 })
         |  |            ^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("gimmeAandwildstr({ a: 3 })", "Int")
    typecheck("gimmeAandwildstr({ a: 3, b: 'hi' })", "Int")
    typecheck("gimmeAandwildstr({ a: 3, b: 'hi', c: 'hello' })", "Int")
    typecheck(
      "gimmeAandwildstr({ a: 3, b: 4 })",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:29
         |  |
         |1 | gimmeAandwildstr({ a: 3, b: 4 })
         |  |                             ^
         |  |""".stripMargin
    )
    typecheck(
      "gimmeAandwildstr({ a: 3, b: 4, c: User })",
      """|error: Type `{ a: 3, b: 4, c: UserCollection }` is not a subtype of `{ a: Int, *: String }`
         |at *query*:1:18
         |  |
         |1 | gimmeAandwildstr({ a: 3, b: 4, c: User })
         |  |                  ^^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |cause: Type `Int` is not a subtype of `String`
         |  |
         |1 | gimmeAandwildstr({ a: 3, b: 4, c: User })
         |  |                             ^
         |  |
         |cause: Type `UserCollection` is not a subtype of `String`
         |  |
         |1 | gimmeAandwildstr({ a: 3, b: 4, c: User })
         |  |                                   ^^^^
         |  |""".stripMargin
    )

    typecheck("createAandwildstr()", "{ a: Int, *: String }")
    typecheck("createAandwildstr().a", "Int")
    typecheck("createAandwildstr().b", "String | Null")
    typecheck("gimmeAandwildstr(createAandwildstr())", "Int")

    typecheck("createAandwildany()", "{ a: Int, *: Any }")
    typecheck("createAandwildany().a", "Int")
    typecheck("createAandwildany().b", "Any")
    typecheck(
      "gimmeAandB(createAandwildany())",
      """|error: Type `{ a: Int, *: Any }` cannot have a wildcard
         |at *query*:1:12
         |  |
         |1 | gimmeAandB(createAandwildany())
         |  |            ^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
    typecheck(
      "gimmeAandwildstr(createAandwildint())",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:18
         |  |
         |1 | gimmeAandwildstr(createAandwildint())
         |  |                  ^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // { a: int, *: int } <: { *: int }
    typecheck("gimmewildint(createAandwildint())", "Int")

    // having wildcard in the value makes it special:
    // { *: int } <: { a: int | Null }
    typecheck("gimmeoptA(createwildint())", "Int")
    typecheck("gimmeoptA({ a: 5 })", "Int")
    typecheck("gimmeoptA({ a: null })", "Int")

    // optional fields work
    typecheck("gimmeoptA({})", "Int")

    // Constraint.Func span should work
    typecheck(
      "gimmebody({ body: (x) => x + 2 })",
      """|error: Type `Int => Int` is not a subtype of `String`
         |at *query*:1:19
         |  |
         |1 | gimmebody({ body: (x) => x + 2 })
         |  |                   ^^^^^^^^^^^^
         |  |""".stripMargin
    )
    // Constraint.Func span should work for short lambdas
    typecheck(
      "gimmebody({ body: (.foo) })",
      """|error: Type `{ foo: A, ... } => A` is not a subtype of `String`
         |at *query*:1:20
         |  |
         |1 | gimmebody({ body: (.foo) })
         |  |                    ^^^^
         |  |""".stripMargin
    )
  }

  it should "implicitly add fields" in {
    // normally, accessing `a` would be disallowed, but the static type allows you to
    // "add" a field to a type that doesn't have that field.
    typecheck("{ let obj: { a: 3 | Null } = {}; obj.a }", "3 | Null")
  }

  it should "typecheck documents" in {
    typecheck("User.all()", "Set<User>")
    typecheck("User.create({ name: 'Bob' })", "User")
    typecheck("User.create({ name: 'Bob' }).exists()", "true")

    typecheck("User.byId('1234')", "Ref<User>")
    typecheck("User.byId('1234').coll", "Any | UserCollection")
    typecheck("User.byId('1234').exists()", "Boolean")
    typecheck(
      "User.byId('1234').name",
      """|error: Type `Null` does not have field `name`
         |at *query*:1:19
         |  |
         |1 | User.byId('1234').name
         |  |                   ^^^^
         |  |
         |hint: Use the ! or ?. operator to handle the null case
         |  |
         |1 | User.byId('1234')!.name
         |  |                  +
         |  |""".stripMargin
    )

    typecheck(
      "User.byId('1234').update({})",
      """|error: Type `Null` does not have field `update`
         |at *query*:1:19
         |  |
         |1 | User.byId('1234').update({})
         |  |                   ^^^^^^
         |  |
         |hint: Use the ! or ?. operator to handle the null case
         |  |
         |1 | User.byId('1234')!.update({})
         |  |                  +
         |  |""".stripMargin
    )

    typecheck("User.byId('1234')!", "User")
    typecheck("User.byId('1234')!.coll", "UserCollection")
    typecheck("User.byId('1234')!.exists()", "true")
    typecheck("User.byId('1234')!.name", "String")

    typecheck(
      "User.create(3)",
      """|error: Type `Int` is not a subtype of `{ id: ID | Null, name: String }`
         |at *query*:1:13
         |  |
         |1 | User.create(3)
         |  |             ^
         |  |""".stripMargin
    )

    typecheck("User.byId('1234') { id }", "{ id: ID } | Null")
    typecheck("User.byId('1234')! { id }", "{ id: ID }")
  }

  it should "typecheck doc refs" in {
    // These tests are based on a repro of the Ref alias hierarchy, which is
    // better captured in FQL2DocRefSpec.
    typecheck(
      "{ let a: Any = null; let b: Ref<User> = a; let c: User | NullUser = b; c }",
      "User | NullUser")

    typecheck(
      "{ let a: Any = null; let b: Ref<User> = a; let c: Ref<User> | Int = b; c }",
      "Int | Ref<User>")
  }

  it should "show type not used as function error" in {
    typecheck(
      "[].map(true)",
      """|error: Type `Boolean` cannot be used as a function
         |at *query*:1:8
         |  |
         |1 | [].map(true)
         |  |        ^^^^
         |  |""".stripMargin
    )

    typecheck(
      "[].map(2 == 3)",
      """|error: Type `Boolean` cannot be used as a function
         |at *query*:1:8
         |  |
         |1 | [].map(2 == 3)
         |  |        ^^^^^^
         |  |""".stripMargin
    )
  }

  it should "suggest bang operator in the right place" in {
    typecheck(
      "(if (true) 3 else null).foo",
      """|error: Type `3 | Null` is not a subtype of `{ foo: Any, ... }`
         |at *query*:1:1
         |  |
         |1 | (if (true) 3 else null).foo
         |  | ^^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |cause: Type `Int` does not have field `foo`
         |  |
         |1 | (if (true) 3 else null).foo
         |  |                         ^^^
         |  |
         |hint: Type `Int` inferred here
         |  |
         |1 | (if (true) 3 else null).foo
         |  |            ^
         |  |
         |cause: Type `Null` does not have field `foo`
         |  |
         |1 | (if (true) 3 else null).foo
         |  |                         ^^^
         |  |
         |hint: Type `Null` inferred here
         |  |
         |1 | (if (true) 3 else null).foo
         |  |                   ^^^^
         |  |
         |hint: Use the ! or ?. operator to handle the null case
         |  |
         |1 | (if (true) 3 else null)!.foo
         |  |                        +
         |  |""".stripMargin
    )

    typecheck(
      "{ let a: Array<5> = [5]; a.foo }",
      """|error: Type `Array<5>` does not have field `foo`
         |at *query*:1:28
         |  |
         |1 | { let a: Array<5> = [5]; a.foo }
         |  |                            ^^^
         |  |
         |hint: Type `Array<5>` inferred here
         |  |
         |1 | { let a: Array<5> = [5]; a.foo }
         |  |          ^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "typecheck record fields & record access" in {
    typecheck(
      "{}.foo",
      """|error: Type `{}` does not have field `foo`
         |at *query*:1:4
         |  |
         |1 | {}.foo
         |  |    ^^^
         |  |""".stripMargin
    )

    typecheck("{ foo: 1, bar: 2 }['foo']", "1")
    typecheck("""{ foo: 1, bar: 2 }["foo"]""", "1")

    typecheck("""{ foo: 1, bar: 2 }["#{'foo'}"]""", "1 | 2 | Null")

    typecheck("{ let x = 'foo'; { foo: 1, bar: 2 }[x] }", "1 | 2 | Null")
    typecheck("x => { foo: 1, bar: 2 }[x]", "String => 1 | 2 | Null")

    typecheck(
      "{}['foo']",
      """|error: Type `{}` does not have field `foo`
         |at *query*:1:4
         |  |
         |1 | {}['foo']
         |  |    ^^^^^
         |  |""".stripMargin
    )

    typecheck(
      "{}[3]",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:4
         |  |
         |1 | {}[3]
         |  |    ^
         |  |""".stripMargin
    )

    typecheck("User.create({ name: 'Bob' }).name", "String")
    typecheck("User.create({ name: 'Bob' })['name']", "String")
    typecheck(
      "User.create({ name: 'Bob' })[\"#{'foo'}\"]",
      "ID | Time | String | Null")
    typecheck(
      "User.create({ name: 'Bob' })[3]",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:30
         |  |
         |1 | User.create({ name: 'Bob' })[3]
         |  |                              ^
         |  |""".stripMargin
    )

    typecheck(
      "{}['3', '4']",
      """|error: Function was called with too many arguments. Expected 1, received 2
         |at *query*:1:3
         |  |
         |1 | {}['3', '4']
         |  |   ^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("foobarwild['hi']", "Boolean | Null")

    typecheck(
      "{ a: 3, b: 4 }['hi']",
      """|error: Type `{ a: 3, b: 4 }` does not have field `hi`
         |at *query*:1:16
         |  |
         |1 | { a: 3, b: 4 }['hi']
         |  |                ^^^^
         |  |""".stripMargin
    )
  }

  it should "typecheck intIntBoolAny" in {
    typecheck(
      "(x => intIntBoolAny(intIntBoolAny(x, 1), 2))",
      "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => intIntBoolAny(intIntBoolAny(x, 1), 2))(3)", "Int")
    typecheck("(x => intIntBoolAny(intIntBoolAny(x, 1), 2))(false)", "Boolean")

    // FIXME: should prune uninhabited input types
    typecheck(
      "(x => intIntBoolAny(x, intIntBoolAny(x, 1)))",
      "(Int => Int) & (Int & Boolean => Boolean) & (Boolean => Boolean)")
    typecheck("(x => intIntBoolAny(x, intIntBoolAny(x, 1)))(3)", "Int")
    typecheck("(x => intIntBoolAny(x, intIntBoolAny(x, 1)))(true)", "Boolean")

    typecheck(
      "x => intIntBoolAny(x, intBool(if (true) 1 else true))",
      "Boolean => Boolean")
    typecheck(
      "(x => intIntBoolAny(x, intBool(if (true) 1 else true)))(true)",
      "Boolean")
    typecheck(
      "(x => intIntBoolAny(x, intBool(if (true) 1 else true)))(1)",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:57
         |  |
         |1 | (x => intIntBoolAny(x, intBool(if (true) 1 else true)))(1)
         |  |                                                         ^
         |  |""".stripMargin
    )
  }
  it should "typecheck reduce()" in {
    typecheck(
      "[1, 2].reduce(() => true)",
      """|error: Function was called with too many arguments. Expected 0, received 2
         |at *query*:1:15
         |  |
         |1 | [1, 2].reduce(() => true)
         |  |               ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "typecheck map and +" in {
    typecheck("[1].map(v => (v + 0) * 0)", "Array<Int>")
    typecheck("[1].map(v => 0 * (v + 0))", "Array<Int>")
    typecheck("[1].flatMap((v) => [v * (v + 0)])", "Array<Int>")
    typecheck("[1].flatMap((v) => [0 * (v + 0)])", "Array<Int>")
    typecheck("[1].flatMap((v) => [0 * (v + v * v)])", "Array<Int>")

    typecheck("[anyval].map(v => (v + 0) * 0)", "Array<Int>")
    typecheck("[anyval].map(v => 0 * (v + 0))", "Array<Int>")
    typecheck("[anyval].flatMap((v) => [v * (v + 0)])", "Array<Int>")
    typecheck("[anyval].flatMap((v) => [0 * (v + 0)])", "Array<Int>")
    typecheck("[anyval].flatMap((v) => [0 * (v + v * v)])", "Array<Int>")

    typecheck("[1].map((v) => 0 + (v + (v + v * v)))", "Array<Int>")
    typecheck("[1].map((v) => v + (v + v * v))", "Array<Int>")
  }

  it should "typecheck 1 + 2 + 3" in {
    typecheck("1 + 2 + 3", "Int")
  }

  it should "typecheck lambda overloads" in {
    typecheck("(x) => { if (true) x + '2' else x }", "String => String")
    typecheck(
      "{ let func = (x) => { if (true) x + '2' else x }; func }",
      "String => String")

    typecheck("((x) => { if (true) x + '2' else x })('hi')", "String")

    typecheck(
      "{ let func = (x) => { if (true) x + '2' else x }; func('hi') }",
      "String")
  }

  it should "typecheck tuples" in {
    typecheck("objectFromEntries([])", "{ *: Never }")
    typecheck("objectFromEntries([['hi', 3]])", "{ *: 3 }")
    typecheck("objectFromEntries([['hi', 3], ['hello', 4]])", "{ *: 3 | 4 }")

    typecheck(
      "objectFromEntries([['hi', 3, 4]])",
      """|error: Tuple contains too many elements. Expected 2, received 3
         |at *query*:1:20
         |  |
         |1 | objectFromEntries([['hi', 3, 4]])
         |  |                    ^^^^^^^^^^^^
         |  |""".stripMargin
    )
    typecheck(
      "objectFromEntries([['hi']])",
      """|error: Tuple does not contain enough elements. Expected 2, received 1
         |at *query*:1:20
         |  |
         |1 | objectFromEntries([['hi']])
         |  |                    ^^^^^^
         |  |""".stripMargin
    )
    typecheck(
      "objectFromEntries([['hi'], ['hi', 3]])",
      """|error: Tuple does not contain enough elements. Expected 2, received 1
         |at *query*:1:20
         |  |
         |1 | objectFromEntries([['hi'], ['hi', 3]])
         |  |                    ^^^^^^
         |  |""".stripMargin
    )
    typecheck(
      "objectFromEntries([[3, 3]])",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:21
         |  |
         |1 | objectFromEntries([[3, 3]])
         |  |                     ^
         |  |""".stripMargin
    )
  }

  it should "typecheck tuples (pending)" in {
    typecheck(
      "objectFromEntries([['hi'], ['hi']])",
      """|error: Tuple does not contain enough elements. Expected 2, received 1
         |at *query*:1:20
         |  |
         |1 | objectFromEntries([['hi'], ['hi']])
         |  |                    ^^^^^^
         |  |""".stripMargin
    )

    pendingUntilFixed {
      typecheck(
        "objectFromEntries([['hi'], ['hi']])",
        """|error: Type `[["hi"], ["hi"]]` is not a subtype of `Array<[String, Any]>`
           |at *query*:1:19
           |  |
           |1 | objectFromEntries([['hi'], ['hi']])
           |  |                   ^^^^^^^^^^^^^^^^
           |  |
           |cause: Tuple does not contain enough elements. Expected 2, received 1
           |  |
           |1 | objectFromEntries([['hi'], ['hi']])
           |  |                    ^^^^^^
           |  |
           |cause: Tuple does not contain enough elements. Expected 2, received 1
           |  |
           |1 | objectFromEntries([['hi'], ['hi']])
           |  |                            ^^^^^^
           |  |""".stripMargin
      )
    }
  }

  it should "report multiple errors" in {
    typecheck(
      "{ a: 1.foo, b: 2.bar }",
      """|error: Type `Int` does not have field `foo`
         |at *query*:1:8
         |  |
         |1 | { a: 1.foo, b: 2.bar }
         |  |        ^^^
         |  |
         |
         |error: Type `Int` does not have field `bar`
         |at *query*:1:18
         |  |
         |1 | { a: 1.foo, b: 2.bar }
         |  |                  ^^^
         |  |""".stripMargin
    )
  }

  it should "report method apply errors" in {
    typecheck(
      "'foo'.concat(2)",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:14
         |  |
         |1 | 'foo'.concat(2)
         |  |              ^
         |  |""".stripMargin
    )
  }

  it should "report operator apply errors" in {
    typecheck(
      "2 + 'foo'",
      """|error: Type `String` is not a subtype of `Int`
         |at *query*:1:5
         |  |
         |1 | 2 + 'foo'
         |  |     ^^^^^
         |  |""".stripMargin
    )
  }

  it should "report user-friendly arity mismatch errors" in {
    typecheck(
      "succ(1, 2)",
      """|error: Function was called with too many arguments. Expected 1, received 2
         |at *query*:1:5
         |  |
         |1 | succ(1, 2)
         |  |     ^^^^^^
         |  |""".stripMargin
    )
    typecheck(
      "succ()",
      """|error: Function was not called with enough arguments. Expected 1, received 0
         |at *query*:1:5
         |  |
         |1 | succ()
         |  |     ^^
         |  |""".stripMargin
    )
  }

  it should "type blocks/lets" in {
    typecheck(
      """|x => {
         | let y = x
         | succ(y)
         |}""".stripMargin,
      "Int => Int"
    )

    typecheck(
      """|x => {
         | let y: Int = x
         | succ(y)
         |}""".stripMargin,
      "Int => Int"
    )

    typecheck(
      """|x => {
         | let y: String = x
         | succ(y)
         |}""".stripMargin,
      """|error: Type `String` is not a subtype of `Int`
         |at *query*:3:7
         |  |
         |3 |  succ(y)
         |  |       ^
         |  |
         |cause: Type `String` is not a subtype of `Int`
         |at *query*:2:9
         |  |
         |2 |  let y: String = x
         |  |         ^^^^^^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let doit = s => {
         |    succ(s)
         |    not(s)
         |  }
         |
         |  doit(1)
         |}""".stripMargin,
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:7:8
         |  |
         |7 |   doit(1)
         |  |        ^
         |  |""".stripMargin
    )

    // FIXME the failed type is not good
    typecheck(
      """|{
         |  let doit = s => {
         |    succ(s)
         |    not(s)
         |  }
         |
         |  doit(reverse('1'))
         |}""".stripMargin,
      """|error: Type `String` is not a subtype of `Boolean | Int`
         |at *query*:7:8
         |  |
         |7 |   doit(reverse('1'))
         |  |        ^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // FIXME the spans are bad here
    typecheck(
      """|{
         |  let doit: Boolean => Boolean = s => {
         |    succ(s)
         |    not(s)
         |  }
         |
         |  doit
         |}""".stripMargin,
      """|error: Type `Int & Boolean => Boolean` is not a subtype of `Boolean => Boolean`
         |at *query*:2:7
         |  |
         |2 |   let doit: Boolean => Boolean = s => {
         |  |       ^^^^
         |  |
         |cause: Type `Boolean` is not a subtype of `Int`
         |  |
         |2 |   let doit: Boolean => Boolean = s => {
         |  |             ^^^^^^^
         |  |""".stripMargin
    )

    typecheck(
      """|x => {
         | let y: Int => Int = succ
         | y(x)
         |}""".stripMargin,
      "Int => Int"
    )

    typecheck(
      """|x => {
         | let y: Int => Int = z => z
         | y(x)
         |}""".stripMargin,
      "Int => Int"
    )

    typecheck(
      """|{
         | let y: b => b = z => z
         | y
         |}""".stripMargin,
      "A => A"
    )
    typecheck(
      """|{
         | let y: b => b = z => z
         | y(2)
         |}""".stripMargin,
      "2"
    )

    typecheck(
      """|{
         | let y: { foo: b } => b = z => z.foo
         | y
         |}""".stripMargin,
      "{ foo: A } => A"
    )

    typecheck(
      """|{
         | let y: { foo: A } => B = z => z.foo
         | y
         |}""".stripMargin,
      """|error: Unknown type `A`
         |at *query*:2:16
         |  |
         |2 |  let y: { foo: A } => B = z => z.foo
         |  |                ^
         |  |
         |
         |error: Unknown type `B`
         |at *query*:2:23
         |  |
         |2 |  let y: { foo: A } => B = z => z.foo
         |  |                       ^
         |  |""".stripMargin
    )

    typecheck(
      """|x => {
         | let y: Array<Int> = x
         | y
         |}""".stripMargin,
      "Array<Int> => Array<Int>"
    )

    typecheck(
      """|x => {
         | let y: Array<Int> = x
         | y
         |}""".stripMargin,
      "Array<Int> => Array<Int>"
    )
  }

  it should "disallow outer values constrained by inner generic types" in {

    // FIXME: the span for `k` isn't ideal.
    typecheck(
      "k => { let test: X => X = k; test }",
      """|error: Value cannot be constrained by inner generic type `X => X`
         |at *query*:1:12
         |  |
         |1 | k => { let test: X => X = k; test }
         |  |            ^^^^
         |  |
         |hint: Generic type definition
         |  |
         |1 | k => { let test: X => X = k; test }
         |  |                  ^^^^^^
         |  |
         |hint: Outer value originates here
         |  |
         |1 | k => { let test: X => X = k; test }
         |  | ^
         |  |""".stripMargin
    )

    typecheck(
      "k => y => { let test: X => X = { let z = y(k); z }; test }",
      """|error: Value cannot be constrained by inner generic type `X => X`
         |at *query*:1:17
         |  |
         |1 | k => y => { let test: X => X = { let z = y(k); z }; test }
         |  |                 ^^^^
         |  |
         |hint: Generic type definition
         |  |
         |1 | k => y => { let test: X => X = { let z = y(k); z }; test }
         |  |                       ^^^^^^
         |  |
         |hint: Outer value originates here
         |  |
         |1 | k => y => { let test: X => X = { let z = y(k); z }; test }
         |  |                                           ^^^
         |  |""".stripMargin
    )
  }

  it should "show where type was inferred from" in {
    typecheck(
      """|{
         |  let a = 3
         |
         |  a.foo
         |}""".stripMargin,
      """|error: Type `Int` does not have field `foo`
         |at *query*:4:5
         |  |
         |4 |   a.foo
         |  |     ^^^
         |  |
         |hint: Type `Int` inferred here
         |at *query*:2:11
         |  |
         |2 |   let a = 3
         |  |           ^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let a: Int = 3
         |
         |  a.foo
         |}""".stripMargin,
      """|error: Type `Int` does not have field `foo`
         |at *query*:4:5
         |  |
         |4 |   a.foo
         |  |     ^^^
         |  |
         |hint: Type `Int` inferred here
         |at *query*:2:10
         |  |
         |2 |   let a: Int = 3
         |  |          ^^^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let a = if (true) 3 else null
         |
         |  a.foo
         |}""".stripMargin,
      """|error: Type `3 | Null` is not a subtype of `{ foo: Any, ... }`
         |at *query*:4:3
         |  |
         |4 |   a.foo
         |  |   ^
         |  |
         |cause: Type `Int` does not have field `foo`
         |  |
         |4 |   a.foo
         |  |     ^^^
         |  |
         |hint: Type `Int` inferred here
         |at *query*:2:21
         |  |
         |2 |   let a = if (true) 3 else null
         |  |                     ^
         |  |
         |cause: Type `Null` does not have field `foo`
         |  |
         |4 |   a.foo
         |  |     ^^^
         |  |
         |hint: Type `Null` inferred here
         |at *query*:2:28
         |  |
         |2 |   let a = if (true) 3 else null
         |  |                            ^^^^
         |  |
         |hint: Use the ! or ?. operator to handle the null case
         |  |
         |4 |   a!.foo
         |  |    +
         |  |""".stripMargin
    )
  }

  it should "check annotations (pending)" in {
    typecheck(
      "k => { let fun: A => A | Int = x => if (true) x else succ(2); k(fun) }",
      "((A => A | Int) => B) => B")
  }

  it should "type if exprs" in {
    typecheck("if (true) true else false", "Boolean")
  }

  it should "type ?? operator" in {
    typecheck("null ?? 1", "1")
    typecheck("1 ?? 2", "2 | 1")
    typecheck("1 ?? 'str'", "\"str\" | 1")
    typecheck("null ?? {x: 1}", "{ x: 1 }")

    typecheck("x => x ?? 1", "A => 1 | (A - Null)")
  }

  it should "type lambdas" in {
    typecheck("x => 42", "Any => 42")
    typecheck("x => x", "A => A")
    typecheck("x => x(42)", "(42 => A) => A")
    typecheck("x => if (x) 1 else 2", "Boolean => 1 | 2")
    typecheck("(x => x)(42)", "42")
    typecheck("(x, y) => if (true) x else y", "(A, A) => A")

    typecheck(
      "1(2)",
      """|error: Type `Int` cannot be used as a function
         |at *query*:1:1
         |  |
         |1 | 1(2)
         |  | ^
         |  |
         |hint: `Int` is used as a function here
         |  |
         |1 | 1(2)
         |  |  ^^^
         |  |""".stripMargin
    )
  }

  it should "type lambdas (pending)" in {
    pendingUntilFixed {
      typecheck("f => x => f(f(x))", "(A | B => A) => B => A")
    }
  }

  it should "type short lambdas" in {
    typecheck("(.foo)", "{ foo: A, ... } => A")

    typecheck(
      "(.foo + .bar)",
      "({ foo: Int, bar: Int, ... } => Int) & ({ foo: String, bar: String, ... } => String)")
  }

  it should "type objects & selection" in {
    typecheck("{}", "{}")
    typecheck("{ foo: 1 }", "{ foo: 1 }")
    typecheck("x => x.foo", "{ foo: A, ... } => A")
    typecheck("x => if (true) x.foo else x.bar", "{ foo: A, bar: A, ... } => A")

    typecheck(
      "{ foo: 1 }.bar",
      """|error: Type `{ foo: 1 }` does not have field `bar`
         |at *query*:1:12
         |  |
         |1 | { foo: 1 }.bar
         |  |            ^^^
         |  |""".stripMargin
    )

    typecheck(
      "{ foo: 1 }.foo()",
      """|error: Type `Int` cannot be used as a function
         |at *query*:1:8
         |  |
         |1 | { foo: 1 }.foo()
         |  |        ^
         |  |
         |hint: `Int` is used as a function here
         |  |
         |1 | { foo: 1 }.foo()
         |  |               ^^
         |  |""".stripMargin
    )

    // wildcard fun

    typecheck("intobj.foo", "Int | Null")
    typecheck(
      """|{
         |  let fn = x => x.foo
         |  fn(intobj)
         |}""".stripMargin,
      "Int | Null")
    typecheck("anyobj.foo", "Any")
    typecheck(
      """|{
         |  let fn = x => x.foo
         |  fn(anyobj)
         |}""".stripMargin,
      "Any")

    typecheck("foobarwild", "{ foo: String, bar: Int, *: Boolean }")
    typecheck("if (true) intobj else boolobj", "{ *: Int | Boolean }")
    typecheck(
      "if (true) intobj else foobarwild",
      "{ foo: Int | String, bar: Int, *: Int | Boolean }")

    typecheck(
      "succ(foobarwild.wut)",
      """|error: Type `Boolean | Null` is not a subtype of `Int`
         |at *query*:1:6
         |  |
         |1 | succ(foobarwild.wut)
         |  |      ^^^^^^^^^^^^^^
         |  |
         |cause: Type `Boolean` is not a subtype of `Int`
         |  |
         |1 | succ(foobarwild.wut)
         |  |      ^^^^^^^^^^^^^^
         |  |
         |cause: Type `Null` is not a subtype of `Int`
         |  |
         |1 | succ(foobarwild.wut)
         |  |      ^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck(
      "{ let fn = x => succ(x.wut); fn(foobarwild) }",
      """|error: Type `{ foo: String, bar: Int, *: Boolean }` is not a subtype of `{ wut: Int, ... }`
         |at *query*:1:33
         |  |
         |1 | { let fn = x => succ(x.wut); fn(foobarwild) }
         |  |                                 ^^^^^^^^^^
         |  |
         |cause: Type `Boolean` is not a subtype of `Int`
         |  |
         |1 | { let fn = x => succ(x.wut); fn(foobarwild) }
         |  |                                 ^^^^^^^^^^
         |  |
         |cause: Type `Null` is not a subtype of `Int`
         |  |
         |1 | { let fn = x => succ(x.wut); fn(foobarwild) }
         |  |                                 ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "type array lits tuples" in {
    typecheck("[1]", "[1]")
    typecheck("[1, 2]", "[1, 2]")
    typecheck("[]", "[]")

    typecheck("[succ(1), succ(1)]", "[Int, Int]")

    typecheck(
      "[1, 2].foo()",
      """|error: Type `Array<1 | 2>` does not have field `foo`
         |at *query*:1:8
         |  |
         |1 | [1, 2].foo()
         |  |        ^^^
         |  |""".stripMargin
    )
  }

  it should "type parens lits tuples, except single parens" in {
    typecheck("(1)", "1")
    typecheck("(1, 2)", "[1, 2]")
    typecheck("()", "[]")
  }

  it should "type array methods" in {
    typecheck("[1, 2].map(succ)", "Array<Int>")
    typecheck("""["a", "b"].map(.length)""", "Array<Int>")

    typecheck(
      "[1, 2].fold([], (arr, elem) => arr.concat([elem]))",
      // FIXME: well this is correct but sucks.
      "Array<1 | 2> | []"
    )

    typecheck(
      "['foo', 'bar'].reduce((x, y) => x + y)",
      "String | Null"
    )
  }

  it should "type tuple/array access" in {
    typecheck("[1, 2][0]", "1")
    typecheck("[1, 2].map(succ)[0]", "Int")

    typecheck("{ let x = 0; [1, 2][x] }", "1 | 2")
    typecheck("x => [1, 2][x]", "Int => 1 | 2")

    // FIXME: OOB access blows up, so type to never.
    typecheck("[1, 2][2]", "Never")
    typecheck("[][0]", "Never")

    typecheck(
      "[1, 2][-1]",
      """|error: Index -1 out of bounds for length 2
         |at *query*:1:8
         |  |
         |1 | [1, 2][-1]
         |  |        ^^
         |  |""".stripMargin
    )

    typecheck(
      "[1]['b']",
      """|error: Type `String` is not a subtype of `Int`
         |at *query*:1:5
         |  |
         |1 | [1]['b']
         |  |     ^^^
         |  |""".stripMargin
    )
  }

  it should "type tuple/array access (pending)" in {
    pendingUntilFixed {
      typecheck(
        "[][0]",
        """|error: Index 0 out of bounds for length 0
           |at *query*:1:4
           |  |
           |1 | [][0]
           |  |    ^
           |  |""".stripMargin
      )

      typecheck(
        "[1, 2][2]",
        """|error: Index 2 out of bounds for length 2
           |at *query*:1:8
           |  |
           |1 | [1, 2][2]
           |  |        ^
           |  |""".stripMargin
      )
    }
  }

  it should "typecheck string access" in {
    typecheck("'foo'[0]", "String")

    typecheck("x => 'foo'[x]", "Int => String")

    // FIXME: will fail, but bad errors
    typecheck(
      "'a'['b']",
      """|error: Type `String` is not a subtype of `Int`
         |at *query*:1:5
         |  |
         |1 | 'a'['b']
         |  |     ^^^
         |  |""".stripMargin
    )
  }

  it should "type flatMap (which checks array constraints)" in {
    typecheck("['a', 'b', 'c'].flatMap(x => [x.length, 0])", "Array<Int>")
    typecheck("[anyval].flatMap(v => v.foo)", "Array<Any>")
  }

  it should "type Any as Any" in {
    typecheck("(x => x)(anyval)", "Any")
    typecheck("anyval(1, 2, 2)", "Any")
    typecheck("anyval.foo", "Any")
    typecheck("anyval.foo(1, 2, 3)", "Any")
    typecheck("(x => {a: succ(x), b: not(x) })(anyval)", "{ a: Int, b: Boolean }")
  }

  it should "type Any with other error" in {
    // Any and Null are both in the rendered error types
    typecheck(
      """|{
         |  let a = if (true) { foo: anyval } else null
         |  let x = a?.foo
         |  Math.sum([x])
         |}""".stripMargin,
      """|error: Type `[Any | Null]` is not a subtype of `Array<Int> | Set<Int>`
         |at *query*:4:12
         |  |
         |4 |   Math.sum([x])
         |  |            ^^^
         |  |
         |cause: Type `Null` is not a subtype of `Int`
         |  |
         |4 |   Math.sum([x])
         |  |            ^^^
         |  |
         |cause: Type `[Any | Null]` is not a subtype of `Set<Int>`
         |  |
         |4 |   Math.sum([x])
         |  |            ^^^
         |  |""".stripMargin
    )
  }

  it should "type Any unioned with other" in {
    // Any unified with another type preserves the both via Union. Subsequent
    // checks must satisfy the non-Any side, but it's important to preserve the
    // existence of Any in the type for the developer.
    typecheck("if (true) 1 else anyval", "Any | 1")
    typecheck("succ(if (true) 1 else anyval)", "Int")

    typecheck(
      "not(if (true) 1 else anyval)",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:15
         |  |
         |1 | not(if (true) 1 else anyval)
         |  |               ^
         |  |""".stripMargin
    )

    typecheck(
      "(if (true) 1 else anyval).foo",
      """|error: Type `Int` does not have field `foo`
         |at *query*:1:27
         |  |
         |1 | (if (true) 1 else anyval).foo
         |  |                           ^^^
         |  |
         |hint: Type `Int` inferred here
         |  |
         |1 | (if (true) 1 else anyval).foo
         |  |            ^
         |  |""".stripMargin
    )
  }

  it should "type Any/Never unioned" in {
    typecheck(
      """|{
         |  let f: Never => Never = { let x: Any = null; x }
         |  let g: Int => Int = x => x
         |  x => if (true) f(x) else g(x)
         |}""".stripMargin,
      "Never => Int"
    )
    typecheck(
      """|{
         |  let f: Any => Any = { let x: Any = null; x }
         |  let g: Int => Int = x => x
         |  x => if (true) f(x) else g(x)
         |}""".stripMargin,
      "Int => Any | Int"
    )
    typecheck(
      """|{
         |  let f: Never => Any = { let x: Any = null; x }
         |  let g: Int => Int = x => x
         |  x => if (true) f(x) else g(x)
         |}""".stripMargin,
      "Never => Any | Int"
    )
    typecheck(
      """|{
         |  let f: Any => Never = { let x: Any = null; x }
         |  let g: Int => Int = x => x
         |  x => if (true) f(x) else g(x)
         |}""".stripMargin,
      "Int => Int"
    )
  }

  it should "type field selection" in {
    typecheck("'foo'.length", "Int")
  }

  it should "type projection" in {
    // objects
    typecheck("({ foo: 1, bar: 2, baz: 3 }) { foo, bar }", "{ foo: 1, bar: 2 }")
    typecheck(
      "({ foo: 1, bar: 2, baz: 3 }) { qux: .bar + .baz, foo, bar }",
      "{ qux: Int, foo: 1, bar: 2 }")

    // arrays
    typecheck(
      "[{ foo: 1, bar: 2, baz: 3 }] { foo, bar }",
      "Array<{ foo: 1, bar: 2 }>")
    typecheck(
      "[{ foo: 1, bar: 2, baz: 3 }] { qux: .bar + .baz, foo, bar }",
      "Array<{ qux: Int, foo: 1, bar: 2 }>")

    // refs
    typecheck(
      "{ let r: Ref<User> = User.byId(123)!; r { name } }",
      "{ name: String } | Null")

    typecheck("{ let r: User = User.byId('123')!; r { name } }", "{ name: String }")

    typecheck(
      "{ let r: EmptyRef<User> = { let x: Any = null; x }; r { name } }",
      "Null")
    typecheck(
      "{ let r: EmptyRef<User> = { let x: Any = null; x }; r { foo } }",
      """|error: Type `{ id: ID, ts: Time, ttl: Time | Null, name: String }` does not have field `foo`
         |at *query*:1:57
         |  |
         |1 | { let r: EmptyRef<User> = { let x: Any = null; x }; r { foo } }
         |  |                                                         ^^^
         |  |""".stripMargin
    )

    // legacy nulldocs
    typecheck("{ let r: NullUser = { let x: Any = null; x }; r { name } }", "Null")

    // nulls
    typecheck("null { foo, bar }", "Null")
    typecheck("null { qux: .bar + .baz, foo, bar }", "Null")

    // generic
    typecheck(
      "x => x { foo, bar }",
      ".{ { foo: A, bar: B, ... } => { foo: A, bar: B } } => C => C")

    // singleton objects
    typecheck(
      "User { all }",
      "{ all: (() => Set<User>) & ((range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<User>) }")

    // streams
    typecheck("User.all().toStream() { name }", "EventSource<{ name: String }>")

    // errors
    // FIXME: The first error should say "Cannot use projection on type `Int`",
    // instead of this nonsense.
    typecheck(
      "1 { foo, bar }",
      """|error: Type `1` is not a subtype of `.{ { foo: A, bar: B, ... } => { foo: A, bar: B } } => Any`
         |at *query*:1:1
         |  |
         |1 | 1 { foo, bar }
         |  | ^^^^^^^^^^^^^^
         |  |
         |cause: Type `Int` does not have field `foo`
         |  |
         |1 | 1 { foo, bar }
         |  |     ^^^
         |  |
         |hint: Type `Int` inferred here
         |  |
         |1 | 1 { foo, bar }
         |  | ^
         |  |
         |cause: Type `Int` does not have field `bar`
         |  |
         |1 | 1 { foo, bar }
         |  |          ^^^
         |  |
         |hint: Type `Int` inferred here
         |  |
         |1 | 1 { foo, bar }
         |  | ^
         |  |""".stripMargin
    )
  }

  it should "resolve aliases if necessary" in {
    typecheck("aFoo", "Foo")
    typecheck("aFoo.length", "Int")
  }

  it should "type operator overloads in lambdas" in {
    typecheck("[1, 2, 3].map(x => x + 1)", "Array<Int>")
    typecheck("[1, 2, 3].map(x => 1 + x)", "Array<Int>")
    typecheck("['a', 'b', 'c'].map(x => x + '1')", "Array<String>")

    typecheck(
      """|(x => {
         |  let doubled = x + x
         |  doubled * 2
         |})(7)""".stripMargin,
      "Int"
    )

    // FIXME: The causes here are redundant.
    typecheck(
      "['a', 'b', 'c'].map(x => 1 + x)",
      """|error: Type `Int => Int` is not a subtype of `"a" | "b" | "c" => Any`
         |at *query*:1:21
         |  |
         |1 | ['a', 'b', 'c'].map(x => 1 + x)
         |  |                     ^^^^^^^^^^
         |  |
         |cause: Type `String` is not a subtype of `Int`
         |  |
         |1 | ['a', 'b', 'c'].map(x => 1 + x)
         |  |  ^^^
         |  |
         |cause: Type `String` is not a subtype of `Int`
         |  |
         |1 | ['a', 'b', 'c'].map(x => 1 + x)
         |  |       ^^^
         |  |
         |cause: Type `String` is not a subtype of `Int`
         |  |
         |1 | ['a', 'b', 'c'].map(x => 1 + x)
         |  |            ^^^
         |  |""".stripMargin
    )
  }

  it should "accept equivalent aliased overloads" in {

    typecheck(
      "{ let a: IntOrBool | Boolean = aliasIntAndStr; a }",
      "IntOrBool | Boolean")

    typecheck(
      "{ let a: IntAndStr | Boolean = aliasIntAndStr; let b: IntOrBool = a; b }",
      "IntOrBool")
    typecheck(
      "{ let a: IntAndStr | Boolean = aliasIntAndStr; let b: Int | Boolean = a; b }",
      "Int | Boolean")
    typecheck(
      "{ let a: IntAndStr | Boolean = aliasIntAndStr; let b: String | Boolean = a; b }",
      "String | Boolean")

    typecheck(
      "{ let a: IntAndStr | IntOrStr | Boolean = aliasIntAndStr; let b: IntOrBool | String = a; b }",
      "IntOrBool | String")

    typecheck(
      "{ let a: StrOrBool | IntOrBool = 2; let b: Int | String | Boolean = a; b }",
      "Int | String | Boolean")
    typecheck(
      "{ let a: StrOrBool | IntOrBool = 2; let b: Int | StrOrBool = a; b }",
      "Int | StrOrBool")

    typecheck(
      "{ let a: StrOrBool | IntOrBool = 2; let b: Int | String = a; b }",
      """|error: Type `Boolean` is not a subtype of `Int | String`
         |at *query*:1:41
         |  |
         |1 | { let a: StrOrBool | IntOrBool = 2; let b: Int | String = a; b }
         |  |                                         ^
         |  |""".stripMargin
    )

    typecheck(
      "{ let a: StrOrBool | IntOrBool = 2; let b: IntOrStr = a; b }",
      """|error: Type `Boolean` is not a subtype of `Int | String`
         |at *query*:1:41
         |  |
         |1 | { let a: StrOrBool | IntOrBool = 2; let b: IntOrStr = a; b }
         |  |                                         ^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let f1: StrOrBool => Any = { let x: Any = null; x }
         |  let f2: (StrOrBool | Null) => String = { let x: Any = null; x }
         |
         |  let doit: String => String = s => {
         |    f1(s)
         |    f2(s)
         |  }
         |
         |  doit
         |}""".stripMargin,
      "String => String"
    )
  }

  it should "check Ref aliases" in {
    typecheck(
      "{ let a: Ref<User> = null; let b: User | NullUser = a; b }",
      "User | NullUser")
  }

  it should "prune uninhabited types from overload inputs (pending)" in {

    // FIXME: should prune uninhabited input types on both of these

    typecheck(
      "x => intBool(if (true) intBool(x) else intBool(x))",
      "(Int => Int) & (Int & Boolean => Int | Boolean) & (Boolean & Int => Int | Boolean) & (Boolean => Boolean)"
    )
    typecheck(
      "(x => intIntBoolAny(x, intIntBoolAny(x, 1)))",
      "(Int => Int) & (Int & Boolean => Boolean) & (Boolean => Boolean)")

    // what we really want
    pendingUntilFixed {
      typecheck(
        "x => intBool(if (true) intBool(x) else intBool(x))",
        "(Int => Int) & (Boolean => Boolean)"
      )
      typecheck(
        "(x => intIntBoolAny(x, intIntBoolAny(x, 1)))",
        "(Boolean => Boolean) & (Int => Int)")
    }
  }

  it should "type overloaded functions/func intersections" in {

    typecheck("intBool", "(Int => Int) & (Boolean => Boolean)")
    typecheck("intBool(1)", "Int")
    typecheck("intBool(true)", "Boolean")

    typecheck(
      "(x => { let y = if (true) true else x; intBool(y) })(1)",
      "Int | Boolean")

    typecheck("intOrIntObj(1)", "Int")
    typecheck("intOrIntObj({ a: 1 })", "{ a: Int }")
    typecheck("intOrIntObj(if (true) 1 else { a: 1 })", "Int | { a: Int }")

    typecheck(
      "x => intOrIntBoolObj({ a: x })",
      "(Int => Int) & (Boolean => Boolean)")
    typecheck(
      "x => intOrIntBoolObj(if (true) 1 else { a: x })",
      "(Int => Int | Boolean) & (Boolean => Int | Boolean)")

    typecheck("x => intBool(x)", "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => intBool(x))(2)", "Int")
    typecheck("(x => intBool(x))(true)", "Boolean")

    typecheck(
      """|x => {
         | let y: (String => String) = intStr
         | y(x)
         |}""".stripMargin,
      "String => String"
    )

    // FIXME (should be pretty easy to collapse these not a subtype errors where the
    // callsite is the same.)
    typecheck(
      "intBool('foo')",
      """|error: Type `String` is not a subtype of `Boolean | Int`
         |at *query*:1:9
         |  |
         |1 | intBool('foo')
         |  |         ^^^^^
         |  |""".stripMargin
    )
    typecheck(
      "(x => intBool(x))('foo')",
      """|error: Type `String` is not a subtype of `Boolean | Int`
         |at *query*:1:19
         |  |
         |1 | (x => intBool(x))('foo')
         |  |                   ^^^^^
         |  |""".stripMargin
    )

    // two applications
    typecheck("intBool(intBool(2))", "Int")
    typecheck("intBool(intBool(true))", "Boolean")
    typecheck("x => intBool(intBool(x))", "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => intBool(intBool(x)))(2)", "Int")
    typecheck("(x => intBool(intBool(x)))(true)", "Boolean")
    typecheck(
      "(x => intBool(intBool(x)))('foo')",
      """|error: Type `String` is not a subtype of `Boolean | Int`
         |at *query*:1:28
         |  |
         |1 | (x => intBool(intBool(x)))('foo')
         |  |                            ^^^^^
         |  |""".stripMargin
    )

    // narrows correctly
    typecheck("x => intBoolStr(intBool(x))", "(Int => Int) & (Boolean => Boolean)")
    typecheck("intBoolStr(intBool(1))", "Int")
    typecheck("intBool(intBoolStr(1))", "Int")

    typecheck(
      "intBool(intBoolStr('foo'))",
      """|error: Type `String` is not a subtype of `Int | Boolean`
         |at *query*:1:9
         |  |
         |1 | intBool(intBoolStr('foo'))
         |  |         ^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("succ(intBool(2))", "Int")
    typecheck(
      "succ(intBool(true))",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:6
         |  |
         |1 | succ(intBool(true))
         |  |      ^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("x => succ(intBool(x))", "Int => Int")
    typecheck("(x => succ(intBool(x)))(2)", "Int")
    typecheck(
      "(x => succ(intBool(x)))(true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:25
         |  |
         |1 | (x => succ(intBool(x)))(true)
         |  |                         ^^^^
         |  |""".stripMargin
    )
    typecheck(
      "(x => succ(intBool(x)))(if (true) 1 else true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:42
         |  |
         |1 | (x => succ(intBool(x)))(if (true) 1 else true)
         |  |                                          ^^^^
         |  |""".stripMargin
    )

    typecheck("x => succ(intBool(intBool(x)))", "Int => Int")
    typecheck("(x => succ(intBool(intBool(x))))(2)", "Int")
    typecheck(
      "(x => succ(intBool(intBool(x))))(true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:34
         |  |
         |1 | (x => succ(intBool(intBool(x))))(true)
         |  |                                  ^^^^
         |  |""".stripMargin
    )

    typecheck("succ(intBoolStr(intBool(1)))", "Int")
    typecheck(
      "{ a: x => intBool(x), b: intBool(true) }",
      "{ a: Int => Int, b: Boolean } & { a: Boolean => Boolean, b: Boolean }")

    typecheck("x => succ(intBool(intBoolStr(x)))", "Int => Int")
    typecheck("x => intBoolStr(intBool(succ(x)))", "Int => Int")
    typecheck("x => intBoolStr(intBool(x))", "(Int => Int) & (Boolean => Boolean)")
    typecheck(
      "(x => y => intBoolStr(x(y)))(intBool)",
      "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => y => x(x(y)))(intBool)", "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => y => succ(x(x(y))))(intBool)", "Int => Int")

    typecheck("x => strBool(intBool(x))", "Boolean => Boolean")

    // FIXME: why doesn't the value type reduce?
    typecheck(
      "x => strBool(intBool(intStr(x)))",
      """|error: Type `Int` is not a subtype of `String | Boolean`
         |at *query*:1:14
         |  |
         |1 | x => strBool(intBool(intStr(x)))
         |  |              ^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck(
      "x => strBool(intBool(intStr(succ(x))))",
      """|error: Type `Int` is not a subtype of `String | Boolean`
         |at *query*:1:14
         |  |
         |1 | x => strBool(intBool(intStr(succ(x))))
         |  |              ^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("x => intBoolObj({ a: x })", "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => intBoolObj({ a: x }))(2)", "Int")
    typecheck(
      "x => intBoolObj({ a: intBool(x) })",
      "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => intBoolObj({ a: intBool(x) }))(2)", "Int")

    typecheck("intBool(if (true) 1 else true)", "Int | Boolean")
    typecheck("intBoolStr(intBool(if (true) 1 else true))", "Int | Boolean")
    typecheck("intBoolStr(if (true) 1 else true)", "Int | Boolean")
    typecheck("intBoolStr(if (true) intBool(1) else 'foo')", "Int | String")
    typecheck(
      "{ a: intBool(if (true) 1 else true), b: intBool(if (true) 1 else true) }",
      "{ a: Int | Boolean, b: Int | Boolean }")
    typecheck(
      "intBoolStr(if (true) (if (true) 1 else true) else 'foo')",
      "Int | Boolean | String")
    typecheck(
      "intBoolStr(if (true) intBool(if (true) 1 else true) else 'foo')",
      "Int | Boolean | String")
    typecheck(
      "intBoolStr(if (true) intBoolStr(if (true) 1 else true) else 'foo')",
      "Int | Boolean | String")

    typecheck(
      "x => intBoolStr(if (true) intBool(if (true) 1 else x) else 'foo')",
      "(Int => Int | String) & (Boolean => Int | Boolean | String)")
    typecheck(
      "x => intBoolStr(if (true) intBool(if (true) intBool(x) else 1) else 'foo')",
      "(Int => Int | String) & (Boolean => Int | Boolean | String)")
    typecheck(
      "(x => intBoolStr(if (true) intBool(if (true) intBool(x) else 1) else 'foo'))(true)",
      "Int | Boolean | String")
    typecheck(
      "x => intBoolStr(if (true) intBool(if (true) strBool(x) else 1) else 'foo')",
      "Boolean => Int | Boolean | String")
    typecheck(
      "(x => intBoolStr(if (true) intBool(if (true) strBool(x) else 1) else 'foo'))(true)",
      "Int | Boolean | String")

    typecheck(
      "(x, y) => intBool(if (true) intStr(x) else boolNull(y))",
      "(Int, Boolean) => Int | Boolean")

    typecheck(
      "intBool(if (true) 1 else null)",
      """|error: Type `Null` is not a subtype of `Boolean | Int`
         |at *query*:1:26
         |  |
         |1 | intBool(if (true) 1 else null)
         |  |                          ^^^^
         |  |""".stripMargin
    )

    typecheck(
      "x => intBool(if (true) 1 else x)",
      "(Int => Int) & (Boolean => Int | Boolean)")
    typecheck(
      "x => intBool(intBool(if (true) 1 else x))",
      "(Int => Int) & (Boolean => Int | Boolean)")

    // FIXME: would be better to say "Type `Int | Boolean` is not a subtype of
    // `Int`"
    typecheck(
      "x => succ(intBool(if (true) true else x))",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:11
         |  |
         |1 | x => succ(intBool(if (true) true else x))
         |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("x => succ(intBool(if (true) 1 else x))", "Int => Int")
    typecheck("(x => x(if (true) 1 else true))(intBool)", "Int | Boolean")
    typecheck(
      "(x => x(if (true) 1 else true))(y => intBoolStr(intBool(y)))",
      "Int | Boolean")
    typecheck(
      "x => intBoolStr(if (true) intBool(x) else 'foo')",
      "(Int => Int | String) & (Boolean => Boolean | String)")
    typecheck(
      "x => intBoolStr(if (true) intBool(intBool(x)) else 'foo')",
      "(Int => Int | String) & (Boolean => Boolean | String)")

    // FIXME: would be better to say "Type `Int | Boolean` is not a subtype of
    // `Int`"
    typecheck(
      "x => intBool(succ(intBool(if (true) true else x)))",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:19
         |  |
         |1 | x => intBool(succ(intBool(if (true) true else x)))
         |  |                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck("x => intBool(succ(intBool(if (true) 1 else x)))", "Int => Int")

    typecheck("(x => intBool(x))(if (true) 1 else true)", "Int | Boolean")

    typecheck(
      "(x, y) => { a: intBool(if (true) 1 else x), b: intBool(if (true) true else y) }",
      "((Int, Int) => { a: Int, b: Int | Boolean }) & " +
        "((Int, Boolean) => { a: Int, b: Boolean }) & " +
        "((Boolean, Int) => { a: Int | Boolean, b: Int | Boolean }) & " +
        "((Boolean, Boolean) => { a: Int | Boolean, b: Boolean })"
    )

    typecheck(
      "{ a: x => intBool(x), b: intBool(true) }",
      "{ a: Int => Int, b: Boolean } & { a: Boolean => Boolean, b: Boolean }")

    typecheck(
      "x => { a: intBool(if (true) 1 else 1), b: x }",
      "A => { a: Int, b: A }")
    typecheck(
      "x => { a: intBool(if (true) 1 else x), b: x }",
      // this is a case where a and b should be eliminated, since `_ & Int` should be
      // `Int` and `_ & Boolean` should be `Boolean`
      "(A & Int => { a: Int, b: A }) & (B & Boolean => { a: Int | Boolean, b: B })"
    )

    typecheck("x => y => [x(true), x(y)]", "(A | true => B) => A => [B, B]")
    typecheck(
      "(x => y => [x(true), x(y)])(intBool)",
      "(Int => [Int | Boolean, Int | Boolean]) & (Boolean => [Boolean, Boolean])")
    typecheck(
      "(x => y => [x(true), x(y)])(intBool)(1)",
      "[Int | Boolean, Int | Boolean]")
    typecheck("(x => y => [x(true), x(y)])(intBool)(false)", "[Boolean, Boolean]")

    // ib uses do not get specific types w/ no let
    // FIXME: this seems like it would be simpler to unify to
    // Boolean | Int => { a: Boolean | Int, b: Boolean | Int }
    typecheck(
      "(x => y => { a: x(y), b: x(x(if (true) 1 else 1)) })(intBool)",
      "(Int => { a: Int, b: Int }) & (Boolean => { a: Int | Boolean, b: Int | Boolean })"
    )

    // ib uses do not get specific types w/ no let
    typecheck(
      "(ib => { a: ib(true), b: ib(1) })(x => intBool(x))",
      "{ a: Int | Boolean, b: Int | Boolean }")

    // ib uses get specific types due to let polymorphism
    typecheck(
      "{ let ib = x => intBool(x); { a: ib(true), b: ib(1) } }",
      "{ a: Boolean, b: Int }")

    // intBool is only instantiated once, so no specific types
    typecheck(
      "(x => { let ib = y => x(y); { a: ib(true), b: ib(1) } })(intBool)",
      "{ a: Int | Boolean, b: Int | Boolean }")

    // let involved
    typecheck(
      "x => { let ib = intBool; [ib(x), ib(if (true) 1 else true)] }",
      "(Int => [Int, Int | Boolean]) & (Boolean => [Boolean, Int | Boolean])")

    // no let so outputs are unified, but we still narrow inputs
    typecheck(
      "(ib => x => [ib(x), ib(if (true) 1 else true)])(intBool)",
      "(Int => [Int | Boolean, Int | Boolean]) & (Boolean => [Int | Boolean, Int | Boolean])")

    typecheck("gener(1, x => intBool(x))", "Int")
    typecheck("gener(1, intBool)", "Int")
    typecheck("x => gener(x, intBool)", "(Int => Int) & (Boolean => Boolean)")
    typecheck("(x => gener(x, intBool))(2)", "Int")
    typecheck("(x => gener(x, intBool))(true)", "Boolean")
    typecheck(
      "(x => gener(x, intBool))('foo')",
      """|error: Type `String` is not a subtype of `Boolean | Int`
         |at *query*:1:26
         |  |
         |1 | (x => gener(x, intBool))('foo')
         |  |                          ^^^^^
         |  |""".stripMargin
    )

    typecheck("gener(if (true) 1 else true, x => intBool(x))", "Int | Boolean")
    typecheck("gener(if (true) 1 else true, intBool)", "Int | Boolean")

    typecheck(
      "(x => x(if (true) 2 else x))(intBool)",
      """|error: Type `(Int => Int) & (Boolean => Boolean)` is not a subtype of `A & (A | 2 => Any)`
         |at *query*:1:30
         |  |
         |1 | (x => x(if (true) 2 else x))(intBool)
         |  |                              ^^^^^^^
         |  |
         |cause: Type `Boolean => Boolean` is not a subtype of `Boolean`
         |  |
         |1 | (x => x(if (true) 2 else x))(intBool)
         |  |                              ^^^^^^^
         |  |
         |cause: Type `Int => Int` is not a subtype of `Int`
         |  |
         |1 | (x => x(if (true) 2 else x))(intBool)
         |  |                              ^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "type overloaded HO functions" in {

    typecheck("intBoolHOF(succ)", "Int => Int")
    typecheck("intBoolHOF(x => succ(intBool(x)))", "Int => Int")
    typecheck("intBoolHOF(x => intStr(intBool(x)))", "Int => Int")
    typecheck(
      "intBoolStrHOF(x => intBool(x))",
      "(Boolean => Boolean) & (Int => Int)")
    typecheck("intBoolHOF(x => intBool(x))", "(Boolean => Boolean) & (Int => Int)")
    typecheck("(x => intBoolHOF(x))(succ)", "Int => Int")
    typecheck(
      "(x => intBoolHOF(x))(reverse)",
      """|error: Type `String => String` is not a subtype of `(Int => Int) | (Boolean => Boolean)`
         |at *query*:1:22
         |  |
         |1 | (x => intBoolHOF(x))(reverse)
         |  |                      ^^^^^^^
         |  |
         |cause: Type `Boolean` is not a subtype of `String`
         |  |
         |1 | (x => intBoolHOF(x))(reverse)
         |  |                      ^^^^^^^
         |  |
         |cause: Type `Int` is not a subtype of `String`
         |  |
         |1 | (x => intBoolHOF(x))(reverse)
         |  |                      ^^^^^^^
         |  |
         |cause: Type `String` is not a subtype of `Boolean | Int`
         |  |
         |1 | (x => intBoolHOF(x))(reverse)
         |  |                      ^^^^^^^
         |  |""".stripMargin
    )

    typecheck("intBoolStrHOF(intBool)", "(Boolean => Boolean) & (Int => Int)")
    typecheck("intBoolHOF(intBool)", "(Boolean => Boolean) & (Int => Int)")
  }

  it should "type intersections of interfaces" in {
    typecheck(
      """|{
         |  let obj: { a: Int, ... } & { b: Int, ... } = { a: 2, b: 3 }
         |  obj.a
         |}
         |""".stripMargin,
      "Int"
    )

    typecheck(
      """|{
         |  let obj: { a: Int, ... } & { b: String, ... } = { a: 2, b: "hi" }
         |  obj { a }
         |}
         |""".stripMargin,
      "{ a: Int }"
    )
  }

  it should "type intersections of interfaces (pending)" in pendingUntilFixed {
    typecheck(
      """|{
         |  let obj: { a: Int, ... } & { b: String, ... } = { a: 2, b: "hi" }
         |  obj { a, b }
         |}
         |""".stripMargin,
      "{ a: Int, b: String }"
    )
  }

  it should "type enums/union overloads" in {
    typecheck("enumABC", """"a" | "b" | "c" => Int""")
    typecheck("enumABC('a')", "Int")
    typecheck("{ let a = 'a'; enumABC('a') }", "Int")
    typecheck("(x => enumABC(x))('a')", "Int")
    typecheck(
      "enumABC('d')",
      """|error: Type `"d"` is not a subtype of `"a" | "b" | "c"`
         |at *query*:1:9
         |  |
         |1 | enumABC('d')
         |  |         ^^^
         |  |
         |cause: `"d"` is not the value `"a"`
         |  |
         |1 | enumABC('d')
         |  |         ^^^
         |  |
         |cause: `"d"` is not the value `"b"`
         |  |
         |1 | enumABC('d')
         |  |         ^^^
         |  |
         |cause: `"d"` is not the value `"c"`
         |  |
         |1 | enumABC('d')
         |  |         ^^^
         |  |""".stripMargin
    )

    typecheck("enumABCObj", """{ x: "a" } | { x: "b" } | { x: "c" } => Int""")
    typecheck("enumABCObj({ x: 'a' })", "Int")
    typecheck("{ let a = 'a'; enumABCObj({ x: a }) }", "Int")
    typecheck("(x => enumABCObj({ x: x }))('a')", "Int")
    typecheck("(x => { let a = x; enumABCObj({ x: a }) })('a')", "Int")

    typecheck(
      """|{
         |  let fn: (Array<{ a: 'x' | 'y' }>) => Int = x => 1
         |  fn([{ a: 'x' }, { a: 'y' }])
         |}""".stripMargin,
      "Int")
  }

  it should "typecheck free overloads" in {
    typecheck(
      "i1 => x => intBool(i1(x))",
      "((A => Int) => A => Int) & ((B => Boolean) => B => Boolean)")
    typecheck(
      "(i1 => x => intBool(i1(x)))(intBool)",
      "(Int => Int) & (Boolean => Boolean)")
    typecheck("(i1 => x => intBool(i1(x)))(intBool)(2)", "Int")
    typecheck("(i1 => x => intBool(i1(x)))(intBool)(true)", "Boolean")
    typecheck(
      "(i1 => x => intBool(i1(x)))(intBool)('foo')",
      """|error: Type `String` is not a subtype of `Int | Boolean`
         |at *query*:1:38
         |  |
         |1 | (i1 => x => intBool(i1(x)))(intBool)('foo')
         |  |                                      ^^^^^
         |  |""".stripMargin
    )

    typecheck(
      "i1 => { let i2 = x => intBool(i1(x)); i2 }",
      "((A => Int) => (A => Int)) & ((B => Boolean) => (B => Boolean))")
    typecheck(
      "(i1 => { let i2 = x => intBool(i1(x)); i2 })(intBool)",
      "(Int => Int) & (Boolean => Boolean)")
    typecheck("(i1 => { let i2 = x => intBool(i1(x)); i2 })(intBool)(2)", "Int")
    typecheck(
      "(i1 => { let i2 = x => intBool(i1(x)); i2 })(intBool)(true)",
      "Boolean")
    typecheck(
      "(i1 => { let i2 = x => intBool(i1(x)); i2 })(intBool)('foo')",
      """|error: Type `String` is not a subtype of `Boolean | Int`
         |at *query*:1:55
         |  |
         |1 | (i1 => { let i2 = x => intBool(i1(x)); i2 })(intBool)('foo')
         |  |                                                       ^^^^^
         |  |""".stripMargin
    )
  }

  it should "type overloaded functions/func intersections (pending)" in {
    // these fit, but a more precise type would be the one marked pendingUntilFixed
    typecheck(
      "x => y => intBool(if (true) x else y)",
      "(Int => Int => Int) & (Boolean => Boolean => Boolean)")

    typecheck(
      "x => y => intBool(intBool(if (true) x else y))",
      "(Int => Int => Int) & (Boolean => Boolean => Boolean)")

    pendingUntilFixed {
      typecheck(
        "x => y => intBool(if (true) x else y)",
        "(Int => Int => Int) & " +
          "(Int => Boolean => Int | Boolean) & " +
          "(Boolean => Int => Int | Boolean) & " +
          "(Boolean => Boolean => Boolean)"
      )
    }
  }

  it should "type overloaded 2-arity functions/func intersections" in {

    typecheck(
      "(x, y) => intIntBoolBool(x, y)",
      "((Int, Int) => Int) & ((Boolean, Boolean) => Boolean)")

    typecheck("intIntBoolBool(1, 1)", "Int")
    typecheck("intIntBoolBool(intBool(1), intBool(1))", "Int")
    typecheck(
      "intIntBoolBool(1, true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:19
         |  |
         |1 | intIntBoolBool(1, true)
         |  |                   ^^^^
         |  |""".stripMargin
    )
    typecheck("intIntBoolBool(1, intBool(true))", "ERROR")
    typecheck("intIntBoolBool(intBool(1), intBool(true))", "ERROR")
    typecheck(
      "intIntBoolBool(intBool(if (true) 1 else true), intBool(if (true) 1 else true))",
      "ERROR")
    typecheck(
      "intIntBoolBool(intBool(if (true) intBool(1) else intBool(true)), intBool(if (true) intBool(1) else intBool(true)))",
      "ERROR")

    typecheck("x => intIntBoolBool(x, 1)", "Int => Int")
    typecheck("(x, y) => intIntBoolBool(x, if (true) 1 else y)", "(Int, Int) => Int")
    typecheck(
      "x => intIntBoolBool(if (true) intBool(1) else x, intBool(1))",
      "Int => Int")
    typecheck(
      "x => intIntBoolBool(intBool(if (true) 1 else x), intBool(1))",
      "Int => Int")

    typecheck(
      "(x, y) => intIntBoolBool(intBool(x), intBool(y))",
      "((Int, Int) => Int) & ((Boolean, Boolean) => Boolean)")

    typecheck(
      "((x, y) => intIntBoolBool(intBool(x), intBool(y)))(1, true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:55
         |  |
         |1 | ((x, y) => intIntBoolBool(intBool(x), intBool(y)))(1, true)
         |  |                                                       ^^^^
         |  |""".stripMargin
    )
    typecheck(
      "((x, y) => intIntBoolBool(intBool(x), y))(1, true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:46
         |  |
         |1 | ((x, y) => intIntBoolBool(intBool(x), y))(1, true)
         |  |                                              ^^^^
         |  |""".stripMargin
    )
    typecheck(
      "(x, y) => intIntBoolBool(intBool(x), intBool(if (true) 1 else y))",
      "(Int, Int) => Int")

    typecheck(
      "(x => intIntBoolBool(x, true))(2)",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:32
         |  |
         |1 | (x => intIntBoolBool(x, true))(2)
         |  |                                ^
         |  |""".stripMargin
    )
    typecheck(
      "(x => intIntBoolBool(x, 1))(true)",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:29
         |  |
         |1 | (x => intIntBoolBool(x, 1))(true)
         |  |                             ^^^^
         |  |""".stripMargin
    )

    // FIXME: Special-case exclusivity error.
    typecheck(
      "intIntBoolBool(intBool(if (true) 1 else true), intBool(if (true) 1 else true))",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:16
         |  |
         |1 | intIntBoolBool(intBool(if (true) 1 else true), intBool(if (true) 1 else true))
         |  |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |
         |
         |error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:48
         |  |
         |1 | intIntBoolBool(intBool(if (true) 1 else true), intBool(if (true) 1 else true))
         |  |                                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // all fail, but errors are bad as above
    typecheck("intIntBoolBool(if (true) true else 1, if(true) true else 1)", "ERROR")
    typecheck("x => intIntBoolBool(x, if (true) 1 else true)", "ERROR")

    // FIXME: and another bad one
    typecheck("(x, y) => intIntBoolBool(intStr(x), boolNull(y))", "ERROR")

    // FIXME we could accept this, because the argument is the same type.
    typecheck(
      "(x => intIntBoolBool(x, x))(if (true) 1 else true)",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:39
         |  |
         |1 | (x => intIntBoolBool(x, x))(if (true) 1 else true)
         |  |                                       ^
         |  |""".stripMargin
    )

    typecheck(
      "(x, y) => intIntBoolBoolCurried(x)(y)",
      "((Int, Int) => Int) & ((Boolean, Boolean) => Boolean)")
    typecheck("x => intIntBoolBoolCurried(x)(2)", "Int => Int")
    typecheck(
      "(x, y) => intIntBoolBoolCurried(x)(if (true) 1 else y)",
      "(Int, Int) => Int")

    typecheck(
      "(x => intIntBoolBoolCurried(x)(true))(2)",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:39
         |  |
         |1 | (x => intIntBoolBoolCurried(x)(true))(2)
         |  |                                       ^
         |  |""".stripMargin
    )

    // fail, but error is bad
    typecheck("x => intIntBoolBoolCurried(x)(if (true) 1 else true)", "ERROR")

    // will fail because arity doesn't get recalculated
    typecheck("(x => intIntBoolBoolCurried(x)(x))(if (true) 1 else true)", "ERROR")

    typecheck(
      "(x0, y0) => ((x, y) => intIntBoolBool(intBool(x), y))(x0, intBool(if (true) 1 else y0))",
      "(Int, Int) => Int")

    typecheck(
      "(x => { let y = if (true) true else x; intIntBoolBoolCurried(y)(1) })",
      """|error: Type `Int` is not a subtype of `Boolean`
         |at *query*:1:65
         |  |
         |1 | (x => { let y = if (true) true else x; intIntBoolBoolCurried(y)(1) })
         |  |                                                                 ^
         |  |""".stripMargin
    )
    // will fail, bad error
    typecheck(
      "x => (y => intIntBoolBoolCurried(y)(1))(if (true) true else x)",
      "ERROR")

    typecheck("x => intIntBoolBoolCurried(if (true) 1 else x)", "Int => Int => Int")
    typecheck(
      "x => { let foo = 1; intIntBoolBoolCurried(if (true) foo else x)}",
      "Int => Int => Int")

    // passes because let RHS arity is recalcuated when it is "annealed".
    typecheck(
      "{ let f = x => intIntBoolBool(x, x); f(if (true) 1 else true) }",
      "Boolean | Int")

    // passes because let RHS arity is recalcuated when it is "annealed".
    typecheck(
      "{ let f = x => intIntBoolBoolCurried(x)(x); f(if (true) 1 else true) }",
      "Boolean | Int")
  }

  it should "type variadic functions" in {
    typecheck("intStrVarArg(3)", "Int")
    typecheck("intStrVarArg(3, 'a')", "Int")
    typecheck("intStrVarArg(3, 'a', 'b')", "Int")
    typecheck(
      "intStrVarArg(3, 4)",
      """|error: Type `Int` is not a subtype of `String`
         |at *query*:1:17
         |  |
         |1 | intStrVarArg(3, 4)
         |  |                 ^
         |  |""".stripMargin
    )
    typecheck(
      "intStrVarArg()",
      """|error: Function was not called with enough arguments. Expected 1, received 0
         |at *query*:1:13
         |  |
         |1 | intStrVarArg()
         |  |             ^^
         |  |""".stripMargin
    )

    typecheck(
      "if (true) intStrVarArg else intintint",
      "((Int, ...String) => Int) | ((Int, Int, Int) => Int)"
    )
    typecheck(
      "if (true) intStrVarArg else succ",
      "((Int, ...String) => Int) | (Int => Int)"
    )
  }

  // ported over more interesting simple-sub tests

  it should "type self-application" in {
    typecheck("x => x(x)", "A & (A => B) => B")

    typecheck("x => x(x)(x)", "A & (A => A => B) => B")
    typecheck("x => y => x(y)(x)", "A & (B => A => C) => B => C")
    typecheck("x => y => x(x)(y)", "A & (A => B => C) => B => C")
    typecheck("(x => x(x))(x => x(x))", "Never")

    typecheck("x => { l: x(x), r: x }", "A & (A => B) => { l: B, r: A }")

    // From https://github.com/stedolan/mlsub
    // Y combinator:
    typecheck("(f => (x => f (x(x))) (x => f(x(x))))", "(A => A) => A")
    // Z combinator:
    typecheck(
      "f => (x => f(v => x(x)(v)))(x => f(v => x(x)(v)))",
      "((A => B) => C & (A => B)) => C")
    // Function that takes arbitrarily many arguments:
    typecheck(
      "(f => (x => f(v => x (x)(v)))(x => f(v => x(x)(v))))(f => x => f)",
      "Any => (Any => A) as A")

    // TODO: we don't support letrec
    // typecheck(
    //   "let rec trutru = g => trutru (g true) in trutru",
    //   "(bool => 'a) as 'a => ⊥")

    typecheck(
      "i => if (i(i)(true)) false else true",
      "A & (A => true => Boolean) => Boolean")
    // ^ for: λi. if ((i i) true) then true else true,
    //    Dolan's thesis says MLsub infers: (α → ((bool → bool) ⊓ α)) → bool
    //    which does seem equivalent, despite being quite syntactically different
  }

  it should "type let polymorphism" in {
    typecheck("{ let f = x => x; { a: f(0), b: f(true)} }", "{ a: 0, b: true }")
    typecheck(
      "y => { let f = x => x; { a: f(y), b: f(true) } }",
      "A => { a: A, b: true }")
    typecheck(
      "y => { let f = x => y(x); { a: f(0), b: f(true) } }",
      "(true | 0 => A) => { a: A, b: A }")
    typecheck(
      "y => { let f = x => x(y); { a: f(z => z), b: f(z => true) } }",
      "A => { a: A, b: true }")

    typecheck(
      "y => { let f = x => x(y); {a: f(z => z), b: f(z => succ(z)) } }",
      "A & Int => { a: A, b: Int }")

    // Simple example of extruding type variables constrained both ways:
    typecheck(
      "k => { let test = k(x => { let tmp = add(x, 1); x}); test }",
      "((A & Int => A) => B) => B")
    // MLsub: ((((Int & a) -> a) -> b) -> b)

    // Adapted to exhibit a problem if we use the old extrusion algorithm:
    typecheck(
      "k => { let test = k(x => { let tmp = add(x, 1); if (true) x else succ(2) }); test }",
      "((Int => Int) => A) => A"
    )
    // MLsub: ((((Int & a) -> (Int | a)) -> b) -> b)

    // Example loss of polymorphism due to extrusion – the identity function becomes
    // less polymorphic:
    typecheck(
      "k => { let test = (id => { tmp: k(id), res: id }.res)(x => x); { u: test(0), v: test(true) } }",
      // TODO: "((a => a | bool | Int) => Any) => { u: a | Int, v: a | bool }"
      "((A => A | true | 0) => Any) => { u: A | 0, v: A | true }"
    )
    // MLsub: (((a -> (bool | Int | a)) -> Top) -> {u : (Int | a), v : (bool | a)})

    // Compared with this version: (MLsub still agrees)
    typecheck(
      "k => { let test = { tmp: k(x => x), res: (x => x) }.res; { u: test(0), v: test(true) } }",
      "((A => A) => Any) => { u: 0, v: true }"
    )

    typecheck(
      "k => { let test = (thefun => {l: k(thefun), r: thefun(1) })(x => { let tmp = add(x, 1); x }); test }",
      "((A & Int => A | 1) => B) => { l: B, r: A | 1 }"
    )

    typecheck(
      "a => (k => { let test = k (x => { let tmp = add(x, 1); x });test })(f => f(a))",
      "A & Int => A")

    typecheck(
      "k => { let test = k(x => { let tmp = (y => add(y, 1))(x); x }); test}",
      "((A & Int => A) => B) => B")

    typecheck(
      "(k => { let test = k (x => { let tmp = { let f = y => add(y, 1); f(x) }; x }); test })",
      "((A & Int => A) => B) => B")

    typecheck(
      "f => { let r = x => g => { a: f(x), b: g(x) }; r }",
      "(A => B) => A => (A => C) => { a: B, b: C }")

    typecheck(
      "f => { let r = x => g => { a: g(x) }; {u: r(0)(succ), v: r(true)(not) } }",
      "Any => { u: { a: Int }, v: { a: Boolean } }")
    // MLsub:
    // let res = fun f -> let r = fun x -> fun g -> { a = g x } in {u = r 0 (fun n ->
    // n + 1); v = r {t=true} (fun y -> y.t)}
    //   val res : (Top -> {u : {a : Int}, v : {a : bool}})

    typecheck(
      "f => { let r = x => g => { a: g(x), b: f(x) }; {u: r(0)(succ), v: r(true)(not) } }",
      "(true | 0 => A) => { u: { a: Int, b: A }, v: { a: Boolean, b: A } }"
    )

    typecheck(
      "f => { let r = x => g => { a: g(x), b: f(x) }; { u: r(0)(succ), v: r({ t: true })(y => y.t) } }",
      "(0 | { t: true } => A) => { u: { a: Int, b: A }, v: { a: true, b: A } }"
    )
    // MLsub:
    // let res = fun f -> let r = fun x -> fun g -> { a = g x; b = f x } in {u = r 0
    // (fun n -> n + 1); v = r {t=true} (fun y -> y.t)}
    // val res : (({t : bool} | Int -> a) -> {u : {a : Int, b : a}, v : {a : bool, b
    // : a}})

    typecheck(
      "(k => k (x => { let tmp = add(x, 1); x }))(f => f(true))",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:51
         |  |
         |1 | (k => k (x => { let tmp = add(x, 1); x }))(f => f(true))
         |  |                                                   ^^^^
         |  |""".stripMargin
    )

    typecheck(
      "(k => { let test = k(x => { let tmp = add(x, 1); x }); test })(f => f(true))",
      """|error: Type `Boolean` is not a subtype of `Int`
         |at *query*:1:71
         |  |
         |1 | (k => { let test = k(x => { let tmp = add(x, 1); x }); test })(f => f(true))
         |  |                                                                       ^^^^
         |  |""".stripMargin
    )
  }

  it should "type let polymorphism (pending 1)" in {
    typecheck(
      "k => { let test = k(x => { let tmp = add(x, 1); if (true) x else 2 }); test }",
      "((A & Int => A | 2) => B) => B")

    pendingUntilFixed {
      typecheck(
        "k => { let test = k(x => { let tmp = add(x, 1); if (true) x else 2 }); test }",
        // FIXME: see variant of this test above. The result of this check is
        // ((a & Int => a | 2) => b) => b, rather than below result.
        "((Int => Int) => A) => A"
      )
    }
  }

  // FIXME: no let rec, so can't test these without recursive function defs.
  it should "type recursion (pending)" in {
    pendingUntilFixed {
      typecheck("let rec f = fun x -> f x.u in f", "{u: 'a} as 'a -> ⊥")

      // [test:T2]:
      typecheck("let rec r = fun a -> r in if true then r else r", "(⊤ -> 'a) as 'a")
      // ^ without canonicalization, we get the type:
      //    ⊤ -> (⊤ -> 'a) as 'a ∨ (⊤ -> 'b) as 'b
      typecheck(
        "let rec l = fun a -> l in let rec r = fun a -> fun a -> r in if true then l else r",
        "(⊤ -> ⊤ -> 'a) as 'a")
      // ^ without canonicalization, we get the type:
      //    ⊤ -> (⊤ -> 'a) as 'a ∨ (⊤ -> (⊤ -> ⊤ -> 'b) as 'b)
      typecheck(
        "let rec l = fun a -> fun a -> fun a -> l in let rec r = fun a -> fun a -> r in if true then l else r",
        "(⊤ -> ⊤ -> ⊤ -> ⊤ -> ⊤ -> ⊤ -> 'a) as 'a"
      ) // 6 is the LCD of 3 and 2
      // ^ without canonicalization, we get the type:
      //    ⊤ -> ⊤ -> (⊤ -> ⊤ -> 'a) as 'a ∨ (⊤ -> (⊤ -> ⊤ -> ⊤ -> 'b) as 'b)

      // from https://www.cl.cam.ac.uk/~sd601/mlsub/
      typecheck(
        "let rec recursive_monster = fun x -> { thing = x; self = recursive_monster x } in recursive_monster",
        "'a -> {self: 'b, thing: 'a} as 'b")
    }
  }

  it should "type simple-sub random exprs" in {
    typecheck("x => { let y = x(x); 0 }", "A & (A => Any) => 0")

    typecheck("next => 0", "Any => 0")

    typecheck("(x => x(x))(x => x)", "(A | (A => B)) as B")

    typecheck("x => y => x(y(y))", "(A => B) => C & (C => A) => B")

    typecheck("x => { let y = x(x.v); 0 }", "(A => Any) & { v: A, ... } => 0")
  }

  it should "deduplicate errors" in {
    typecheck(
      "''.concat(if (true) true else 3)",
      """|error: Type `true | 3` is not a subtype of `String`
         |at *query*:1:11
         |  |
         |1 | ''.concat(if (true) true else 3)
         |  |           ^^^^^^^^^^^^^^^^^^^^^
         |  |
         |cause: Type `Boolean` is not a subtype of `String`
         |  |
         |1 | ''.concat(if (true) true else 3)
         |  |                     ^^^^
         |  |
         |cause: Type `Int` is not a subtype of `String`
         |  |
         |1 | ''.concat(if (true) true else 3)
         |  |                               ^
         |  |""".stripMargin
    )
  }

  it should "type simple-sub random exprs (pending)" in {
    pendingUntilFixed {
      typecheck("(let rec x = {a = x; b = x} in x)", "{a: 'a, b: 'a} as 'a")

      typecheck(
        "(let rec x = fun v -> {a = x v; b = x v} in x)",
        "⊤ -> {a: 'a, b: 'a} as 'a")

      typecheck(
        "let rec x = (let rec y = {u = y; v = (x y)} in 0) in 0",
        "cannot constrain Int <: 'a -> 'b")

      typecheck(
        "(let rec x = (fun y -> (y (x x))) in x)",
        "('a -> ('a ∧ ('a -> 'b)) as 'b) -> 'a")
      // ^ Note: without canonicalization, we get the simpler:
      // ('b -> 'b ∧ 'a) as 'a -> 'b

      typecheck(
        "(let rec x = (fun y -> (x (y y))) in x)",
        "('b ∧ ('b -> 'a)) as 'a -> ⊥")

      typecheck(
        "(let rec x = (let y = (x x) in (fun z -> z)) in x)",
        "'a -> ('a ∨ ('a -> 'b)) as 'b")

      typecheck(
        "(let rec x = (fun y -> (let z = (x x) in y)) in x)",
        "'a -> ('a ∨ ('a -> 'b)) as 'b")

      typecheck(
        "(let rec x = (fun y -> {u = y; v = (x x)}) in x)",
        "'a -> {u: 'a ∨ ('a -> 'b), v: 'c} as 'c as 'b")

      typecheck(
        "(let rec x = (fun y -> {u = (x x); v = y}) in x)",
        "'a -> {u: 'c, v: 'a ∨ ('a -> 'b)} as 'c as 'b")

      typecheck(
        "(let rec x = (fun y -> (let z = (y x) in y)) in x)",
        "('b ∧ ('a -> ⊤) -> 'b) as 'a")

      typecheck(
        "let rec x = (let y = (x x) in (fun z -> z)) in (x (fun y -> y.u))", // [test:T1]
        "'a ∨ ('a ∧ {u: 'b} -> ('a ∨ 'b ∨ ('a ∧ {u: 'b} -> 'c)) as 'c)")
      // ^ Note: without canonicalization, we get the simpler:
      // ('b ∨ ('b ∧ {u: 'c} -> 'a ∨ 'c)) as 'a
    }
  }

  it should "remove null from union when bang operator is used" in {
    typecheck("(if (true) null else {x: 10})!", "{ x: 10 }")
    typecheck("(if (true) null else {x: 10})!.x", "10")

    typecheck("null!", "Never")

    typecheck("x => x!", "A => A - Null")

    typecheck("(x => x!)(null)", "Never")
    typecheck("(x => x!)(if (true) null else 1)", "1")
    typecheck("(x => x!)(if (true) null else 1) * 2", "Int")
    typecheck("(x => x!)(if (true) null else 1) + 2", "Int")

    // removes null from alias
    typecheck("(x => x!)(aMaybeInt)", "Int")
    typecheck("(x => x!)(aMaybeInt) * 2", "Int")
    typecheck("(x => x!)(aMaybeInt) + 2", "Int")

    // removes null in a lambda
    typecheck("[if (true) '1' else null].map(x => x!.length)", "Array<Int>")
    typecheck("[if (true) 1 else null].map(x => x! + 1)", "Array<Int>")
    typecheck("[aMaybeInt].map(x => x! + 1)", "Array<Int>")
  }

  it should "?. operator adds null to type in the end" in {
    typecheck("{x: 10}?.x", "10 | Null")
    typecheck("{x: () => 10}?.x()", "10 | Null")
    typecheck("{let foo = () => 10; foo?.()}", "10 | Null")
    typecheck("{let foo = [10]; foo?.[0]}", "10 | Null")

    typecheck("'x'?.length", "Int | Null")

    // Handles "{ *: * } | Null" types correctly.
    typecheck("(if (true) null else { x: 10 })?.x", "10 | Null")

    // ?. chains b/c null is stripped out mid-chain.
    typecheck("{x: {y: 10}}?.x.y", "10 | Null")

    // ! excludes null outcome arising from .?.
    typecheck("{x: {y: 10}}?.x?.y!", "10")

    // .? and ! chain together properly.
    typecheck("{ x: { y: 10 } }.x!?.y", "10 | Null")
    typecheck("{ x: { y: 10 } }?.x!?.y", "10 | Null")
    typecheck("{ x: { y: 10 } }?.x.y!", "10")
    typecheck("{ x: { y: 10 } }?.x!?.y!", "10")
  }

  it should "type order() correctly" in {
    typecheck(
      "[{ foo: 3 }, { foo: 4 }].order(.foo)",
      "Array<{ foo: 3 | 4 }>"
    )
    typecheck(
      "[-2, 1].order(v => v * -1)",
      "Array<-2 | 1>"
    )
  }

  it should "type unions" in {
    typecheck(
      "takeaunion(3)",
      "Any"
    )
    typecheck(
      "takeaunion('hi')",
      "Any"
    )
  }

  it should "not stack overflow" in pendingUntilFixed {
    val q = (0 to 10000).mkString("+")
    try {
      typecheck(q, "number")
    } catch {
      case _: StackOverflowError => fail("should not overflow")
    }
  }

  it should "use _ for untypeable things (only)" in {

    // _ improves the error message.
    typecheck(
      "[3.foo].bar",
      """|error: Type `Int` does not have field `foo`
         |at *query*:1:4
         |  |
         |1 | [3.foo].bar
         |  |    ^^^
         |  |
         |
         |error: Type `Array<_>` does not have field `bar`
         |at *query*:1:9
         |  |
         |1 | [3.foo].bar
         |  |         ^^^
         |  |""".stripMargin
    )

    typecheck(
      "{ a: 3.foo }.bar",
      """|error: Type `Int` does not have field `foo`
         |at *query*:1:8
         |  |
         |1 | { a: 3.foo }.bar
         |  |        ^^^
         |  |
         |
         |error: Type `{ a: _ }` does not have field `bar`
         |at *query*:1:14
         |  |
         |1 | { a: 3.foo }.bar
         |  |              ^^^
         |  |""".stripMargin
    )

    // A "proper" never.
    typecheck("abort(0)", "Never")

    // These were fine as is and don't need _ treatment.
    typecheck(
      "((a) => { ''.concat(a) }).foo",
      """|error: Type `String => String` does not have field `foo`
         |at *query*:1:27
         |  |
         |1 | ((a) => { ''.concat(a) }).foo
         |  |                           ^^^
         |  |
         |hint: Type `String => String` inferred here
         |  |
         |1 | ((a) => { ''.concat(a) }).foo
         |  |  ^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    typecheck(
      "(if (true) 3.foo else 4).bar",
      """|error: Type `Int` does not have field `foo`
         |at *query*:1:14
         |  |
         |1 | (if (true) 3.foo else 4).bar
         |  |              ^^^
         |  |
         |
         |error: Type `Int` does not have field `bar`
         |at *query*:1:26
         |  |
         |1 | (if (true) 3.foo else 4).bar
         |  |                          ^^^
         |  |
         |hint: Type `Int` inferred here
         |  |
         |1 | (if (true) 3.foo else 4).bar
         |  |                       ^
         |  |""".stripMargin
    )
  }

  it should "not 500" in {
    // FIXME: Error should get truncated to not be 3000 characters wide
    val query = (0 until 2000).mkString(" + ")
    val underline = query.map { _ => '^' }

    typecheck(
      query,
      s"""|error: Typechecking failed due to query complexity
          |at *query*:1:1
          |  |
          |1 | $query
          |  | $underline
          |  |""".stripMargin
    )
  }

  // FIXME the span should point to the use of `x` in `succ(x)`
  it should "disallow constraints on generic params" in {
    typecheck(
      """|{
         |  let f: A => A = x => {
         |    succ(x)
         |    x
         |  }
         |  f
         |}""".stripMargin,
      """|error: Generic type `A` cannot be used as type `Int`
         |at *query*:2:10
         |  |
         |2 |   let f: A => A = x => {
         |  |          ^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let f: A => A = x => {
         |    'foo'
         |  }
         |  f
         |}""".stripMargin,
      """|error: Type `"foo"` cannot be used as generic type `A`
         |at *query*:3:5
         |  |
         |3 |     'foo'
         |  |     ^^^^^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let f: (A, B, B => A) => A = (a, b, f) => {
         |    f(a)
         |  }
         |  f
         |}""".stripMargin,
      """|error: Generic type `A` cannot be used as type `B`
         |at *query*:2:11
         |  |
         |2 |   let f: (A, B, B => A) => A = (a, b, f) => {
         |  |           ^
         |  |""".stripMargin
    )
  }

  it should "disallow pointless generic params" in {
    typecheck(
      """|{
         |  let f: Array<xxx> => String = x => { let y: Any = null; y }
         |  f
         |}""".stripMargin,
      """|error: Unknown type `xxx`
         |at *query*:2:16
         |  |
         |2 |   let f: Array<xxx> => String = x => { let y: Any = null; y }
         |  |                ^^^
         |  |""".stripMargin
    )

    typecheck(
      """|{
         |  let f: Array<String> => xxx = x => { let y: Any = null; y }
         |  f
         |}""".stripMargin,
      """|error: Unknown type `xxx`
         |at *query*:2:27
         |  |
         |2 |   let f: Array<String> => xxx = x => { let y: Any = null; y }
         |  |                           ^^^
         |  |""".stripMargin
    )
  }

  it should "allow skolems in type parameters" in {
    typecheck(
      """|{
         |  let testFunc: (a: Array<T>, f: T => U) => Array<U> = (a, f) => {
         |    a.map(f)
         |  }
         |
         |  testFunc([1, 2], x => x + 1)
         |}""".stripMargin,
      "Array<Int>"
    )
  }

  it should "handle nested type vars (regression test)" in {
    typecheck(
      """|{
         |let assign: ({ *: A }, { *: B }) => { *: A | B } = (a, b) => a
         |let arr: Array<{ a: String, b: Int }> = []
         |
         |arr.fold({}, (acc, i) => {
         |  let id: String = 'foo'
         |  assign(acc, { x: { a: id, b: i.b + acc[id].b } })
         |})
         |}""".stripMargin,
      "ERROR"
    )
  }
}
