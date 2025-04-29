package fql.test

class MigrationValidatorSpec extends MigrationSpec {
  "A MigrationValidator" should "validate migrations" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: Int
         |
         |  migrations {
         |    add .bar
         |    backfill .bar = 5
         |  }
         |}""".stripMargin,
      "add .bar: Int = 5"
    )

    validate(
      """|collection User {
         |  foo: Int
         |  bar: String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |
         |  migrations {
         |    drop .bar
         |  }
         |}""".stripMargin,
      "drop .bar"
    )
  }

  it should "migrate from the correct continuation point" in {
    // L = a migration item in the live schema.
    // S = a migration item in the submitted schema.
    // _ = a slot with no migration item or no mat.
    // ^ = the continuation point

    //  _ _ _
    //  S S S
    // ^
    validate(
      """|collection User {
         |  foo: Int = 0
         |  bar: Int = 1
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|move .foo -> .tmp
         |move .bar -> .foo
         |move .tmp -> .bar""".stripMargin
    )

    //  L _ _
    //  S S S
    //   ^
    validate(
      """|collection User {
         |  tmp: Int = 0
         |  bar: Int = 1
         |
         |  migrations {
         |    move .foo -> .tmp
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|move .bar -> .foo
         |move .tmp -> .bar""".stripMargin
    )

    //  L L _
    //  S S S
    //     ^
    validate(
      """|collection User {
         |  tmp: Int = 0
         |  foo: Int = 1
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      "move .tmp -> .bar"
    )

    // L L _
    // _ S S
    //    ^
    validate(
      """|collection User {
         |  tmp: Int = 0
         |  foo: Int = 1
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      "move .tmp -> .bar"
    )

    //  L L L
    //  S S S
    //       ^
    // (compatible schemas)
    validate(
      """|collection User {
         |  bar: Int = 0
         |  foo: Int = 1
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      ""
    )

    //  L L L
    //  _ _ _
    //       ^
    validate(
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |}""".stripMargin,
      ""
    )

    //  L L L
    //  _ _ S
    //       ^
    validate(
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      ""
    )

    //  L L L
    //  _ S S
    //       ^
    validate(
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |
         |  migrations {
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      ""
    )

    // (  nada   )
    // (el zilcho)
    //            ^
    // (compatible schemas)
    validate(
      """|collection User {
         |  foo: Int = 1
         |  bar: Int = 0
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | String = 1
         |  bar: Int | String = 0
         |}""".stripMargin,
      ""
    )
  }

  it should "disallow adding fields not in the submitted schema" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |
         |  migrations {
         |    add .bar
         |  }
         |}""".stripMargin,
      """|error: Cannot add field, as `.bar` is not in the submitted schema
         |at main.fsl:5:9
         |  |
         |5 |     add .bar
         |  |         ^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow having fields not added" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: Int = 0
         |}""".stripMargin,
      """|error: Field `.bar` is not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   bar: Int = 0
         |  |   ^^^
         |  |
         |hint: Provide an `add` migration for this field
         |at main.fsl:4:1
         |  |
         |4 |   migrations {
         |  |  _+
         |5 | |     add .bar
         |6 | |   }
         |  | |____^
         |  |""".stripMargin
    )
  }

  it should "disallow backfilling a field without adding or splitting into it" in {
    validate(
      "collection User { _no_wildcard: Null }",
      """|collection User {
         |  foo: Int
         |
         |  migrations {
         |    backfill .foo = 0
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|error: Field `.foo` is backfilled without being declared by an `add` or `split` migration
         |at main.fsl:5:5
         |  |
         |5 |     backfill .foo = 0
         |  |     ^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow dropping fields in the submitted schema" in {
    validate(
      """|collection User {
         |  foo: Int
         |  bar: String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    drop .bar
         |  }
         |}""".stripMargin,
      """|error: Cannot drop field `.bar`. Dropped fields must be removed from schema
         |at main.fsl:6:10
         |  |
         |6 |     drop .bar
         |  |          ^^^^
         |  |
         |hint: Field defined here
         |at main.fsl:3:3
         |  |
         |3 |   bar: String
         |  |   ^^^
         |  |""".stripMargin
    )
  }

  it should "disallow backfilling fields that don't exist" in {
    validate(
      """|collection User {
         |}""".stripMargin,
      """|collection User {
         |  migrations {
         |    backfill .bar = 5
         |  }
         |}""".stripMargin,
      """|error: Cannot backfill field `.bar`, as it is not in the submitted schema
         |at main.fsl:3:14
         |  |
         |3 |     backfill .bar = 5
         |  |              ^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow keeping a field because it's backfilled" in {
    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  x: Int | String
         |  y: String = ""
         |  z: Int = 0
         |
         |  migrations {
         |    add .x
         |    split .x -> .y, .z
         |    backfill .x = 0
         |  }
         |}""".stripMargin,
      """|error: Cannot split field: field is still present in submitted schema
         |at main.fsl:9:11
         |  |
         |9 |     split .x -> .y, .z
         |  |           ^^
         |  |
         |hint: Field defined here
         |at main.fsl:3:3
         |  |
         |3 |   x: Int | String
         |  |   ^
         |  |""".stripMargin
    )

    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  x: Int = 0
         |
         |  migrations {
         |    backfill .x = 0
         |    add .x
         |  }
         |}""".stripMargin,
      """|error: Cannot add field `.x` after it's backfilled
         |at main.fsl:6:5
         |  |
         |6 |     add .x
         |  |     ^^^^^^
         |  |
         |hint: Field backfilled here
         |at main.fsl:5:5
         |  |
         |5 |     backfill .x = 0
         |  |     ^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  x: Int = 0
         |  y: Int
         |
         |  migrations {
         |    add .x
         |    move .x -> .y
         |    backfill .x = 0
         |  }
         |}""".stripMargin,
      // TODO: This error could chill a bit. It's correct but it's too much.
      """|error: Cannot move field `.x`, as it is in the submitted schema
         |at main.fsl:8:10
         |  |
         |8 |     move .x -> .y
         |  |          ^^
         |  |
         |hint: Field defined here
         |at main.fsl:3:3
         |  |
         |3 |   x: Int = 0
         |  |   ^
         |  |
         |hint: To move `.x` to `.y` and add a new field named `x`, add an `add` migration
         |  |
         |8 |       move .x -> .y
         |  |  __________________+
         |9 | |     add .x
         |  | |__________^
         |  |
         |error: Cannot declare non-nullable field `.x`: it has no default and no backfill value
         |at main.fsl:7:9
         |  |
         |7 |     add .x
         |  |         ^^
         |  |
         |hint: Make the field nullable
         |at main.fsl:4:9
         |  |
         |4 |   y: Int?
         |  |         +
         |  |
         |hint: Add a default value to this field
         |at main.fsl:4:9
         |  |
         |4 |   y: Int = <expr>
         |  |         +++++++++
         |  |
         |hint: Add a `backfill` migration
         |at main.fsl:10:3
         |   |
         |10 |     backfill .x = <expr>
         |   |   +++++++++++++++++++++++
         |   |""".stripMargin
    )
  }

  it should "backfill and drop should work together" in {
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  y: Int
         |
         |  migrations {
         |    drop .x
         |    add .y
         |    backfill .y = 3
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|error: Field `.x` was dropped but it doesn't exist in the live schema
         |at main.fsl:5:11
         |  |
         |5 |     drop .x
         |  |           ^
         |  |""".stripMargin
    )

    validate(
      """|collection User {
         |  x: Int
         |}""".stripMargin,
      """|collection User {
         |  y: Int
         |
         |  migrations {
         |    drop .x
         |    add .y
         |    backfill .y = 3
         |  }
         |}""".stripMargin,
      """|drop .x
         |add .y: Int = 3""".stripMargin
    )
  }

  it should "add and drop should compose" in {
    validate(
      """|collection User {
         |  x: Int
         |}""".stripMargin,
      """|collection User {
         |  y: Int = 3
         |
         |  migrations {
         |    drop .x
         |    add .y
         |  }
         |}""".stripMargin,
      """|drop .x
         |add .y: Int = 3""".stripMargin
    )

    validate(
      """|collection User {
         |  x: Int
         |  y: Int
         |}""".stripMargin,
      """|collection User {
         |  y: Int = 3
         |
         |  migrations {
         |    drop .x
         |  }
         |}""".stripMargin,
      "drop .x"
    )
  }

  it should "doesn't allow dropping a field that doesn't exist" in {
    validate(
      """|collection User {}""".stripMargin,
      """|collection User {
         |  migrations {
         |    drop .foo
         |  }
         |}""".stripMargin,
      """|error: Field `.foo` was dropped but it doesn't exist in the live schema
         |at main.fsl:3:11
         |  |
         |3 |     drop .foo
         |  |           ^^^
         |  |""".stripMargin
    )
  }

  it should "handle unions correctly for complex types" in {
    validate(
      """|collection User {
         |  creditCard: {
         |    network: String?,
         |    number: String?
         |  }?
         |}""".stripMargin,
      """|collection User {
         |  creditCard: {
         |    network: "Visa" | "Mastercard" | "American Express"?,
         |    number: String?
         |  }?
         |  garbage: Any
         |
         |  migrations {
         |    split .creditCard -> .creditCard, .garbage
         |  }
         |}""".stripMargin,
      """split .creditCard -> (.creditCard: { network: "Visa" | "Mastercard" | "American Express" | Null, number: String | Null } | Null), (.garbage: Any)"""
    )
  }

  it should "disallow removing fields" in {
    validate(
      """|collection User {
         |  foo: Int
         |  _no_wildcard_: Int
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard_: Int
         |}""".stripMargin,
      """|error: Field `.foo` is missing from the submitted schema
         |at main.fsl:1:1
         |  |
         |1 |   collection User {
         |  |  _^
         |2 | |   _no_wildcard_: Int
         |3 | | }
         |  | |_^
         |  |
         |hint: Drop the field and its data with a `drop` migration
         |at main.fsl:3:1
         |  |
         |3 |   migrations {
         |  |  _+
         |4 | |     drop .foo
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )

    // Even if the field is nullable, it cannot be removed.
    validate(
      """|collection User {
         |  foo: Int | Null
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|error: Field `.foo` is missing from the submitted schema
         |at main.fsl:1:1
         |  |
         |1 |   collection User {
         |  |  _^
         |2 | |   _no_wildcard: Int
         |3 | | }
         |  | |_^
         |  |
         |hint: Drop the field and its data with a `drop` migration
         |at main.fsl:3:1
         |  |
         |3 |   migrations {
         |  |  _+
         |4 | |     drop .foo
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )
  }

  it should "allow adding fields" in {
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | Null
         |
         |  migrations {
         |    add .foo
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      "add .foo: Int | Null".stripMargin
    )

    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|error: Field `.foo` is not present in the live schema
         |at main.fsl:2:3
         |  |
         |2 |   foo: Int
         |  |   ^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     add .foo
         |4 | |     backfill .foo = <expr>
         |5 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |  |
         |2 |   foo: Int = <expr>
         |  |           +++++++++
         |  |
         |hint: Make the field nullable
         |  |
         |2 |   foo: Int?
         |  |           +
         |  |""".stripMargin
    )
  }

  it should "disallow backfilling the wrong type" in {
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  _no_wildcard: Null
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = "hi"
         |  }
         |}""".stripMargin,
      """|error: Type `String` is not a subtype of `Int`
         |at main.fsl:7:21
         |  |
         |7 |     backfill .foo = "hi"
         |  |                     ^^^^
         |  |
         |hint: Field type defined here
         |at main.fsl:2:8
         |  |
         |2 |   foo: Int
         |  |        ^^^
         |  |""".stripMargin
    )

    // This can't be caught by typechecking, it'll be caught up in model after
    // evaling the backfill.
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  _no_wildcard: Null
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = {
         |      let a: Any = "hi"
         |      a
         |    }
         |  }
         |}""".stripMargin,
      """|add .foo: Int = {
         |  let a: Any = "hi"
         |  a
         |}""".stripMargin
    )
  }

  it should "allow widening fields" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | Null
         |}""".stripMargin,
      ""
    )
  }

  it should "disallow narrowing fields without a migration" in {
    // FIXME: We could also suggest a split+drop here, but again, suggesting data
    // deletion seems like a bad idea.
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|error: Field `.foo` of type `Int` does not match the live type `Int | String`
         |at main.fsl:2:8
         |  |
         |2 |   foo: Int
         |  |        ^^^
         |  |
         |hint: Add a migration to split the invalid types into another field
         |  |
         |2 |     tmp: String
         |  |  ___+
         |3 | |   migrations {
         |4 | |     split .foo -> .foo, .tmp
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )
  }

  it should "allow splitting fields" in {
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: String
         |  baz: Int
         |
         |  migrations {
         |    split .foo -> .bar, .baz
         |    backfill .bar = "default"
         |    backfill .baz = 0
         |  }
         |}""".stripMargin,
      """|split .foo -> (.bar: String = "default"), (.baz: Int = 0)""".stripMargin
    )
  }

  it should "allow splitting into nullable fields and fields with a default" in {
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: String | Null
         |  baz: Int = 0
         |
         |  migrations {
         |    split .foo -> .bar, .baz
         |  }
         |}""".stripMargin,
      "split .foo -> (.bar: String | Null), (.baz: Int = 0)"
    )
  }

  it should "allow splitting a nullable field into non-nullable parts" in {
    Seq("Int? | String", "Int | String?", "(Int | String)?") foreach { tyX =>
      validate(
        s"""|collection Foo {
            |  x: $tyX
           |}""".stripMargin,
        """|collection Foo {
           |  y: Int
           |  z: String = "-"
           |
           |  migrations {
           |    split .x -> .y, .z
           |    backfill .y = 0
           |  }
           |}""".stripMargin,
        """split .x -> (.y: Int = 0), (.z: String = "-")"""
      )
    }
  }

  it should "disallow adding and splitting into a field" in {
    validate(
      """|collection Foo {
         |  w: Int | String | Boolean
         |}""".stripMargin,
      """|collection Foo {
         |  x: Int
         |  y: String?
         |  z: Boolean = false
         |  migrations {
         |    split .w -> .x, .y, .z
         |    add .x
         |    backfill .x = 0
         |    add .y
         |    add .z
         |  }
         |}""".stripMargin,
      """|error: Cannot add a field that is also created by a split
         |at main.fsl:6:17
         |  |
         |6 |     split .w -> .x, .y, .z
         |  |                 ^^
         |  |
         |hint: Field is added here
         |at main.fsl:7:9
         |  |
         |7 |     add .x
         |  |         ^^
         |  |
         |hint: Remove the add
         |at main.fsl:7:5
         |  |
         |7 |     add .x
         |  |     ------
         |  |
         |error: Cannot add a field that is also created by a split
         |at main.fsl:6:21
         |  |
         |6 |     split .w -> .x, .y, .z
         |  |                     ^^
         |  |
         |hint: Field is added here
         |at main.fsl:9:9
         |  |
         |9 |     add .y
         |  |         ^^
         |  |
         |hint: Remove the add
         |at main.fsl:9:5
         |  |
         |9 |     add .y
         |  |     ------
         |  |
         |error: Cannot add a field that is also created by a split
         |at main.fsl:6:25
         |  |
         |6 |     split .w -> .x, .y, .z
         |  |                         ^^
         |  |
         |hint: Field is added here
         |at main.fsl:10:9
         |   |
         |10 |     add .z
         |   |         ^^
         |   |
         |hint: Remove the add
         |at main.fsl:10:5
         |   |
         |10 |     add .z
         |   |     ------
         |   |""".stripMargin
    )
  }

  it should "disallow splitting into a field twice" in {
    validate(
      """|collection Foo {
         |  a: Int | String
         |  x: Int | String
         |}""".stripMargin,
      """|collection Foo {
         |  b: Int
         |  y: Int
         |  k: String?
         |  migrations {
         |    split .a -> .b, .k
         |    split .x -> .k, .y
         |    backfill .b = 0
         |    backfill .y = 0
         |  }
         |}""".stripMargin,
      """|error: Cannot split into a field that is already split into
         |at main.fsl:7:17
         |  |
         |7 |     split .x -> .k, .y
         |  |                 ^^
         |  |
         |hint: Field is also split into here
         |at main.fsl:6:21
         |  |
         |6 |     split .a -> .b, .k
         |  |                     ^^
         |  |
         |hint: Remove the later split
         |  |
         |7 |     split .x -> .k, .y
         |  |     ------------------
         |  |""".stripMargin
    )
  }

  it should "disallow splitting without a backfill" in {
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: String
         |  baz: Int
         |
         |  migrations {
         |    split .foo -> .bar, .baz
         |  }
         |}""".stripMargin,
      """|error: Cannot declare non-nullable field `.bar`: it has no default and no backfill value
         |at main.fsl:6:19
         |  |
         |6 |     split .foo -> .bar, .baz
         |  |                   ^^^^
         |  |
         |hint: Make the field nullable
         |at main.fsl:2:14
         |  |
         |2 |   bar: String?
         |  |              +
         |  |
         |hint: Add a default value to this field
         |at main.fsl:2:14
         |  |
         |2 |   bar: String = <expr>
         |  |              +++++++++
         |  |
         |hint: Add a `backfill` migration
         |at main.fsl:7:3
         |  |
         |7 |     backfill .bar = <expr>
         |  |   +++++++++++++++++++++++++
         |  |
         |error: Cannot declare non-nullable field `.baz`: it has no default and no backfill value
         |at main.fsl:6:25
         |  |
         |6 |     split .foo -> .bar, .baz
         |  |                         ^^^^
         |  |
         |hint: Make the field nullable
         |at main.fsl:3:11
         |  |
         |3 |   baz: Int?
         |  |           +
         |  |
         |hint: Add a default value to this field
         |at main.fsl:3:11
         |  |
         |3 |   baz: Int = <expr>
         |  |           +++++++++
         |  |
         |hint: Add a `backfill` migration
         |at main.fsl:7:3
         |  |
         |7 |     backfill .baz = <expr>
         |  |   +++++++++++++++++++++++++
         |  |""".stripMargin
    )
  }

  it should "type > 2 splits correctly" in {
    validate(
      """|collection User {
         |  foo: Int | String | Boolean
         |}""".stripMargin,
      """|collection User {
         |  bar: String
         |  baz: Boolean
         |  qux: Int
         |
         |  migrations {
         |    split .foo -> .bar, .baz, .qux
         |
         |    backfill .bar = "default"
         |    backfill .baz = true
         |    backfill .qux = 0
         |  }
         |}""".stripMargin,
      """|split .foo -> (.bar: String = "default"), (.baz: Boolean = true), (.qux: Int = 0)""".stripMargin
    )
  }

  // The model kinda falls apart here. `bar` gets merged, which removes the backfill.
  // Then `bar` is merged with `foo`, which fails because `bar` doesn't have a
  // backfill value.
  it should "type multiple splits correctly (pending)" in pendingUntilFixed {
    validate(
      """|collection User {
         |  foo: Int | String | Boolean
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: String
         |  baz: Boolean
         |
         |  migrations {
         |    split .foo -> .bar
         |    split .foo -> .bar, .baz
         |
         |    backfill .foo = 0
         |    backfill .bar = "default"
         |    backfill .baz = true
         |  }
         |}""".stripMargin,
      """|split .foo: Int = 0 -> .bar = "default"
         |split .bar: String = "default" -> .baz = true""".stripMargin
    )
  }

  it should "split and then drop" in {
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: Int
         |
         |  migrations {
         |    split .foo -> .bar, .tmp
         |    backfill .bar = 0
         |    drop .tmp
         |  }
         |}""".stripMargin,
      """|split .foo -> (.bar: Int = 0), (.tmp: Any)
         |drop .tmp""".stripMargin
    )
  }

  it should "split and then drop Any" in {
    validate(
      """|collection User {
         |  foo: Any
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |
         |  migrations {
         |    split .foo -> .foo, .tmp
         |    backfill .foo = 3
         |    drop .tmp
         |  }
         |}""".stripMargin,
      """|split .foo -> (.foo: Int = 3), (.tmp: Any)
         |drop .tmp""".stripMargin
    )
  }

  it should "split into a field with the same name" in {
    val base =
      """|collection User {
         |  foo: Int | String | Boolean
         |}""".stripMargin

    // Split into a same-name field as the first destination.
    validate(
      base,
      """|collection User {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    split .foo -> .foo, .bar, .tmp
         |    backfill .foo = 0
         |    backfill .bar = "0"
         |    drop .tmp
         |  }
         |}""".stripMargin,
      """|split .foo -> (.foo: Int = 0), (.bar: String = "0"), (.tmp: Any)
         |drop .tmp""".stripMargin
    )

    // Split into a same-name field not the first destination.
    validate(
      base,
      """|collection User {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    split .foo -> .bar, .foo, .tmp
         |    backfill .foo = 0
         |    backfill .bar = "0"
         |    drop .tmp
         |  }
         |}""".stripMargin,
      """|split .foo -> (.bar: String = "0"), (.foo: Int = 0), (.tmp: Any)
         |drop .tmp""".stripMargin
    )

    // Weirder split with multiple backfills for the same field.
    // Something like this conceivably could come up applying a development schema
    // to a higher environment.
    validate(
      "collection User { _no_wildcard: Null }",
      """|collection User {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = 0
         |    split .foo -> .bar, .foo, .tmp
         |    backfill .foo = 1
         |    backfill .bar = "0"
         |    drop .tmp
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Any | String | Int | Null = 0
         |split .foo -> (.bar: String = "0"), (.foo: Int = 1), (.tmp: Any)
         |drop .tmp""".stripMargin
    )
  }

  it should "disallow invalid splits" in {
    // FIXME: This should determine the type `Int | Boolean` came from a migration,
    // and point at it, instead of a null span.
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: Boolean
         |  baz: Int
         |
         |  migrations {
         |    split .foo -> .bar, .baz
         |    backfill .bar = true
         |    backfill .baz = 0
         |  }
         |}""".stripMargin,
      """|error: Field `.foo` of type `Boolean | Int | Null` does not match the live type `Int | String`
         |at <no source>
         |hint: Add a migration to split the invalid types into another field
         |at main.fsl:2:3
         |  |
         |2 |     tmp: String
         |  |  ___+
         |3 | |   migrations {
         |4 | |     split .foo -> .foo, .tmp
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )
  }

  it should "disallow keeping fields in a split around" in {
    // TODO: The error message should mention the source of the field.
    validate(
      """|collection User {
         |  foo: Int | Boolean
         |  bar: Boolean
         |}""".stripMargin,
      """|collection User {
         |  bar: Boolean
         |  baz: Int
         |
         |  migrations {
         |    split .foo -> .bar, .baz
         |    backfill .bar = true
         |    backfill .baz = 0
         |  }
         |}""".stripMargin,
      """|error: Field `.bar` is in the live schema, so it cannot be added or backfilled
         |at main.fsl:2:3
         |  |
         |2 |   bar: Boolean
         |  |   ^^^
         |  |""".stripMargin
    )
  }

  it should "disallow splitting into fields that don't exist" in {
    validate(
      """|collection User {
         |  foo: Int | Boolean
         |}""".stripMargin,
      """|collection User {
         |  bar: Int
         |
         |  migrations {
         |    split .foo -> .bar, .baz
         |    backfill .bar = 0
         |  }
         |}""".stripMargin,
      """|error: Cannot split field, as `.baz` is not in the submitted schema
         |at main.fsl:5:25
         |  |
         |5 |     split .foo -> .bar, .baz
         |  |                         ^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow splitting a field and keeping it around" in {
    validate(
      """|collection Foo {
         |  x: Int | String
         |}""".stripMargin,
      """|collection Foo {
         |  x: Int | String
         |  y: Int?
         |  z: String?
         |
         |  migrations {
         |    split .x -> .y, .z
         |  }
         |}""".stripMargin,
      """|error: Cannot split field: field is still present in submitted schema
         |at main.fsl:7:11
         |  |
         |7 |     split .x -> .y, .z
         |  |           ^^
         |  |
         |hint: Field defined here
         |at main.fsl:2:3
         |  |
         |2 |   x: Int | String
         |  |   ^
         |  |""".stripMargin
    )
  }

  it should "disallow splitting a field twice" in {
    validate(
      """|collection Foo {
         |  x: Int | String
         |}""".stripMargin,
      """|collection Foo {
         |  y: Int?
         |  z: String?
         |  a: Int?
         |  b: String?
         |
         |  migrations {
         |    split .x -> .y, .z
         |    split .x -> .a, .b
         |  }
         |}""".stripMargin,
      """|error: Cannot split field twice
         |at main.fsl:9:11
         |  |
         |9 |     split .x -> .a, .b
         |  |           ^^
         |  |
         |hint: Field is also split here
         |at main.fsl:8:11
         |  |
         |8 |     split .x -> .y, .z
         |  |           ^^
         |  |
         |hint: Remove this split
         |at main.fsl:8:5
         |  |
         |8 |     split .x -> .y, .z
         |  |     ------------------
         |  |
         |hint: Remove the later split
         |  |
         |9 |     split .x -> .a, .b
         |  |     ------------------
         |  |""".stripMargin
    )
  }

  it should "disallow dropping a field consumed by a split" in {
    validate(
      """|collection Foo {
         |  x: Int | String
         |}""".stripMargin,
      """|collection Foo {
         |  y: Int?
         |  z: String?
         |
         |  migrations {
         |    split .x -> .y, .z
         |    drop .x
         |  }
         |}""".stripMargin,
      """|error: Cannot drop field that was split
         |at main.fsl:6:11
         |  |
         |6 |     split .x -> .y, .z
         |  |           ^^
         |  |
         |hint: Remove the `drop`
         |at main.fsl:7:5
         |  |
         |7 |     drop .x
         |  |     -------
         |  |""".stripMargin
    )
  }

  it should "make a nice error after splitting into fields and dropping them" in {
    validate(
      """|collection User {
         |  foo: Int | Boolean
         |}""".stripMargin,
      """|collection User {
         |
         |  migrations {
         |    drop .tmp
         |    drop .tmp2
         |    split .foo -> .tmp, .tmp2
         |  }
         |}""".stripMargin,
      """|error: Cannot split field, as `.tmp` is not in the submitted schema
         |at main.fsl:6:19
         |  |
         |6 |     split .foo -> .tmp, .tmp2
         |  |                   ^^^^
         |  |
         |error: Cannot split field, as `.tmp2` is not in the submitted schema
         |at main.fsl:6:25
         |  |
         |6 |     split .foo -> .tmp, .tmp2
         |  |                         ^^^^^
         |  |""".stripMargin
    )
  }

  it should "allow you to add defaults to fields" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 3
         |}""".stripMargin,
      ""
    )
  }

  it should "allow you to remove defaults from fields" in {
    validate(
      """|collection User {
         |  foo: Int = 3
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      ""
    )
  }

  it should "move fields" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  bar: Int
         |
         |  migrations {
         |    move .foo -> .bar
         |  }
         |}""".stripMargin,
      "move .foo -> .bar"
    )
  }

  it should "swap fields" in {
    validate(
      """|collection User {
         |  foo: Int
         |  bar: String
         |}""".stripMargin,
      """|collection User {
         |  foo: String
         |  bar: Int
         |
         |  migrations {
         |    move .foo -> .tmp
         |    move .bar -> .foo
         |    move .tmp -> .bar
         |  }
         |}""".stripMargin,
      """|move .foo -> .tmp
         |move .bar -> .foo
         |move .tmp -> .bar""".stripMargin
    )
  }

  it should "allow adding multiple nullable fields" in {
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | Null
         |  bar: Int | Null
         |
         |  migrations {
         |    add .foo
         |    add .bar
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Int | Null
         |add .bar: Int | Null""".stripMargin
    )
  }

  it should "allow renaming a field and adding it again with a backfill" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: Int
         |
         |  migrations {
         |    move .foo -> .bar
         |    add .foo
         |    backfill .foo = 3
         |  }
         |}""".stripMargin,
      """|move .foo -> .bar
         |add .foo: Int = 3""".stripMargin
    )
  }

  it should "allow renaming a field and adding it again with a default" in {
    validate(
      """|collection User {
         |  foo: String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 0
         |  bar: String
         |
         |  migrations {
         |    move .foo -> .bar
         |    add .foo
         |  }
         |}""".stripMargin,
      """|move .foo -> .bar
         |add .foo: Int = 0""".stripMargin
    )
  }

  it should "disallow renaming a field in the submitted schema" in {
    validate(
      """|collection User {
         |  foo: String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: String
         |
         |  migrations {
         |    move .foo -> .bar
         |  }
         |}""".stripMargin,
      """|error: Cannot move field `.foo`, as it is in the submitted schema
         |at main.fsl:6:10
         |  |
         |6 |     move .foo -> .bar
         |  |          ^^^^
         |  |
         |hint: Field defined here
         |at main.fsl:2:3
         |  |
         |2 |   foo: Int
         |  |   ^^^
         |  |
         |hint: To move `.foo` to `.bar` and add a new field named `foo`, add an `add` migration
         |  |
         |6 |       move .foo -> .bar
         |  |  ______________________+
         |7 | |     add .foo
         |  | |____________^
         |  |""".stripMargin
    )
  }

  it should "allow backfilling a field after move" in {
    validate(
      "collection User { _no_wildcard: Null }",
      """|collection User {
         |  bar: Int
         |
         |  migrations {
         |    add .foo
         |    move .foo -> .bar
         |    backfill .bar = 0
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Int = 0
         |move .foo -> .bar""".stripMargin
    )
  }

  it should "allow renaming a field that's added" in {
    validate(
      "collection User { _no_wildcard: Null }",
      """|collection User {
         |  bar: Int
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = 0
         |    move .foo -> .bar
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Int = 0
         |move .foo -> .bar""".stripMargin
    )
  }

  // TODO: The error should say what introduced the field (add vs. move).
  //            Also it's just bad error.
  it should "disallow double-creating a field by add and move" in {
    // Move then add.
    validate(
      """|collection User {
         |  foo: Int = 0
         |}""".stripMargin,
      """|collection User {
         |  bar: Int = 0
         |
         |  migrations {
         |    move .foo -> .bar
         |    add .bar
         |  }
         |}""".stripMargin,
      """|error: Field `.foo` is in the live schema, so it cannot be added or backfilled
         |at main.fsl:5:11
         |  |
         |5 |     move .foo -> .bar
         |  |           ^^^
         |  |""".stripMargin
    )

    // Add then move.
    validate(
      """|collection User {
         |  foo: Int = 0
         |}""".stripMargin,
      """|collection User {
         |  bar: Int = 0
         |
         |  migrations {
         |    add .bar
         |    move .foo -> .bar
         |  }
         |}""".stripMargin,
      """|error: Cannot move into field `.bar` as it was also added
         |at main.fsl:6:5
         |  |
         |6 |     move .foo -> .bar
         |  |     ^^^^^^^^^^^^^^^^^
         |  |
         |hint: Field moved into here
         |  |
         |6 |     move .foo -> .bar
         |  |                  ^^^^
         |  |
         |hint: Remove the add
         |at main.fsl:5:5
         |  |
         |5 |     add .bar
         |  |     --------
         |  |
         |hint: Remove the move
         |  |
         |6 |     move .foo -> .bar
         |  |     -----------------
         |  |""".stripMargin
    )
  }

  it should "disallow adding the same field twice" in {
    validate(
      "collection User {}",
      """|collection User {
         |  foo: Int = 0
         |
         |  migrations {
         |    add .foo
         |    add .foo
         |  }
         |}""".stripMargin,
      """|error: Cannot add field `.foo` twice
         |at main.fsl:6:9
         |  |
         |6 |     add .foo
         |  |         ^^^^
         |  |
         |hint: Field added here
         |at main.fsl:5:9
         |  |
         |5 |     add .foo
         |  |         ^^^^
         |  |
         |hint: Remove the extra add
         |  |
         |6 |     add .foo
         |  |     --------
         |  |""".stripMargin
    )
  }

  it should "disallow backfilling the same field twice" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = 3
         |    backfill .foo = 4
         |  }
         |}""".stripMargin,
      """|error: Cannot backfill field `.foo` multiple times
         |at main.fsl:7:14
         |  |
         |7 |     backfill .foo = 4
         |  |              ^^^^
         |  |
         |hint: Earlier backfill defined here
         |at main.fsl:6:14
         |  |
         |6 |     backfill .foo = 3
         |  |              ^^^^
         |  |
         |hint: Delete the later backfill
         |  |
         |7 |     backfill .foo = 4
         |  |     -----------------
         |  |""".stripMargin
    )
  }

  it should "add and drop is allowed" in {
    // No backfill.
    validate(
      "collection User { _no_wildcard: Null }".stripMargin,
      """|collection User {
         |  migrations {
         |    add .foo
         |    drop .foo
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Any
         |drop .foo""".stripMargin
    )

    // With backfill.
    validate(
      "collection User { _no_wildcard: Null }".stripMargin,
      """|collection User {
         |  migrations {
         |    add .foo
         |    backfill .foo = 5
         |    drop .foo
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Any = 5
         |drop .foo""".stripMargin
    )
  }

  it should "disallow adding and then splitting into the same field" in {
    // Without backfills.
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: Int = 0
         |
         |  migrations {
         |    add .bar
         |    split .foo -> .bar, .baz
         |    drop .baz
         |  }
         |}""".stripMargin,
      """|error: Cannot split field: field `.bar` cannot both be added and be a split output
         |at main.fsl:5:9
         |  |
         |5 |     add .bar
         |  |         ^^^^
         |  |
         |hint: Field is an output of a split
         |at main.fsl:6:19
         |  |
         |6 |     split .foo -> .bar, .baz
         |  |                   ^^^^
         |  |
         |hint: Remove the add
         |  |
         |5 |     add .bar
         |  |     --------
         |  |
         |hint: Remove the split
         |at main.fsl:6:5
         |  |
         |6 |     split .foo -> .bar, .baz
         |  |     ------------------------
         |  |""".stripMargin
    )

    // With backfills.
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  bar: Int
         |
         |  migrations {
         |    add .bar
         |    backfill .bar = 0
         |    split .foo -> .bar, .baz
         |    backfill .bar = 1
         |    drop .baz
         |  }
         |}""".stripMargin,
      """|error: Cannot split field `.bar`: cannot split into a field after it's backfilled
         |at main.fsl:6:14
         |  |
         |6 |     backfill .bar = 0
         |  |              ^^^^
         |  |
         |hint: Field is an output of a split
         |at main.fsl:7:19
         |  |
         |7 |     split .foo -> .bar, .baz
         |  |                   ^^^^
         |  |
         |hint: Move the backfill after the split
         |at main.fsl:7:5
         |  |
         |7 |     backfill .bar = 0
         |  |     ~~~~~~~~~~~~~~~~~
         |  |
         |hint: Remove the backfill
         |  |
         |6 |     backfill .bar = 0
         |  |     -----------------
         |  |
         |hint: Remove the split
         |at main.fsl:7:5
         |  |
         |7 |     split .foo -> .bar, .baz
         |  |     ------------------------
         |  |
         |error: Cannot split field: field `.bar` cannot both be added and be a split output
         |at main.fsl:5:9
         |  |
         |5 |     add .bar
         |  |         ^^^^
         |  |
         |hint: Field is an output of a split
         |at main.fsl:7:19
         |  |
         |7 |     split .foo -> .bar, .baz
         |  |                   ^^^^
         |  |
         |hint: Remove the add
         |  |
         |5 |     add .bar
         |  |     --------
         |  |
         |hint: Remove the split
         |at main.fsl:7:5
         |  |
         |7 |     split .foo -> .bar, .baz
         |  |     ------------------------
         |  |""".stripMargin
    )
  }

  it should "allow splitting out of a field and adding it back" in {
    validate(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |  bar: String
         |  migrations {
         |    split .foo -> .bar, .baz
         |    drop .baz
         |    add .foo
         |    backfill .foo = 0
         |    backfill .bar = "yay"
         |  }
         |}""".stripMargin,
      """|split .foo -> (.bar: String = "yay"), (.baz: Any)
         |drop .baz
         |add .foo: Int = 0""".stripMargin
    )
  }

  it should "handle multiple moves correctly" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 3
         |
         |  migrations {
         |    move .foo -> .bar
         |    move .bar -> .foo
         |  }
         |}""".stripMargin,
      """|move .foo -> .bar
         |move .bar -> .foo""".stripMargin
    )
  }

  it should "not backfill fields with multiple moves" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int
         |
         |  migrations {
         |    move .foo -> .bar
         |    move .bar -> .foo
         |
         |    backfill .foo = 3
         |  }
         |}""".stripMargin,
      """|error: Field `.foo` is backfilled without being declared by an `add` or `split` migration
         |at main.fsl:8:5
         |  |
         |8 |     backfill .foo = 3
         |  |     ^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "allow backfill and multiple moves" in {
    validate(
      "collection User { _no_wildcard: Null }",
      """|collection User {
         |  baaz: Int
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = 3
         |    move .foo -> .bar
         |    move .bar -> .baaz
         |  }
         |
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|add .foo: Int = 3
         |move .foo -> .bar
         |move .bar -> .baaz""".stripMargin
    )
  }

  it should "not backfill fields that got moved" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 3
         |  bar: Int = 3
         |
         |  migrations {
         |    move .foo -> .bar
         |    add .foo
         |  }
         |}""".stripMargin,
      """|move .foo -> .bar
         |add .foo: Int = 3""".stripMargin
    )
  }

  it should "backfill moves with defaults correctly" in {
    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 3
         |  bar: Int
         |
         |  migrations {
         |    move .foo -> .bar
         |    add .foo
         |  }
         |}""".stripMargin,
      """|move .foo -> .bar
         |add .foo: Int = 3""".stripMargin
    )

    validate(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 3
         |  bar: Int
         |
         |  migrations {
         |    move .foo -> .bar
         |    add .foo
         |    backfill .foo = 5
         |  }
         |}""".stripMargin,
      """|move .foo -> .bar
         |add .foo: Int = 5""".stripMargin
    )
  }

  it should "disallow renaming a field to itself" in {
    validate(
      """|collection User {
         |  a: Int = 0
         |}""".stripMargin,
      """|collection User {
         |  a: Int = 0
         |
         |  migrations {
         |    move .a -> .a
         |  }
         |}""".stripMargin,
      """|error: Cannot move a field to itself
         |at main.fsl:5:10
         |  |
         |5 |     move .a -> .a
         |  |          ^^
         |  |
         |hint: Remove the futile `move`
         |  |
         |5 |     move .a -> .a
         |  |     -------------
         |  |""".stripMargin
    )

    // The error should also appear in this situation, which was the original
    // case from the fuzzer.
    validate(
      "collection User {}",
      """|collection User {
         |  d: Int = 0
         |  migrations {
         |    move .q -> .d
         |    move .d -> .d
         |  }
         |}""".stripMargin,
      """|error: Cannot move a field to itself
         |at main.fsl:5:10
         |  |
         |5 |     move .d -> .d
         |  |          ^^
         |  |
         |hint: Remove the futile `move`
         |  |
         |5 |     move .d -> .d
         |  |     -------------
         |  |""".stripMargin
    )
  }

  it should "detect when an add is missing" in {
    // Field has a default.
    validate(
      """|collection User {
         |  name: String
         |}""".stripMargin,
      """|collection User {
         |  name2: String
         |  city: String = "SF"
         |
         |  migrations {
         |    move .name -> .name2
         |  }
         |}""".stripMargin,
      """|error: Field `.city` is not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   city: String = "SF"
         |  |   ^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |at main.fsl:7:3
         |  |
         |7 |     add .city
         |  |   ++++++++++++
         |  |""".stripMargin
    )

    // Field is nullable.
    validate(
      """|collection User {
         |  name: String
         |}""".stripMargin,
      """|collection User {
         |  name2: String
         |  city: String?
         |
         |  migrations {
         |    move .name -> .name2
         |  }
         |}""".stripMargin,
      """|error: Field `.city` is not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   city: String?
         |  |   ^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |at main.fsl:7:3
         |  |
         |7 |     add .city
         |  |   ++++++++++++
         |  |""".stripMargin
    )

    // Backfill required.
    validate(
      """|collection User {
         |  name: String
         |}""".stripMargin,
      """|collection User {
         |  name2: String
         |  city: String
         |
         |  migrations {
         |    move .name -> .name2
         |  }
         |}""".stripMargin,
      """|error: Field `.city` is not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   city: String
         |  |   ^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |3 |     migrations {
         |  |  ___+
         |4 | |     add .city
         |5 | |     backfill .city = <expr>
         |6 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |  |
         |3 |   city: String = <expr>
         |  |               +++++++++
         |  |
         |hint: Make the field nullable
         |  |
         |3 |   city: String?
         |  |               +
         |  |""".stripMargin
    )
  }

  it should "render proper errors and hints for adds lacking backfill" in {
    validate(
      "collection User {}",
      """|collection User {
         | i: Int
         |
         |  migrations {
         |    add .i
         |  }
         |}""".stripMargin,
      """|error: Cannot declare non-nullable field `.i`: it has no default and no backfill value
         |at main.fsl:5:9
         |  |
         |5 |     add .i
         |  |         ^^
         |  |
         |hint: Make the field nullable
         |at main.fsl:2:8
         |  |
         |2 |  i: Int?
         |  |        +
         |  |
         |hint: Add a default value to this field
         |at main.fsl:2:8
         |  |
         |2 |  i: Int = <expr>
         |  |        +++++++++
         |  |
         |hint: Add a `backfill` migration
         |at main.fsl:6:3
         |  |
         |6 |     backfill .i = <expr>
         |  |   +++++++++++++++++++++++
         |  |""".stripMargin
    )
  }

  // TODO
  it should "handle non-identifier names" in {
    pendingUntilFixed {
      validate(
        """|collection User {
           |  "o o h a w%": Int
           |}""".stripMargin,
        """|collection User {
           |  "%w a h o o": Int
           |
           |  migrations {
           |    move ."o o h a w%" -> ."%w a h o o"
           |  }
           |}""".stripMargin,
        """move ."o o h a w%" -> ."%w a h o o""""
      )
    }
  }

  // FIXME: This is broken in so many ways. Here be dragons.
  it should "accept the Ref type (temporarily)" in {
    validate(
      """|collection Bar {}
         |collection User {}""".stripMargin,
      """|collection Bar {}
         |collection User {
         |  foo: Ref<Bar>?
         |}""".stripMargin,
      ""
    )
  }

  it should "allow dropping a move_conflicts field" in {
    validate(
      """|collection Foo {}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |
         |  migrations {
         |    add .a
         |    backfill .a = 1
         |    add .tmp
         |    move_conflicts .tmp
         |    move_wildcard .tmp
         |    drop .tmp
         |  }
         |}""".stripMargin,
      """|add .a: Int = 1
         |add .tmp: Any
         |move_conflicts .tmp
         |move_wildcard -> .tmp [a, tmp]
         |drop .tmp""".stripMargin
    )
  }

  it should "not allow moving into and splitting into" in {
    // Split into then move into.
    validate(
      """|collection Foo {
         |  _no_wc: Int = 0
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wc: Int = 0
         |  c: Int = 0
         |
         |  migrations {
         |    add .a
         |    add .d
         |    split .a -> .b, .c
         |    move .d -> .c
         |    drop .b
         |  }
         |}""".stripMargin,
      """|error: Cannot move into a field that is also a split target
         |at main.fsl:9:5
         |  |
         |9 |     move .d -> .c
         |  |     ^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "handle dropping a collection gracefully" in {
    validate(
      """|collection User {
         |  foo: String
         |  address: Ref<Address>
         |}
         |
         |collection Address {
         |}""".stripMargin,
      """|collection User {
         |  foo: String
         |  address: Ref<Address>
         |}""".stripMargin,
      """|error: Unknown type `Address`
         |at main.fsl:3:16
         |  |
         |3 |   address: Ref<Address>
         |  |                ^^^^^^^
         |  |""".stripMargin
    )

    validate(
      """|collection User {
         |  foo: String
         |  address: Ref<Address>
         |}
         |
         |collection Address {
         |}""".stripMargin,
      """|collection User {
         |  foo: String
         |}""".stripMargin,
      """|error: Field `.address` is missing from the submitted schema
         |at main.fsl:1:1
         |  |
         |1 |   collection User {
         |  |  _^
         |2 | |   foo: String
         |3 | | }
         |  | |_^
         |  |
         |hint: Drop the field and its data with a `drop` migration
         |at main.fsl:3:1
         |  |
         |3 |   migrations {
         |  |  _+
         |4 | |     drop .address
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )

    validate(
      """|collection User {
         |  foo: String
         |  address: Ref<Address>
         |}
         |
         |collection Address {
         |}""".stripMargin,
      """|collection User {
         |  foo: String
         |  address: Int
         |}""".stripMargin,
      """|error: Field `.address` of type `Int` does not match the live type `Ref<Address>`
         |at main.fsl:3:12
         |  |
         |3 |   address: Int
         |  |            ^^^
         |  |
         |hint: Add a migration to split the invalid types into another field
         |  |
         |3 |     tmp: Ref<Address>
         |  |  ___+
         |4 | |   migrations {
         |5 | |     split .address -> .address, .tmp
         |6 | |   }
         |  | |____^
         |  |""".stripMargin
    )
  }

  it should "disallow dropping nested fields of optional structs" in {
    validate(
      """|collection User {
         |  foo: {
         |    bar: Int
         |  }?
         |}""".stripMargin,
      """|collection User {
         |  foo: {}?
         |
         |  migrations {
         |    drop .foo.bar
         |  }
         |}""".stripMargin,
      """|error: Cannot drop field `.foo.bar`, as the parent is not an object
         |at main.fsl:5:10
         |  |
         |5 |     drop .foo.bar
         |  |          ^^^^^^^^
         |  |""".stripMargin
    )
  }
}
