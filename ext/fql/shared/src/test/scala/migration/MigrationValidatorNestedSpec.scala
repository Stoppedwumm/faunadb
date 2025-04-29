package fql.test

class MigrationValidatorNestedSpec extends MigrationSpec {
  "A MigrationValidator" should "allow nested fields" in {
    validate(
      """|collection User {
         |  foo: {
         |    bar: Int
         |  }
         |  baz: {}
         |}""".stripMargin,
      """|collection User {
         |  foo: {}
         |  baz: {
         |    bar: Int
         |  }
         |
         |  migrations {
         |    move .foo.bar -> .baz.bar
         |  }
         |}""".stripMargin,
      "move .foo.bar -> .baz.bar"
    )
  }

  it should "allow splits" in {
    validate(
      """|collection User {
         |  foo: {
         |    bar: Int | String
         |  }
         |  baz: {}
         |}""".stripMargin,
      """|collection User {
         |  foo: {
         |    bar: Int
         |  }
         |  baz: {
         |    bar: String
         |  }
         |
         |  migrations {
         |    split .foo.bar -> .foo.bar, .baz.bar
         |    backfill .foo.bar = 0
         |    backfill .baz.bar = "hi"
         |  }
         |}""".stripMargin,
      """split .foo.bar -> (.foo.bar: Int = 0), (.baz.bar: String = "hi")"""
    )
  }

  it should "allow adding nested fields" in {
    validate(
      """|collection User {
         |  foo: {
         |    bar: Int
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: {
         |    bar: Int,
         |    baz: String
         |  }
         |
         |  migrations {
         |    add .foo.baz
         |    backfill .foo.baz = "hello"
         |  }
         |}""".stripMargin,
      """add .foo.baz: String = "hello""""
    )
  }

  it should "allow dropping nested fields" in {
    validate(
      """|collection User {
         |  foo: {
         |    bar: Int,
         |    baz: String
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: {
         |    bar: Int
         |  }
         |
         |  migrations {
         |    drop .foo.baz
         |  }
         |}""".stripMargin,
      "drop .foo.baz"
    )
  }

  it should "allow adding a parent field" in {
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard: Null
         |  foo: {
         |    baz: Int
         |  }
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = { bar: 3 }
         |    move .foo.bar -> .foo.baz
         |  }
         |}""".stripMargin,
      """|add .foo: { bar: Int } = {
         |  bar: 3
         |}
         |move .foo.bar -> .foo.baz""".stripMargin
    )
  }

  it should "disallow backfilling nested fields of the wrong type" in {
    // TODO: This error needs to chill a bit. Like it's just one wrong field
    // and it's a page long.
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard: Null
         |  foo: {
         |    bar: Int
         |  }
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = { wrong_field: 3 }
         |  }
         |}""".stripMargin,
      """|error: Type `{ wrong_field: 3 }` is not a subtype of `{ bar: Int }`
         |at main.fsl:9:21
         |  |
         |9 |     backfill .foo = { wrong_field: 3 }
         |  |                     ^^^^^^^^^^^^^^^^^^
         |  |
         |hint: Field type defined here
         |at main.fsl:3:8
         |  |
         |3 |     foo: {
         |  |  ________^
         |4 | |     bar: Int
         |5 | |   }
         |  | |___^
         |  |
         |cause: Type `{ wrong_field: 3 }` contains extra field `wrong_field`
         |at main.fsl:3:8
         |  |
         |3 |     foo: {
         |  |  ________^
         |4 | |     bar: Int
         |5 | |   }
         |  | |___^
         |  |
         |cause: Type `{ wrong_field: 3 }` does not have field `bar`
         |at main.fsl:3:8
         |  |
         |3 |     foo: {
         |  |  ________^
         |4 | |     bar: Int
         |5 | |   }
         |  | |___^
         |  |
         |hint: Type `{ wrong_field: 3 }` inferred here
         |  |
         |9 |     backfill .foo = { wrong_field: 3 }
         |  |                     ^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow backfilling nested fields without an add or split" in {
    validate(
      """|collection User {
         |  foo: {
         |    bar: Int
         |  }
         |}""".stripMargin,
      """|collection User {
         |  foo: {
         |    bar: Int,
         |    baz: Int
         |  }
         |
         |  migrations {
         |    backfill .foo.baz = 1
         |  }
         |}""".stripMargin,
      """|error: Field `.foo.baz` is backfilled without being declared by an `add` or `split` migration
         |at main.fsl:8:5
         |  |
         |8 |     backfill .foo.baz = 1
         |  |     ^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "allow adding nested fields in multiple steps" in {
    // TODO: It might be nice if `add .foo.bar` inferred the `add .foo` and
    // `backfill .foo = {}`. Maybe something for later.
    //
    // However, the internal migration to add `.foo` as an empty struct is strictly
    // required. The internal migrations will throw ISEs if there isn't a parent
    // struct to add a field into.
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard: Null
         |  foo: {
         |    bar: Int
         |  }
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = {}
         |
         |    add .foo.bar
         |    backfill .foo.bar = 0
         |  }
         |}""".stripMargin,
      """|add .foo: {} = {
         |}
         |add .foo.bar: Int = 0""".stripMargin
    )
  }

  it should "disallow adding fields without a parent" in {
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard: Null
         |
         |  migrations {
         |    add .foo.bar
         |    backfill .foo.bar = 0
         |  }
         |}""".stripMargin,
      """|error: Cannot backfill field `.foo.bar`, as it is not in the submitted schema
         |at main.fsl:6:14
         |  |
         |6 |     backfill .foo.bar = 0
         |  |              ^^^^^^^^
         |  |
         |error: Cannot add field, as `.foo.bar` is not in the submitted schema
         |at main.fsl:5:9
         |  |
         |5 |     add .foo.bar
         |  |         ^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "disallow adding fields to a parent that isn't a struct" in {
    // TODO: This isn't quite ideal. `.foo` is in the submitted schema, its
    // just not a struct.
    validate(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  _no_wildcard: Null
         |  foo: Int
         |
         |  migrations {
         |    add .foo.bar
         |    backfill .foo.bar = 0
         |  }
         |}""".stripMargin,
      """|error: Cannot backfill field `.foo.bar`, as it is not in the submitted schema
         |at main.fsl:7:14
         |  |
         |7 |     backfill .foo.bar = 0
         |  |              ^^^^^^^^
         |  |
         |error: Cannot add field, as `.foo.bar` is not in the submitted schema
         |at main.fsl:6:9
         |  |
         |6 |     add .foo.bar
         |  |         ^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "allow splitting out of nested fields without blowing up" in {
    // This splits out of `foo`, and drops `foo`, so we need some checks to make sure
    // we don't try and add the nested field back while rewinding.
    validate(
      """|collection User {
         |  bar: {}
         |  foo: {
         |    nested: Int | String
         |  }
         |}""".stripMargin,
      """|collection User {
         |  bar: {
         |    hello: Int,
         |    world: String
         |  }
         |
         |  migrations {
         |    split .foo.nested -> .bar.hello, .bar.world
         |    backfill .bar.hello = 0
         |    backfill .bar.world = ""
         |
         |    drop .foo
         |  }
         |}""".stripMargin,
      """|split .foo.nested -> (.bar.hello: Int = 0), (.bar.world: String = "")
         |drop .foo""".stripMargin
    )
  }

  it should "allow defaults in nested fields" in {
    validate(
      """|collection Foo {
         |  a: {}
         |}""".stripMargin,
      """|collection Foo {
         |  a: {
         |    b: Int = 3
         |    c: String = "hi"
         |    d: Boolean
         |  }
         |
         |  migrations {
         |    add .a.b
         |    add .a.c
         |    add .a.d
         |    backfill .a.d = true
         |  }
         |}""".stripMargin,
      """|add .a.b: Int = 3
         |add .a.c: String = "hi"
         |add .a.d: Boolean = true""".stripMargin
    )

    validate(
      """|collection Foo {
         |  a: {}
         |}""".stripMargin,
      """|collection Foo {
         |  a: {
         |    b: Int
         |    c: String = "hi"
         |    d: Boolean = true
         |  }
         |
         |  migrations {
         |    add .a.b
         |    add .a.c
         |    add .a.d
         |  }
         |}""".stripMargin,
      """|error: Cannot declare non-nullable field `.a.b`: it has no default and no backfill value
         |at main.fsl:9:9
         |  |
         |9 |     add .a.b
         |  |         ^^^^
         |  |
         |hint: Make the field nullable
         |at main.fsl:3:11
         |  |
         |3 |     b: Int?
         |  |           +
         |  |
         |hint: Add a default value to this field
         |at main.fsl:3:11
         |  |
         |3 |     b: Int = <expr>
         |  |           +++++++++
         |  |
         |hint: Add a `backfill` migration
         |at main.fsl:12:3
         |   |
         |12 |     backfill .a.b = <expr>
         |   |   +++++++++++++++++++++++++
         |   |""".stripMargin
    )
  }

  it should "disallow a migration referring to any nested fields with a wildcard" in {
    validate(
      """|collection Foo {
         |  address: {
         |    *: Any
         |  }
         |}""".stripMargin,
      """|collection Foo {
         |  address: {
         |    street: String
         |    *: Any
         |  }
         |
         |  migrations {
         |    add .address.street
         |    backfill .address.street = ""
         |
         |    add .address.tmp
         |    split .address.street -> .address.street, .address.tmp
         |    drop .address.tmp
         |
         |    add .foo
         |    move .foo -> .address.foo
         |    drop .foo
         |  }
         |}""".stripMargin,
      """|error: Cannot move field `.address.foo` into a struct with a wildcard definition
         |at main.fsl:16:18
         |   |
         |16 |     move .foo -> .address.foo
         |   |                  ^^^^^^^^^^^^
         |   |
         |error: Cannot split field `.address.street`, which is in a struct with a wildcard definition
         |at main.fsl:12:11
         |   |
         |12 |     split .address.street -> .address.street, .address.tmp
         |   |           ^^^^^^^^^^^^^^^
         |   |
         |error: Cannot split into field `.address.street`, which is in a struct with a wildcard definition
         |at main.fsl:12:30
         |   |
         |12 |     split .address.street -> .address.street, .address.tmp
         |   |                              ^^^^^^^^^^^^^^^
         |   |
         |error: Cannot split into field `.address.tmp`, which is in a struct with a wildcard definition
         |at main.fsl:12:47
         |   |
         |12 |     split .address.street -> .address.street, .address.tmp
         |   |                                               ^^^^^^^^^^^^
         |   |
         |error: Cannot add field, as there is a wildcard in the same struct as `.address.tmp`
         |at main.fsl:11:9
         |   |
         |11 |     add .address.tmp
         |   |         ^^^^^^^^^^^^
         |   |
         |error: Cannot backfill field, as there is a wildcard in the same struct as `.address.street`
         |at main.fsl:9:14
         |  |
         |9 |     backfill .address.street = ""
         |  |              ^^^^^^^^^^^^^^^
         |  |
         |error: Cannot add field, as there is a wildcard in the same struct as `.address.street`
         |at main.fsl:8:9
         |  |
         |8 |     add .address.street
         |  |         ^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    validate(
      """|collection Foo {
         |  address: {
         |    street: String
         |    *: Any
         |  }
         |}""".stripMargin,
      """|collection Foo {
         |  address: {
         |    *: Any
         |  }
         |  street: String
         |
         |  migrations {
         |    move .address.street -> .street
         |  }
         |}""".stripMargin,
      """|error: Cannot move field `.address.street` out of a struct with a wildcard definition
         |at main.fsl:8:10
         |  |
         |8 |     move .address.street -> .street
         |  |          ^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "allow dropping a field within a struct with a wildcard" in {
    validate(
      """|collection Foo {
         |  address: {
         |    street: String
         |    *: Any
         |  }
         |}""".stripMargin,
      """|collection Foo {
         |  address: {
         |    *: Any
         |  }
         |
         |  migrations {
         |    drop .address.street
         |  }
         |}""".stripMargin,
      "drop .address.street"
    )
  }

  it should "disallow migrations when there is a wildcard in the parent field" in {
    // FIXME: This error isn't quite right, but making it any more precise
    // would be too verbose.
    validate(
      """|collection Foo {
         |  address: {
         |    street: {}
         |    *: Any
         |  }
         |}""".stripMargin,
      """|collection Foo {
         |  address: {
         |    street: {
         |      name: String
         |    }
         |    *: Any
         |  }
         |
         |  migrations {
         |    add .address.street.name
         |    backfill .address.street.name = ""
         |  }
         |}""".stripMargin,
      """|error: Cannot backfill field, as there is a wildcard in the same struct as `.address.street.name`
         |at main.fsl:11:14
         |   |
         |11 |     backfill .address.street.name = ""
         |   |              ^^^^^^^^^^^^^^^^^^^^
         |   |
         |error: Cannot add field, as there is a wildcard in the same struct as `.address.street.name`
         |at main.fsl:10:9
         |   |
         |10 |     add .address.street.name
         |   |         ^^^^^^^^^^^^^^^^^^^^
         |   |""".stripMargin
    )
  }

  it should "implicitly add in empty objects" in {
    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  address: {
         |    street: String
         |  }
         |
         |  migrations {
         |    add .address.street
         |    backfill .address.street = ""
         |  }
         |}""".stripMargin,
      """|add .address.street: String = """"".stripMargin
    )

    // All children fields must be added for the implicit migration to be allowed.
    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  address: {
         |    street: String
         |    city: String
         |  }
         |
         |  migrations {
         |    add .address.street
         |    backfill .address.street = ""
         |  }
         |}""".stripMargin,
      """|error: Field `.address.city` is not present in the live schema
         |at main.fsl:5:5
         |  |
         |5 |     city: String
         |  |     ^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |5 |       migrations {
         |  |  _____+
         |6 | |     add .city
         |7 | |     backfill .city = <expr>
         |8 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |  |
         |5 |     city: String = <expr>
         |  |                 +++++++++
         |  |
         |hint: Make the field nullable
         |  |
         |5 |     city: String?
         |  |                 +
         |  |""".stripMargin
    )

    // If multiple children are missing, the error becomes more generic.
    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  address: {
         |    street: String
         |    city: String
         |    state: String
         |  }
         |
         |  migrations {
         |    add .address.street
         |    backfill .address.street = ""
         |  }
         |}""".stripMargin,
      """|error: Field `.address` is not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   address: {
         |  |   ^^^^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |3 |     migrations {
         |  |  ___+
         |4 | |     add .address
         |5 | |     backfill .address = <expr>
         |6 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |at main.fsl:7:4
         |  |
         |7 |   } = <expr>
         |  |    +++++++++
         |  |
         |hint: Make the field nullable
         |at main.fsl:7:4
         |  |
         |7 |   }?
         |  |    +
         |  |""".stripMargin
    )
  }

  it should "implicitly add parent structs recursively" in {
    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  address: {
         |    foo: {
         |      bar: String
         |    }
         |  }
         |
         |  migrations {
         |    add .address.foo.bar
         |    backfill .address.foo.bar = ""
         |  }
         |}""".stripMargin,
      """|add .address.foo.bar: String = """"".stripMargin
    )

    validate(
      """|collection Foo {
         |  _no_wildcard: Int
         |}""".stripMargin,
      """|collection Foo {
         |  _no_wildcard: Int
         |  address: {
         |    foo: {
         |      bar: String
         |      baz: String
         |    }
         |  }
         |
         |  migrations {
         |    add .address.foo.bar
         |    backfill .address.foo.bar = ""
         |  }
         |}""".stripMargin,
      """|error: Field `.address.foo.baz` is not present in the live schema
         |at main.fsl:6:7
         |  |
         |6 |       baz: String
         |  |       ^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |6 |         migrations {
         |  |  _______+
         |7 | |     add .baz
         |8 | |     backfill .baz = <expr>
         |9 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |  |
         |6 |       baz: String = <expr>
         |  |                  +++++++++
         |  |
         |hint: Make the field nullable
         |  |
         |6 |       baz: String?
         |  |                  +
         |  |""".stripMargin
    )
  }

  it should "allow adding nested fields when there is a wildcard" in {
    validate(
      """|collection Foo {
         |  *: Any
         |  address: {}
         |}""".stripMargin,
      """|collection Foo {
         |  *: Any
         |  address: {
         |    street: String
         |  }
         |
         |  migrations {
         |    add .address.street
         |    backfill .address.street = ""
         |  }
         |}""".stripMargin,
      """|add .address.street: String = """"".stripMargin
    )

    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  *: Any
         |  address: {
         |    street: String
         |  }
         |
         |  migrations {
         |    add .address.street
         |    backfill .address.street = ""
         |  }
         |}""".stripMargin,
      """|error: Field `.address` is not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   address: {
         |  |   ^^^^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |3 |     migrations {
         |  |  ___+
         |4 | |     add .address
         |5 | |     backfill .address = <expr>
         |6 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |at main.fsl:5:4
         |  |
         |5 |   } = <expr>
         |  |    +++++++++
         |  |
         |hint: Make the field nullable
         |at main.fsl:5:4
         |  |
         |5 |   }?
         |  |    +
         |  |""".stripMargin
    )
  }

  it should "work when dropping the parent record" in {
    validate(
      """|collection User {
         |  foo: {
         |    id: String
         |  }
         |
         |  index byFoo {
         |    terms [.foo.id]
         |  }
         |}""".stripMargin,
      """|collection User {
         |  bar: String
         |
         |  index byFoo {
         |    terms [.bar]
         |  }
         |
         |
         |  migrations {
         |    move .foo.id -> .bar
         |  }
         |}""".stripMargin,
      """|error: Cannot move field `.foo.id`, as the parent object was removed.
         |at main.fsl:10:10
         |   |
         |10 |     move .foo.id -> .bar
         |   |          ^^^^^^^
         |   |""".stripMargin
    )

    validate(
      """|collection User {
         |  foo: {
         |    id: String
         |  }
         |
         |  index byFoo {
         |    terms [.foo.id]
         |  }
         |}""".stripMargin,
      """|collection User {
         |  bar: String
         |
         |  index byFoo {
         |    terms [.bar]
         |  }
         |
         |
         |  migrations {
         |    move .foo.id -> .bar
         |    drop .foo
         |  }
         |}""".stripMargin,
      """|move .foo.id -> .bar
         |drop .foo""".stripMargin
    )
  }
}
