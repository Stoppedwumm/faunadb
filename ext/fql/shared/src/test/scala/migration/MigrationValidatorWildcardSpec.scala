package fql.test

class MigrationValidatorWildcardSpec extends MigrationSpec {
  "A MigrationValidator" should "handle wildcards correctly" in {
    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: Any
         |  *: Any
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      ""
    )

    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String
         |  *: Any
         |
         |  migrations {
         |    add .foo
         |    backfill .foo = ""
         |  }
         |}""".stripMargin,
      """|error: Cannot add field, as `.foo` conflicts with the wildcard
         |at main.fsl:6:9
         |  |
         |6 |     add .foo
         |  |         ^^^^
         |  |
         |hint: Add a `move_conflicts` migration
         |  |
         |6 |       add .foo
         |  |  _____________+
         |7 | |     move_conflicts .conflicts
         |  | |_____________________________^
         |  |""".stripMargin
    )

    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: Any
         |  dump: { *: Any }?
         |  *: Any
         |
         |  migrations {
         |    add .dump
         |    add .foo
         |    backfill .foo = ""
         |    move_conflicts .dump
         |  }
         |}""".stripMargin,
      """|add .dump: { *: Any } | Null
         |add .foo: Any = ""
         |move_conflicts .dump""".stripMargin
    )

    // FIXME: The order of `add` shouldn't matter.
    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: Any
         |  dump: { *: Any }?
         |  *: Any
         |
         |  migrations {
         |    add .foo
         |    add .dump
         |    backfill .foo = ""
         |    move_conflicts .dump
         |  }
         |}""".stripMargin,
      """|add .foo: Any = ""
         |add .dump: { *: Any } | Null
         |move_conflicts .dump""".stripMargin
    )
  }

  it should "disallow move_conflicts when there isn't a wildcard" in {
    validate(
      """|collection Foo {
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String
         |  dump: { *: String }?
         |
         |  migrations {
         |    add .dump
         |    add .foo
         |    backfill .foo = ""
         |    move_conflicts .dump
         |  }
         |}""".stripMargin,
      """|error: Cannot move wildcard conflicts, as there is no wildcard in the submitted schema
         |at main.fsl:9:20
         |  |
         |9 |     move_conflicts .dump
         |  |                    ^^^^^
         |  |
         |hint: Add a wildcard constraint
         |at main.fsl:3:23
         |  |
         |3 |     dump: { *: String }?
         |  |  _______________________+
         |4 | |   *: Any
         |  | |________^
         |  |
         |hint: Move the extra fields into a conflicts field with `move_wildcard`
         |at main.fsl:10:3
         |   |
         |10 |     move_wildcard .dump
         |   |   ++++++++++++++++++++++
         |   |""".stripMargin
    )
  }

  it should "handle Any correctly" in {
    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: Any
         |  *: Any
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      ""
    )

    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String?
         |  *: Any
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      """|error: Cannot add field, as `.foo` conflicts with the wildcard
         |at main.fsl:6:9
         |  |
         |6 |     add .foo
         |  |         ^^^^
         |  |
         |hint: Add a `move_conflicts` migration
         |  |
         |6 |       add .foo
         |  |  _____________+
         |7 | |     move_conflicts .conflicts
         |  | |_____________________________^
         |  |""".stripMargin
    )
  }

  it should "move_wildcard to another field" in {
    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String?
         |  dump: { *: Any }?
         |
         |  migrations {
         |    add .dump
         |    add .foo
         |    move_conflicts .dump
         |    move_wildcard .dump
         |  }
         |}""".stripMargin,
      """|add .dump: { *: Any } | Null
         |add .foo: String | Null
         |move_conflicts .dump
         |move_wildcard -> .dump [foo, dump]""".stripMargin
    )
  }

  it should "allow adding a dump field for non-any wildcards" in {
    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String?
         |  dump: { *: Any }?
         |
         |  migrations {
         |    // #1. Add a field which is a subtype of the wildcard. This
         |    // adds a migration, which won't do anything.
         |    add .foo
         |
         |    // #2. Add the dump field, which is not a subtype of the wildcard.
         |    // This requires a `move_conflicts` migration.
         |    add .dump
         |    move_conflicts .dump
         |
         |    // #3. Now bump the wildcard into `dump`.
         |    move_wildcard .dump
         |  }
         |}""".stripMargin,
      """|add .foo: String | Null
         |add .dump: { *: Any } | Null
         |move_conflicts .dump
         |move_wildcard -> .dump [foo, dump]""".stripMargin
    )
  }

  it should "handle the implicit wildcard correctly" in {
    validate(
      """|collection Foo {
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String = ""
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      """|error: Missing wildcard definition
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   foo: String = ""
         |3 | |
         |4 | |   migrations {
         |5 | |     add .foo
         |6 | |   }
         |7 | | }
         |  | |_^
         |  |
         |hint: Collections with no fields defined allow arbitrary fields by default. To keep this behavior, add a wildcard declaration to the schema definition
         |at main.fsl:2:3
         |  |
         |2 |   *: Any
         |  |   +++++++
         |  |""".stripMargin
    )

    validate(
      """|collection Foo {
         |}""".stripMargin,
      """|collection Foo {
         |  foo: Any
         |  *: Any
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      ""
    )

    validate(
      """|collection Foo {
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String
         |  dump: { *: Any }?
         |  *: Any
         |
         |  migrations {
         |    add .dump
         |    add .foo
         |    backfill .foo = ""
         |    move_conflicts .dump
         |  }
         |}""".stripMargin,
      """|add .dump: { *: Any } | Null
         |add .foo: String = ""
         |move_conflicts .dump""".stripMargin
    )
  }

  it should "disallow removing a wildcard" in {
    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: Int?
         |}""".stripMargin,
      """|error: Field `.foo` is not present in the live schema
         |at main.fsl:2:3
         |  |
         |2 |   foo: Int?
         |  |   ^^^
         |  |
         |hint: Provide an `add` migration for this field
         |at main.fsl:3:1
         |  |
         |3 |   migrations {
         |  |  _+
         |4 | |     add .foo
         |5 | |   }
         |  | |____^
         |  |
         |error: Missing wildcard definition
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   foo: Int?
         |3 | | }
         |  | |_^
         |  |
         |hint: Add the wildcard back
         |at main.fsl:2:3
         |  |
         |2 |   *: Any
         |  |   +++++++
         |  |
         |hint: Move the extra fields with a `move_wildcard` migration
         |at main.fsl:2:3
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     move_wildcard .conflicts
         |4 | |   }
         |  | |____^
         |  |
         |hint: Drop the extra fields with a `move_wildcard` and `drop` migration
         |at main.fsl:2:3
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     move_wildcard .conflicts
         |4 | |     drop .conflicts
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )

    validate(
      """|collection Foo {
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  foo: String = ""
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      """|error: Missing wildcard definition
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   foo: String = ""
         |3 | |
         |4 | |   migrations {
         |5 | |     add .foo
         |6 | |   }
         |7 | | }
         |  | |_^
         |  |
         |hint: Add the wildcard back
         |at main.fsl:2:3
         |  |
         |2 |   *: Any
         |  |   +++++++
         |  |
         |hint: Move the extra fields with a `move_wildcard` migration
         |at main.fsl:6:3
         |  |
         |6 |     move_wildcard .conflicts
         |  |   +++++++++++++++++++++++++++
         |  |
         |hint: Drop the extra fields with a `move_wildcard` and `drop` migration
         |at main.fsl:6:3
         |  |
         |6 |       move_wildcard .conflicts
         |  |  ___+
         |7 | |     drop .conflicts
         |  | |____________________^
         |  |""".stripMargin
    )
  }

  it should "allow adding a wildcard" in {
    // add_wildcard required.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |  *: Any
         |}""".stripMargin,
      """|error: Wildcard was not present in the live schema
         |at main.fsl:3:3
         |  |
         |3 |   *: Any
         |  |   ^^^^^^
         |  |
         |hint: Add the wildcard with an `add_wildcard` migration
         |at main.fsl:4:1
         |  |
         |4 |   migrations {
         |  |  _+
         |5 | |     add_wildcard
         |6 | |   }
         |  | |____^
         |  |""".stripMargin
    )

    // Wildcard declaration required.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |
         |  migrations {
         |    add_wildcard
         |  }
         |}""".stripMargin,
      """|error: Cannot add wildcard, as the submitted schema lacks a wildcard
         |at main.fsl:5:5
         |  |
         |5 |     add_wildcard
         |  |     ^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // OK.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |  *: Any
         |
         |  migrations {
         |    add_wildcard
         |  }
         |}""".stripMargin,
      "add_wildcard"
    )
  }

  it should "allow adding an implicit wildcard" in {
    // add_wildcard required for the implicit wildcard.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  migrations {
         |    drop .a
         |  }
         |}""".stripMargin,
      """|error: All fields from the live schema are dropped, reintroducing the implicit wildcard that requires an `add_wildcard` migration. Are you sure you meant to do this?
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   migrations {
         |3 | |     drop .a
         |4 | |   }
         |5 | | }
         |  | |_^
         |  |""".stripMargin
    )

    // OK.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  migrations {
         |    drop .a
         |    add_wildcard
         |  }
         |}""".stripMargin,
      """|drop .a
         |add_wildcard""".stripMargin
    )

    // Also OK to be explicit.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  *: Any
         |  migrations {
         |    drop .a
         |    add_wildcard
         |  }
         |}""".stripMargin,
      """|drop .a
         |add_wildcard""".stripMargin
    )
  }

  it should "handle add_wildcard and move_wildcard together" in {
    // Re-adding a wildcard requires add_wildcard.
    validate(
      """|collection Foo {
         |  a: Int
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |  *: Any
         |
         |  migrations {
         |    add .dump
         |    move_wildcard .dump
         |    drop .dump
         |  }
         |}""".stripMargin,
      """|error: Cannot move wildcard, as there is still a wildcard in the submitted schema
         |at main.fsl:7:19
         |  |
         |7 |     move_wildcard .dump
         |  |                   ^^^^^
         |  |""".stripMargin
    )

    // OK.
    validate(
      """|collection Foo {
         |  a: Int
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |  *: Any
         |
         |  migrations {
         |    add .dump
         |    move_wildcard .dump
         |    drop .dump
         |    add_wildcard
         |  }
         |}""".stripMargin,
      """|move_wildcard -> .dump [a, dump]
         |drop .dump
         |add_wildcard""".stripMargin
    )

    // Dropping an added wildcard requires move_wildcard.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |
         |  migrations {
         |    add_wildcard
         |  }
         |}""".stripMargin,
      """|error: Cannot add wildcard, as the submitted schema lacks a wildcard
         |at main.fsl:5:5
         |  |
         |5 |     add_wildcard
         |  |     ^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // OK.
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |
         |  migrations {
         |    add_wildcard
         |    add .dump
         |    move_wildcard .dump
         |    drop .dump
         |  }
         |}""".stripMargin,
      """|add_wildcard
         |move_wildcard -> .dump [a, dump]
         |drop .dump""".stripMargin
    )
  }

  it should "disallow double-adding wildcards" in {
    validate(
      """|collection Foo {
         |  a: Int
         |}""".stripMargin,
      """|collection Foo {
         |  a: Int
         |  *: Any
         |
         |  migrations {
         |    add_wildcard
         |    add_wildcard
         |  }
         |}""".stripMargin,
      """|error: Cannot add a wildcard twice
         |at main.fsl:7:5
         |  |
         |7 |     add_wildcard
         |  |     ^^^^^^^^^^^^
         |  |
         |hint: Remove the duplicate `add_wildcard` migration
         |  |
         |7 |     add_wildcard
         |  |     ------------
         |  |""".stripMargin
    )
  }

  it should "not hit an exception when removing a wildcard in an invalid way" in {
    validate(
      """|collection Foo {
         |  slug: Int
         |  *: Any
         |}""".stripMargin,
      """|collection Foo {
         |  slug: Int
         |}""".stripMargin,
      """|error: Missing wildcard definition
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   slug: Int
         |3 | | }
         |  | |_^
         |  |
         |hint: Add the wildcard back
         |at main.fsl:2:3
         |  |
         |2 |   *: Any
         |  |   +++++++
         |  |
         |hint: Move the extra fields with a `move_wildcard` migration
         |at main.fsl:2:3
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     move_wildcard .conflicts
         |4 | |   }
         |  | |____^
         |  |
         |hint: Drop the extra fields with a `move_wildcard` and `drop` migration
         |at main.fsl:2:3
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     move_wildcard .conflicts
         |4 | |     drop .conflicts
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )
  }
}
