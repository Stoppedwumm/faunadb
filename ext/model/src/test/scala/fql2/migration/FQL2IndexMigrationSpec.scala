package fauna.model.test

import fauna.auth.Auth
import fauna.model.{ Collection, Index }
import fauna.model.schema.index.CollectionIndexManager
import fauna.repo.values.Value

class FQL2IndexMigrationSpec extends FQL2Spec {
  var auth: Auth = _

  before {
    auth = newDB
  }

  def backingIndexFor(auth: Auth, coll: String, index: String) = {
    ctx ! (for {
      id      <- Collection.idByNameActive(auth.scopeID, coll).map(_.get)
      collDoc <- ModelStore.get(auth.scopeID, id.toDocID)
    } yield {
      val mgr = CollectionIndexManager(auth.scopeID, id, collDoc.get.data)
      val indexDefinition = mgr.userDefinedIndexes(index)
      mgr.findBackingIndexOrError(indexDefinition)
    })
  }

  def backingIndexForUnique(auth: Auth, coll: String) = {
    ctx ! (for {
      id      <- Collection.idByNameActive(auth.scopeID, coll).map(_.get)
      collDoc <- ModelStore.get(auth.scopeID, id.toDocID)
    } yield {
      val mgr = CollectionIndexManager(auth.scopeID, id, collDoc.get.data)
      val indexDefinition = mgr.uniqueConstraints.head.toIndexDefinition
      mgr.findBackingIndexOrError(indexDefinition)
    })
  }

  def check(
    auth: Auth,
    prev: String,
    next: String,
    rebuilds: Seq[String],
    create: String => String = _ => "User.create({})") = {
    Seq(true, false).foreach { isSyncBuild =>
      val coll = "User"

      updateSchemaOk(auth, "main.fsl" -> prev)

      // Make a doc so that we actually migrate something.
      evalOk(auth, create("0"))

      if (!isSyncBuild) {
        val n = Index.BuildSyncSize + 1
        evalOk(auth, s"Set.sequence(1, $n).forEach(i => ${create("i")})")
      }

      val oldIds = ctx ! (for {
        id      <- Collection.idByNameActive(auth.scopeID, coll).map(_.get)
        collDoc <- ModelStore.get(auth.scopeID, id.toDocID)
        mgr = CollectionIndexManager(auth.scopeID, id, collDoc.get.data)
      } yield {

        mgr.userDefinedIndexes.map { case (name, index) =>
          name -> mgr.findBackingIndexOrError(index).indexID
        }.toMap
      })

      updateSchemaOk(auth, "main.fsl" -> next)

      ctx ! (for {
        id      <- Collection.idByNameActive(auth.scopeID, coll).map(_.get)
        collDoc <- ModelStore.get(auth.scopeID, id.toDocID)
        mgr = CollectionIndexManager(auth.scopeID, id, collDoc.get.data)
      } yield {

        mgr.userDefinedIndexes.foreach { case (name, index) =>
          val newID = mgr.findBackingIndexOrError(index).indexID
          if (rebuilds.contains(name)) {
            if (oldIds(name) == newID) {
              fail(s"Index $name was not rebuilt correctly")
            }

            if (isSyncBuild) {
              evalOk(
                auth,
                s"""|let assert_eq = (a, b) => if (a != b) abort("assertion failed: #{a} != #{b}")
                    |
                    |assert_eq(User.definition.indexes.$name!.status, "complete")
                    |assert_eq(User.definition.indexes.$name!.queryable, true)
                    |""".stripMargin
              )
            } else {
              evalOk(
                auth,
                s"""|let assert_eq = (a, b) => if (a != b) abort("assertion failed: #{a} != #{b}")
                    |
                    |assert_eq(User.definition.indexes.$name!.status, "building")
                    |assert_eq(User.definition.indexes.$name!.queryable, false)
                    |""".stripMargin
              )
            }
          } else {
            if (oldIds(name) != newID) {
              fail(s"Index $name was unexpectedly rebuilt")
            }

            evalOk(
              auth,
              s"""|let assert_eq = (a, b) => if (a != b) abort("assertion failed: #{a} != #{b}")
                  |
                  |assert_eq(User.definition.indexes.$name!.status, "complete")
                  |assert_eq(User.definition.indexes.$name!.queryable, true)
                  |""".stripMargin
            )
          }
        }
      })

      updateSchemaOk(auth, "main.fsl" -> "")
    }
  }

  "it rebuilds fields that have been split" in {
    check(
      auth,
      """|collection User {
         |  name: String | Time = ""
         |
         |  index byName {
         |    terms [.name]
         |  }
         |}
         |""".stripMargin,
      """|collection User {
         |  name: String = ""
         |  time: Time = Time.now()
         |
         |  index byName {
         |    terms [.name]
         |  }
         |
         |  migrations {
         |    split .name -> .name, .time
         |  }
         |}
         |""".stripMargin,
      Seq("byName")
    )
  }

  "it rebuilds nested fields" in {
    check(
      auth,
      """|collection User {
         |  address: {
         |    street: String
         |  }
         |
         |  index byStreet {
         |    terms [.address.street]
         |  }
         |}
         |""".stripMargin,
      """|collection User {
         |  address: {}
         |  street: String
         |
         |  index byStreet {
         |    terms [.street]
         |  }
         |
         |  migrations {
         |    move .address.street -> .street
         |  }
         |}
         |""".stripMargin,
      Seq.empty,
      create = _ => "User.create({ address: { street: '123 Main St' } })"
    )

    check(
      auth,
      """|collection User {
         |  street: String
         |  address: {}
         |
         |  index byStreet {
         |    terms [.street]
         |  }
         |}
         |""".stripMargin,
      """|collection User {
         |  address: {
         |    street: String
         |  }
         |
         |  index byStreet {
         |    terms [.address.street]
         |  }
         |
         |  migrations {
         |    move .street -> .address.street
         |  }
         |}
         |""".stripMargin,
      Seq.empty,
      create = _ => "User.create({ street: '123 Main St' })"
    )
  }

  "it unique constraints for fields that have been split" in {
    check(
      auth,
      """|collection User {
         |  name: String | Time = ""
         |
         |  index byName {
         |    terms [.name]
         |  }
         |
         |  unique [.name]
         |}
         |""".stripMargin,
      """|collection User {
         |  name: String = ""
         |  time: Time = Time.now()
         |
         |  index byName {
         |    terms [.name]
         |  }
         |
         |  unique [.name]
         |
         |  migrations {
         |    split .name -> .name, .time
         |  }
         |}
         |""".stripMargin,
      Seq("byName"),
      create = v => s"""User.create({ name: "#{$v}" })"""
    )
  }

  "it doesn't rebuild added fields if their type is the same" in {
    check(
      auth,
      """|collection User {
         |  *: Any
         |
         |  index byName {
         |    terms [.name]
         |  }
         |}
         |""".stripMargin,
      """|collection User {
         |  name: Any
         |  *: Any
         |
         |  index byName {
         |    terms [.name]
         |  }
         |
         |  migrations {
         |    add .name
         |  }
         |}
         |""".stripMargin,
      Seq.empty
    )
  }

  "it doesn't rebuild from moved fields" in {
    check(
      auth,
      """|collection User {
         |  name: String = ""
         |
         |  index byName {
         |    terms [.name]
         |  }
         |}
         |""".stripMargin,
      """|collection User {
         |  firstName: String = ""
         |
         |  index byName {
         |    terms [.firstName]
         |  }
         |
         |  migrations {
         |    move .name -> .firstName
         |  }
         |}
         |""".stripMargin,
      Seq.empty
    )
  }

  "it rebuilds indexes from move_conflicts" in {
    check(
      auth,
      """|collection User {
         |  conflicts: { *: Any }?
         |  *: Any
         |
         |  index byName { terms [.name] }
         |  index byConflicts { terms [.conflicts] }
         |}
         |""".stripMargin,
      """|collection User {
         |  conflicts: { *: Any }?
         |  name: String = ""
         |  *: Any
         |
         |  index byName { terms [.name] }
         |  index byConflicts { terms [.conflicts] }
         |
         |  migrations {
         |    add .name
         |    move_conflicts .conflicts
         |  }
         |}
         |""".stripMargin,
      Seq("byName", "byConflicts")
    )
  }

  "it rebuilds indexes from move_wildcard" in {
    // Note that typechecking is off for this test, so we can create the index
    // `byFoo`.
    val auth = newDB(typeChecked = false)
    check(
      auth,
      """|collection User {
         |  name: String = ""
         |  conflicts: { *: Any }?
         |  *: Any
         |
         |  index byName { terms [.name] }
         |  index byConflicts { terms [.conflicts] }
         |  index byFoo { terms [.foo] }
         |}
         |""".stripMargin,
      """|collection User {
         |  name: String = ""
         |  conflicts: { *: Any }?
         |
         |  index byName { terms [.name] }
         |  index byConflicts { terms [.conflicts] }
         |  index byFoo { terms [.foo] }
         |
         |  migrations {
         |    move_wildcard .conflicts
         |  }
         |}
         |""".stripMargin,
      Seq("byConflicts", "byFoo")
    )
  }

  "it does not rebuild indexes when adding without backfilling" in {
    check(
      auth,
      """|collection User {
         |  *: Any
         |
         |  index byName { terms [.name] }
         |}
         |""".stripMargin,
      """|collection User {
         |  name: Any
         |  *: Any
         |
         |  index byName { terms [.name] }
         |
         |  migrations {
         |    add .name
         |  }
         |}
         |""".stripMargin,
      Seq.empty
    )
  }

  "it rebuilds indexes after migrations" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |""".stripMargin
    )

    val index = backingIndexFor(auth, "User", "byName").indexID

    evalOk(auth, "User.create({ name: 'Alice' })")
    evalOk(auth, "User.create({ name: 'Bob' })")
    evalOk(auth, "User.create({ name: 'Carol' })")

    // Add a `backfill` migration for a new field.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  age: Int = 0
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  migrations {
           |    add .age
           |  }
           |}
           |""".stripMargin
    )

    backingIndexFor(auth, "User", "byName").indexID shouldBe index
  }

  "it rebuilds unique constraint indexes after changing schema" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |}
           |""".stripMargin
    )

    val index = backingIndexForUnique(auth, "User").indexID

    evalOk(auth, "User.create({ name: 'Alice' })")
    evalOk(auth, "User.create({ name: 'Bob' })")
    evalOk(auth, "User.create({ name: 'Carol' })")

    // Add a `backfill` migration for a new field.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  age: Int = 0
           |
           |  unique [.name]
           |
           |  migrations {
           |    add .age
           |  }
           |}
           |""".stripMargin
    )

    backingIndexForUnique(auth, "User").indexID shouldBe index
  }

  "it keeps migrations when rebuilding indexes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: String | Int
           |
           |  unique [.foo]
           |}
           |""".stripMargin
    )

    val index = backingIndexForUnique(auth, "User").indexID

    evalOk(auth, "User.create({ id: 0, foo: 'Alice' })")
    evalOk(auth, "User.create({ id: 1, foo: 3 })")

    // Add a new field.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: String | Int
           |  bar: Int = 0
           |
           |  unique [.foo]
           |
           |  migrations {
           |    add .bar
           |  }
           |}
           |""".stripMargin
    )

    backingIndexForUnique(auth, "User").indexID shouldBe index

    evalOk(auth, "User(0)! { foo, bar }") shouldBe Value.Struct(
      "foo" -> Value.Str("Alice"),
      "bar" -> Value.Int(0))

    evalOk(auth, "User(1)! { foo, bar }") shouldBe Value.Struct(
      "foo" -> Value.Int(3),
      "bar" -> Value.Int(0))

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: String
           |  bar: Int = 0
           |  baz: Int
           |
           |  unique [.foo]
           |
           |  migrations {
           |    add .bar
           |
           |    split .foo -> .foo, .baz
           |    backfill .foo = ""
           |    backfill .baz = 0
           |  }
           |}
           |""".stripMargin
    )

    backingIndexForUnique(auth, "User").indexID shouldNot be(index)

    evalOk(auth, "User(0)! { foo, bar, baz }") shouldBe Value.Struct(
      "foo" -> Value.Str("Alice"),
      "bar" -> Value.Int(0),
      "baz" -> Value.Int(0))
    evalOk(auth, "User(1)! { foo, bar, baz }") shouldBe Value.Struct(
      "foo" -> Value.Str(""),
      "bar" -> Value.Int(0),
      "baz" -> Value.Int(3))
  }

  "it builds indexes using the updated schema" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |""".stripMargin
    )

    val doc = evalOk(auth, "User.create({ name: 'Bob' })")

    // Let's just be sure here
    evalOk(auth, "User.byName('Bob').toArray()") shouldBe Value.Array(doc)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  firstName: String
           |  city: String = 'SF'
           |
           |  // index of moved field
           |  index byName {
           |    terms [.firstName]
           |  }
           |
           |  // index of new, backfilled field
           |  index byCity {
           |    terms [.city]
           |  }
           |
           |  migrations {
           |    move .name -> .firstName
           |    add .city
           |  }
           |}
           |""".stripMargin
    )

    // If we've built or migrated these indexes correctly, they should still work
    evalOk(auth, "User.byName('Bob').toArray()") shouldBe Value.Array(doc)
    evalOk(auth, "User.byCity('SF').toArray()") shouldBe Value.Array(doc)

    // And finally, add some more docs with the new field names to make sure the
    // index got updated.
    evalOk(auth, "User.byName('Bob').first()!.update({ firstName: 'Alice' })")
    val doc2 = evalOk(auth, "User.create({ firstName: 'Alice' })")

    evalOk(auth, "User.byName('Alice').toArray()") shouldBe Value.Array(doc, doc2)
  }

  "it updates indexes and unique constraints correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |""".stripMargin
    )

    val doc = evalOk(auth, "User.create({ name: 'Bob' })")

    // Let's just be sure here
    evalOk(auth, "User.byName('Bob').toArray()") shouldBe Value.Array(doc)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  firstName: String
           |
           |  unique [.firstName]
           |
           |  index byName {
           |    terms [.firstName]
           |  }
           |
           |  migrations {
           |    move .name -> .firstName
           |  }
           |}
           |""".stripMargin
    )

    // If we've built or migrated these indexes correctly, they should still work
    evalOk(auth, "User.byName('Bob').toArray()") shouldBe Value.Array(doc)

    // And the unique constraint should still apply
    renderErr(auth, "User.create({ firstName: 'Bob' })") shouldBe (
      """|error: Failed unique constraint.
         |constraint failures:
         |  firstName: Failed unique constraint
         |at *query*:1:12
         |  |
         |1 | User.create({ firstName: 'Bob' })
         |  |            ^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "it allows indexing the 'data' field" in {
    // NB: `data` isn't part of the static type, so we add `*: Any` to get around
    // that.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  name2: String?
           |  *: Any
           |
           |  index byData {
           |    terms [.data]
           |  }
           |}""".stripMargin
    )

    val initial = backingIndexFor(auth, "User", "byData").indexID

    evalOk(auth, "User.create({ id: 1, name: 'Alice' })")

    evalOk(auth, "User.byData({ name: 'Alice' }).map(.id).toArray()") shouldBe (
      Value.Array(Value.ID(1))
    )

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name2: String
           |  *: Any
           |
           |  migrations {
           |    drop .name2
           |    move .name -> .name2
           |  }
           |
           |  index byData {
           |    terms [.data]
           |  }
           |}""".stripMargin
    )

    backingIndexFor(auth, "User", "byData").indexID shouldNot be(initial)
  }

  "it rebuilds fields with numerical indexes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: Array<String> | Array<Number>
           |
           |  index byName {
           |    terms [.name[0]]
           |  }
           |}""".stripMargin
    )

    val initial = backingIndexFor(auth, "User", "byName").indexID

    evalOk(auth, "User.create({ id: 1, name: ['Alice'] })")
    evalOk(auth, "User.create({ id: 2, name: [3] })")

    evalOk(auth, "User.byName('Alice').map(.id).toArray()") shouldBe (
      Value.Array(Value.ID(1))
    )
    evalOk(auth, "User.byName(3).map(.id).toArray()") shouldBe (
      Value.Array(Value.ID(2))
    )

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: Array<String>?
           |  name2: Array<Number>?
           |
           |  migrations {
           |    split .name -> .name, .name2
           |  }
           |
           |  index byName {
           |    terms [.name[0]]
           |  }
           |  index byName2 {
           |    terms [.name2[0]]
           |  }
           |}""".stripMargin
    )
    val index2 = backingIndexFor(auth, "User", "byName").indexID

    index2 shouldNot be(initial)

    evalOk(auth, "User.byName('Alice').map(.id).toArray()") shouldBe (
      Value.Array(Value.ID(1))
    )
    evalOk(auth, "User.byName(3).map(.id).toArray()", typecheck = false) shouldBe (
      Value.Array()
    )
    evalOk(auth, "User.byName2(3).map(.id).toArray()") shouldBe (
      Value.Array(Value.ID(2))
    )
    evalOk(
      auth,
      "User.byName2('Alice').map(.id).toArray()",
      typecheck = false) shouldBe (
      Value.Array()
    )

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: Array<String>?
           |  name2: Array<Number>?
           |
           |  migrations {
           |    split .name -> .name, .name2
           |  }
           |
           |  compute foo = _ => 3
           |
           |  index byName {
           |    terms [.name[0]]
           |  }
           |  index byName2 {
           |    terms [.name2[0]]
           |  }
           |}""".stripMargin
    )

    // No migrations added, so the index should stay as-is.
    backingIndexFor(auth, "User", "byName").indexID shouldBe index2
  }

  "can update unique constraints" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'hi' })")

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name2: String
           |
           |  unique [.name2]
           |
           |  migrations {
           |    move .name -> .name2
           |  }
           |}""".stripMargin
    )
  }
}
