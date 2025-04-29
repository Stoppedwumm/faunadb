package fauna.api.test

import fauna.codex.json._
import fauna.prop.api.Database
import fauna.prop.Prop

class FQL2PaginationSpec extends FQL2APISpec {

  def mkDocs(db: Database, collname: String, count: Int) = {
    val creates =
      (1 to count)
        .map { i => s"$collname.create({ foo: $i, bar: 'baz' })" }
        .mkString("\n")

    queryOk(
      s"""|$creates
          |$collname.all()
          |  .paginate($count)
          |  .data.map(.id)
          |""".stripMargin,
      db
    ).as[Seq[String]]
  }

  "array pagination" - {
    test("paginates") {
      val db = aDatabase.sample
      val is = (0 until 16 * 3) // 3 pages
      val literal = is.mkString("[", ",", "]")

      val res = queryOk(s"$literal.toSet().paginate()", db)
      val resIDs = (res / "data").as[Seq[JSValue]] map { _.as[Long] }

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual is.take(16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map { _.as[Long] }

      (res2 / "after").isEmpty shouldEqual false
      res2IDs shouldEqual is.drop(16).take(16)

      val curs3 = (res2 / "after").as[String]
      val res3 = queryOk(s"""Set.paginate("$curs3")""", db)
      val res3IDs = (res3 / "data").as[Seq[JSValue]].map { _.as[Long] }

      (res3 / "after").isEmpty shouldEqual true
      res3IDs shouldEqual is.drop(32).take(16)
    }

    test("paginates with size") {
      val db = aDatabase.sample
      val is = (0 until 4)
      val literal = is.mkString("[", ",", "]")

      for (size <- 1 to 5) {
        withClue(s"page size $size") {
          val res0 = queryOk(s"$literal.toSet().paginate(1)", db)
          val resID0 = (res0 / "data").as[Seq[JSValue]] map { _.as[Long] }

          (res0 / "after").isEmpty shouldEqual false
          resID0 shouldEqual is.take(1)

          var seen = 1
          var curs = (res0 / "after").as[String]
          while (seen < is.size) {
            val res = queryOk(s"""Set.paginate("$curs", $size)""", db)
            val resIDs = (res / "data").as[Seq[JSValue]].map { _.as[Long] }

            resIDs shouldEqual is.drop(seen).take(size)
            seen += resIDs.size
            val next = (res / "after")
            next.isEmpty shouldEqual (seen == is.size)
            if (!next.isEmpty) {
              curs = next.as[String]
            }
          }
        }
      }
    }
  }

  "set pagination" - {
    test("page size must be between 0 and 100k") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample

      val zero = queryErr(s"$collName.all().paginate(0)", db)
      (zero / "error" / "code").as[String] should equal("invalid_bounds")

      val negative = queryErr(s"$collName.all().paginate(-1)", db)
      (negative / "error" / "code").as[String] should equal("invalid_bounds")

      val large = queryErr(s"$collName.all().paginate(100001)", db)
      (large / "error" / "code").as[String] should equal("invalid_bounds")
    }

    test("sets materialize as page with cursors") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 16 * 3) // 3 pages

      val res = queryOk(s"$collName.all()", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual docs.take(16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res2 / "after").isEmpty shouldEqual false
      res2IDs shouldEqual docs.drop(16).take(16)

      val curs3 = (res2 / "after").as[String]
      val res3 = queryOk(s"""Set.paginate("$curs3")""", db)
      val res3IDs = (res3 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res3 / "after").isEmpty shouldEqual true
      res3IDs shouldEqual docs.drop(32).take(16)
    }

    test("set pagination is stable in presence of subsequent writes") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 32)

      // snapshot time determined by this query
      val res = queryOk(s"$collName.all()", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      resIDs shouldEqual docs.take(16)

      // create another doc. should not be seen by next page.
      mkDocs(db, collName, 1)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      res2IDs shouldEqual docs.drop(16).take(16)

      (res2 / "after").isEmpty shouldEqual true
    }

    test("set pagination works with closures") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      mkDocs(db, collName, 32)

      val res = queryOk(s"""let fuu = -1; $collName.all().map(.foo * fuu)""", db)
      val resVals = (res / "data").as[Seq[Long]]

      resVals shouldEqual (1 to 16).map { i => i * -1 }

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2Vals = (res2 / "data").as[Seq[Long]]

      res2Vals shouldEqual (17 to 32).map { i => i * -1 }

      (res2 / "after").isEmpty shouldEqual true
    }

    test("set pagination works with bang operator") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      mkDocs(db, collName, 32)

      val res = queryOk(s"""$collName.all().map(.foo!)""", db)
      val resVals = (res / "data").as[Seq[Long]]

      resVals shouldEqual (1 to 16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2Vals = (res2 / "data").as[Seq[Long]]

      res2Vals shouldEqual (17 to 32)

      (res2 / "after").isEmpty shouldEqual true
    }

    test("set pagination works with closed over globals") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 32)

      val res =
        queryOk(s"""$collName.all().where(d => (d == $collName) || true)""", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      resIDs shouldEqual docs.take(16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      res2IDs shouldEqual docs.drop(16).take(16)

      (res2 / "after").isEmpty shouldEqual true
    }

    test("cursor serialization - index") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample

      queryOk(
        s"""|$collName.definition.update({
            |  indexes: {
            |    byBar: {
            |      terms: [{ field: "bar" }]
            |    }
            |  }
            |})""".stripMargin,
        db
      )

      val docs = mkDocs(db, collName, 16 * 3) // 3 pages

      val res = queryOk(s"$collName.byBar('baz')", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual docs.slice(0, 16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res2 / "after").isEmpty shouldEqual false
      res2IDs shouldEqual docs.slice(16, 32)
    }

    test("cursor serialization - take") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 16 * 3) // 3 pages

      val res = queryOk(s"$collName.all().take(20)", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual docs.slice(0, 16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res2 / "after").isEmpty shouldEqual true
      res2IDs shouldEqual docs.slice(16, 20)
    }

    test("cursor serialization - where") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 16 * 3) // 3 pages

      val res = queryOk(s"$collName.all().where(.foo <= 20)", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual docs.slice(0, 16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res2 / "after").isEmpty shouldEqual true
      res2IDs shouldEqual docs.slice(16, 20)
    }

    test("cursor serialization - order") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 16 * 3) // 3 pages

      val res = queryOk(s"$collName.all().order(.foo)", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual docs.slice(0, 16)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])

      (res2 / "after").isEmpty shouldEqual false
      res2IDs shouldEqual docs.slice(16, 32)
    }

    test("cursor serialization - projection") {
      val db = aDatabase.sample
      val collName = aCollection(db).sample
      val docs = mkDocs(db, collName, 16 * 3) // 3 pages

      val res = queryOk(s"$collName.all() { id, null_key }", db)
      val resIDs = (res / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])
      val nulls = (res / "data").as[Seq[JSValue]].map(d => d / "null_key")

      (res / "after").isEmpty shouldEqual false
      resIDs shouldEqual docs.slice(0, 16)
      nulls shouldEqual Seq.fill(16)(JSNull)

      val curs2 = (res / "after").as[String]
      val res2 = queryOk(s"""Set.paginate("$curs2")""", db)
      val res2IDs = (res2 / "data").as[Seq[JSValue]].map(d => (d / "id").as[String])
      val nulls2 = (res2 / "data").as[Seq[JSValue]].map(d => d / "null_key")

      (res2 / "after").isEmpty shouldEqual false
      res2IDs shouldEqual docs.slice(16, 32)
      nulls2 shouldEqual Seq.fill(16)(JSNull)
    }

    test("pagination on indexed objects") {
      val auth = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "address" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }]
            |    }
            |  }
            |})""".stripMargin,
        auth
      )

      queryOk(
        s"""
           |Person.create({
           |  name: "A",
           |  address: {
           |    city: "A",
           |    state: "B"
           |  }
           |})
           |Person.create({
           |  name: "A",
           |  address: {
           |    city: "Y",
           |    state: "Z"
           |  }
           |})
           |""".stripMargin,
        auth
      )

      val res = queryOk(
        """
          |let page = Person.byName("A").paginate(1)
          |page
          |""".stripMargin,
        auth
      )

      val after = (res / "after").as[String]

      val res2 = queryOk(
        s"""
           |Set.paginate("${after}")
           |""".stripMargin,
        auth
      )

      val pageResult = (res2 / "data" / 0)
      (pageResult / "name").as[String] shouldEqual "A"
      (pageResult / "address" / "city").as[String] shouldEqual "Y"
      (pageResult / "address" / "state").as[String] shouldEqual "Z"
    }

    test("pagination on indexed arrays") {
      val auth = aDatabase.sample

      queryOk(
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byName: {
            |      terms: [{ field: "name" }],
            |      values: [{ field: "address" }]
            |    },
            |    byAge: {
            |      terms: [{ field: "age" }]
            |    }
            |  }
            |})""".stripMargin,
        auth
      )

      queryOk(
        s"""
           |Person.create({
           |  name: "A",
           |  address: [
           |    "A",
           |    "B"
           |  ]
           |})
           |Person.create({
           |  name: "A",
           |  address: [
           |    "Y",
           |    "Z"
           |  ]
           |})
           |""".stripMargin,
        auth
      )

      val res = queryOk(
        """
          |let page = Person.byName("A").paginate(1)
          |page
          |""".stripMargin,
        auth
      )

      val after = (res / "after").as[String]

      val res2 = queryOk(
        s"""
           |Set.paginate("${after}")
           |""".stripMargin,
        auth
      )

      val pageResult = (res2 / "data" / 0)
      (pageResult / "name").as[String] shouldEqual "A"
      (pageResult / "address" / 0).as[String] shouldEqual "Y"
      (pageResult / "address" / 1).as[String] shouldEqual "Z"
    }
  }

  test("partial value projection") {
    val auth = aDatabase.sample

    queryOk(
      s"""|Collection.create({
          |  name: "Person",
          |  indexes: {
          |    byName: {
          |      terms: [{ field: "name" }],
          |      values: [{ field: "address.nested" }]
          |    },
          |    byAge: {
          |      terms: [{ field: "age" }]
          |    }
          |  }
          |})""".stripMargin,
      auth
    )

    queryOk(
      s"""|Person.create({
          |  name: "A",
          |  address: {
          |    city: "San Mateo"
          |  },
          |  tree: "green"
          |})
          |""".stripMargin,
      auth
    )

    // Note that .address is a partial value and should be materialized.
    val res =
      queryOk(
        """|Person.byName("A").map(.address)
           |""".stripMargin,
        auth
      )

    (res / "data" / 0 / "city").as[String] shouldEqual "San Mateo"
  }

  test("pagination works with partials in tokens") {
    val auth = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: 'things',
        |  indexes: {
        |    by_color__value_asc: {
        |      terms: [{ field: '.color' }],
        |      values: [{ field: '.value.foo' }]
        |    }
        |  }
        |})
        |""".stripMargin,
      auth
    )

    queryOk(
      """
        |
        |Set.sequence(0, 5).forEach(i => things.create({ value: { foo: i, color: "yellow" }, color: 'yellow' }))
        |Set.sequence(0, 5).forEach(i => things.create({ value: { foo: i, color: "purple" }, color: 'purple' }))
        |""".stripMargin,
      auth
    )

    var nextToken = {
      val res = queryOk(
        """
          |let b = things.by_color__value_asc("yellow") { value }
          |let a = things.by_color__value_asc("purple") { value }
          |a.concat(b).order(.value.foo).pageSize(4)
          |""".stripMargin,
        auth
      )

      val tuples =
        (res / "data").as[Seq[JSValue]].map { jv =>
          ((jv / "value" / "foo").as[Long], (jv / "value" / "color").as[String])
        }

      tuples shouldEqual Seq(
        (0, "purple"),
        (0, "yellow"),
        (1, "purple"),
        (1, "yellow")
      )

      (res / "after")
    }

    nextToken = {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )
      val tuples =
        (res / "data").as[Seq[JSValue]].map { jv =>
          ((jv / "value" / "foo").as[Long], (jv / "value" / "color").as[String])
        }

      tuples shouldEqual Seq(
        (2, "purple"),
        (2, "yellow"),
        (3, "purple"),
        (3, "yellow")
      )

      (res / "after")
    }

    {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )
      val tuples =
        (res / "data").as[Seq[JSValue]].map { jv =>
          ((jv / "value" / "foo").as[Long], (jv / "value" / "color").as[String])
        }

      tuples shouldEqual Seq(
        (4, "purple"),
        (4, "yellow")
      )
    }
  }

  test("pagination works with docs in tokens") {
    val auth = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: 'things',
        |  indexes: {
        |    by_color__value_asc: {
        |      terms: [{ field: '.color' }],
        |      values: [{ field: '.value' }]
        |    }
        |  }
        |})
        |""".stripMargin,
      auth
    )

    queryOk(
      """
        |
        |Set.sequence(0, 5).forEach(i => things.create({ value: i, color: 'yellow' }))
        |Set.sequence(0, 5).forEach(i => things.create({ value: i, color: 'purple' }))
        |""".stripMargin,
      auth
    )

    var nextToken = {
      val res = queryOk(
        """
          |let b = things.by_color__value_asc("yellow")
          |let a = things.by_color__value_asc("purple")
          |a.concat(b).order(.value).pageSize(4)
          |""".stripMargin,
        auth
      )

      val tuples =
        (res / "data").as[Seq[JSValue]].map { jv =>
          ((jv / "value").as[Long], (jv / "color").as[String])
        }

      tuples shouldEqual Seq(
        (0, "purple"),
        (0, "yellow"),
        (1, "purple"),
        (1, "yellow")
      )

      (res / "after")
    }

    nextToken = {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )

      val tuples =
        (res / "data").as[Seq[JSValue]].map { jv =>
          ((jv / "value").as[Long], (jv / "color").as[String])
        }

      tuples shouldEqual Seq(
        (2, "purple"),
        (2, "yellow"),
        (3, "purple"),
        (3, "yellow")
      )

      (res / "after")
    }

    {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )
      val tuples =
        (res / "data").as[Seq[JSValue]].map { jv =>
          ((jv / "value").as[Long], (jv / "color").as[String])
        }

      tuples shouldEqual Seq(
        (4, "purple"),
        (4, "yellow")
      )
    }
  }

  test("pagination works with partials (decorated format)") {
    val auth = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: 'things',
        |  indexes: {
        |    by_color__value_asc: {
        |      terms: [{ field: '.color' }],
        |      values: [{ field: '.value.foo' }]
        |    }
        |  }
        |})
        |""".stripMargin,
      auth
    )

    queryOk(
      """
        |
        |Set.sequence(0, 5).forEach(i => things.create({ value: { foo: i, color: "yellow" }, color: 'yellow' }))
        |Set.sequence(0, 5).forEach(i => things.create({ value: { foo: i, color: "purple" }, color: 'purple' }))
        |""".stripMargin,
      auth
    )

    var nextToken = {
      val res = queryOk(
        """
          |let b = things.by_color__value_asc("yellow") { value }
          |let a = things.by_color__value_asc("purple") { value }
          |a.concat(b).order(.value.foo).take(4)
          |""".stripMargin,
        auth,
        params = FQL2Params(format = Some("decorated"))
      )

      res.as[String] shouldEqual """|{
                         |  data: [
                         |    {
                         |      value: {
                         |        foo: 0,
                         |        color: "purple"
                         |      }
                         |    },
                         |    {
                         |      value: {
                         |        foo: 0,
                         |        color: "yellow"
                         |      }
                         |    },
                         |    {
                         |      value: {
                         |        foo: 1,
                         |        color: "purple"
                         |      }
                         |    },
                         |    {
                         |      value: {
                         |        foo: 1,
                         |        color: "yellow"
                         |      }
                         |    }
                         |  ]
                         |}""".stripMargin

      queryOk(
        """
          |let b = things.by_color__value_asc("yellow") { value }
          |let a = things.by_color__value_asc("purple") { value }
          |a.concat(b).order(.value.foo).pageSize(4).paginate().after
          |""".stripMargin,
        auth,
        params = FQL2Params(format = Some("decorated"))
      ).as[String]
    }

    nextToken = {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken).data
           |""".stripMargin,
        auth,
        params = FQL2Params(format = Some("decorated"))
      )
      res.as[String] shouldEqual """|[
                                    |  {
                                    |    value: {
                                    |      foo: 2,
                                    |      color: "purple"
                                    |    }
                                    |  },
                                    |  {
                                    |    value: {
                                    |      foo: 2,
                                    |      color: "yellow"
                                    |    }
                                    |  },
                                    |  {
                                    |    value: {
                                    |      foo: 3,
                                    |      color: "purple"
                                    |    }
                                    |  },
                                    |  {
                                    |    value: {
                                    |      foo: 3,
                                    |      color: "yellow"
                                    |    }
                                    |  }
                                    |]""".stripMargin

      queryOk(
        s"""
           |Set.paginate($nextToken).after
           |""".stripMargin,
        auth,
        params = FQL2Params(format = Some("decorated"))
      ).as[String]
    }

    {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken).data
           |""".stripMargin,
        auth,
        params = FQL2Params(format = Some("decorated"))
      )
      res.as[String] shouldEqual """|[
                                    |  {
                                    |    value: {
                                    |      foo: 4,
                                    |      color: "purple"
                                    |    }
                                    |  },
                                    |  {
                                    |    value: {
                                    |      foo: 4,
                                    |      color: "yellow"
                                    |    }
                                    |  }
                                    |]""".stripMargin
    }
  }

  test("pagination works with set tokens in set tokens") {
    val auth = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: 't1',
        |})
        |Collection.create({
        |  name: 't2',
        |})
        |""".stripMargin,
      auth
    )
    queryOk(
      """
        |
        |Set.sequence(0, 5).forEach(i => t1.create({ num: i }))
        |Set.sequence(0, 5).forEach(i => t2.create({ num: i }))
        |""".stripMargin,
      auth
    )

    var nextToken = {
      val res = queryOk(
        """
          |t1.all().map(tt => t2.all().paginate(2)).order(.after).pageSize(2)
          |""".stripMargin,
        auth
      )

      (res / "data").as[Seq[JSValue]].flatMap { jv =>
        (jv / "data").as[Seq[JSValue]].map { jvInner =>
          (jvInner / "num").as[Int]
        }
      } shouldEqual Seq(0, 1, 0, 1)

      val after = (res / "data" / 0 / "after")
      val resAfter = queryOk(
        s"""
           |Set.paginate($after)
           |""".stripMargin,
        auth
      )
      (resAfter / "data").as[Seq[JSValue]].map { jv =>
        (jv / "num").as[Int]
      } shouldEqual Seq(2, 3)

      (res / "after")
    }

    nextToken = {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )

      (res / "data").as[Seq[JSValue]].flatMap { jv =>
        (jv / "data").as[Seq[JSValue]].map { jvInner =>
          (jvInner / "num").as[Int]
        }
      } shouldEqual Seq(0, 1, 0, 1)

      val after = (res / "data" / 0 / "after")
      val resAfter = queryOk(
        s"""
           |Set.paginate($after)
           |""".stripMargin,
        auth
      )
      (resAfter / "data").as[Seq[JSValue]].map { jv =>
        (jv / "num").as[Int]
      } shouldEqual Seq(2, 3)

      (res / "after")
    }

    {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )

      (res / "data").as[Seq[JSValue]].flatMap { jv =>
        (jv / "data").as[Seq[JSValue]].map { jvInner =>
          (jvInner / "num").as[Int]
        }
      } shouldEqual Seq(0, 1)

      (res / "after").isEmpty shouldBe true
    }
  }

  test("pagination works with nested sets") {
    val auth = aDatabase.sample
    queryOk(
      """
        |Collection.create({
        |  name: 't1',
        |})
        |Collection.create({
        |  name: 't2',
        |})
        |""".stripMargin,
      auth
    )
    queryOk(
      """
        |
        |Set.sequence(0, 5).forEach(i => t1.create({ num: i }))
        |Set.sequence(0, 20).forEach(i => t2.create({ num: i }))
        |""".stripMargin,
      auth
    )

    var nextToken = {
      val res = queryOk(
        """
          |t1.all().map(tt => t2.all()).order(.count()).pageSize(2)
          |""".stripMargin,
        auth
      )

      (res / "data").as[Seq[JSValue]].flatMap { jv =>
        (jv / "data").as[Seq[JSValue]].map { jvInner =>
          (jvInner / "num").as[Int]
        }
      } shouldEqual Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1,
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

      val after = (res / "data" / 0 / "after")
      val resAfter = queryOk(
        s"""
           |Set.paginate($after)
           |""".stripMargin,
        auth
      )
      (resAfter / "data").as[Seq[JSValue]].map { jv =>
        (jv / "num").as[Int]
      } shouldEqual Seq(16, 17, 18, 19)

      (res / "after")
    }

    nextToken = {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )

      (res / "data").as[Seq[JSValue]].flatMap { jv =>
        (jv / "data").as[Seq[JSValue]].map { jvInner =>
          (jvInner / "num").as[Int]
        }
      } shouldEqual Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1,
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

      val after = (res / "data" / 0 / "after")
      val resAfter = queryOk(
        s"""
           |Set.paginate($after)
           |""".stripMargin,
        auth
      )
      (resAfter / "data").as[Seq[JSValue]].map { jv =>
        (jv / "num").as[Int]
      } shouldEqual Seq(16, 17, 18, 19)

      (res / "after")
    }

    {
      val res = queryOk(
        s"""
           |Set.paginate($nextToken)
           |""".stripMargin,
        auth
      )

      (res / "data").as[Seq[JSValue]].flatMap { jv =>
        (jv / "data").as[Seq[JSValue]].map { jvInner =>
          (jvInner / "num").as[Int]
        }
      } shouldEqual Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)

      (res / "after").isEmpty shouldBe true
    }
  }

  test("pageSize") {
    val auth = aDatabase.sample

    val expected = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")

    val set = queryOk(
      """
        |["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        |  .toSet()
        |  .pageSize(2)
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, set / "@set", expected)
  }

  test("pageSize + where") {
    val auth = aDatabase.sample

    val expected =
      Seq("1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19")

    val before = queryOk(
      """
        |Set.sequence(1, 20)
        |  .toSet()
        |  .map(.toString())
        |  .pageSize(2)
        |  .where(.startsWith("1"))
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |Set.sequence(1, 20)
        |  .toSet()
        |  .map(.toString())
        |  .where(.startsWith("1"))
        |  .pageSize(2)
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + map") {
    val auth = aDatabase.sample

    val expected = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")

    val before = queryOk(
      """
        |Set.sequence(0, 10)
        |  .pageSize(2)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged")))

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |Set.sequence(0, 10)
        |  .map(.toString())
        |  .pageSize(2)
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged")))

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + flatMap") {
    val auth = aDatabase.sample

    val expected = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")

    val before = queryOk(
      """
        |Set.sequence(0, 5)
        |  .pageSize(2)
        |  .flatMap(x => Set.sequence(2*x, 2*x + 2))
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |Set.sequence(0, 5)
        |  .flatMap(x => Set.sequence(2*x, 2*x + 2))
        |  .pageSize(2)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + concat") {
    val auth = aDatabase.sample

    val expected = Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")

    val before = queryOk(
      """
        |Set.sequence(0, 5)
        |  .concat(Set.sequence(5, 10))
        |  .pageSize(2)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    // ConcatSet fallback to ValueSet.DefaultPageSize
    val after = queryOk(
      """
        |Set.sequence(0, 5)
        |  .pageSize(2)
        |  .concat(Set.sequence(5, 10).pageSize(2))
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected, pageSize = 16)
  }

  test("pageSize + reverse") {
    val auth = aDatabase.sample

    val expected = Seq("9", "8", "7", "6", "5", "4", "3", "2", "1", "0")

    val before = queryOk(
      """
        |Set.sequence(0, 10)
        |  .pageSize(2)
        |  .reverse()
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |Set.sequence(0, 10)
        |  .reverse()
        |  .pageSize(2)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + take") {
    val auth = aDatabase.sample

    val expected = Seq("0", "1", "2", "3", "4")

    val before = queryOk(
      """
        |Set.sequence(0, 10)
        |  .pageSize(2)
        |  .take(5)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |Set.sequence(0, 10)
        |  .take(5)
        |  .pageSize(2)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + drop") {
    val auth = aDatabase.sample

    val expected = Seq("5", "6", "7", "8", "9")

    val before = queryOk(
      """
        |Set.sequence(0, 10)
        |  .pageSize(2)
        |  .drop(5)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |Set.sequence(0, 10)
        |  .drop(5)
        |  .pageSize(2)
        |  .map(.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + distinct") {
    val auth = aDatabase.sample

    val expected = Seq("0", "1", "2", "3", "4")

    val before = queryOk(
      """
        |["0", "1", "2", "3", "4", "0", "1", "2", "3", "4"]
        |  .toSet()
        |  .pageSize(2)
        |  .distinct()
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, before / "@set", expected)

    val after = queryOk(
      """
        |["0", "1", "2", "3", "4", "0", "1", "2", "3", "4"]
        |  .toSet()
        |  .distinct()
        |  .pageSize(2)
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, after / "@set", expected)
  }

  test("pageSize + index") {
    val auth = aDatabase.sample
    val coll = aCollection(auth).sample
    val expected = aDocument(auth, coll, Prop.const(JSObject.empty)).times(15).sample

    val set = queryOk(
      s"""
        |$coll.all().pageSize(2).map(.id.toString())
        |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, set / "@set", expected)
  }

  test("pageSize + index + reverse") {
    val auth = aDatabase.sample
    val coll = aCollection(auth).sample
    val expected = aDocument(auth, coll, Prop.const(JSObject.empty)).times(15).sample

    val set = queryOk(
      s"""
         |$coll.all().pageSize(2).reverse().map(.id.toString())
         |""".stripMargin,
      auth,
      FQL2Params(format = Some("tagged"))
    )

    assertPaged(auth, set / "@set", expected.reverse)
  }

  def assertPaged(
    auth: Database,
    page: JSValue,
    expected: Seq[String],
    pageSize: Int = 2): Unit = {
    (page / "data").as[Seq[String]] shouldBe expected.take(pageSize)
    val after = page / "after"

    val nextSet = expected.drop(pageSize)

    if (nextSet.isEmpty) {
      after.isEmpty shouldBe true
    } else {
      after.isEmpty shouldBe false

      val result =
        queryOk(s"Set.paginate($after)", auth, FQL2Params(format = Some("tagged")))
      assertPaged(auth, result, nextSet, pageSize)
    }
  }

}
