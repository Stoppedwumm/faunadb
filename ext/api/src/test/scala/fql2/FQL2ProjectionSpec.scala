package fauna.api.test

import fauna.codex.json._

class FQL2ProjectionSpec extends FQL2APISpec {
  "struct" - {
    test("supports projection on a struct") {
      val db = aDatabase.sample

      val res = queryOk(
        """|
        |let x = {
        |  "foo": "bar",
        |  "fooGone": "bar",
        |  "hello": "friend",
        |  "gentleman": {
        |    "locke": "lamora",
        |    "jean": "tannen",
        |    "other": {
        |      "calo": "galdo",
        |      "bug": "chains"
        |    }
        |  }
        |}
        |x {
        |  hello,
        |  gentleman {
        |    locke,
        |    other {
        |      calo
        |    }
        |  },
        |  foo
        |}""".stripMargin,
        db
      )

      (res / "hello").as[String] shouldEqual "friend"
      (res / "gentleman" / "locke").as[String] shouldEqual "lamora"
      (res / "gentleman" / "other" / "calo").as[String] shouldEqual "galdo"

      (res / "fooGone").isEmpty shouldBe true
      (res / "gentleman" / "jean").isEmpty shouldBe true
      (res / "gentleman" / "other" / "bug").isEmpty shouldBe true
    }

    test("supports rebinding") {
      val db = aDatabase.sample
      val res = queryOk(
        """|
           |let x = { a: 1, b: 10, c: { d: 5, e: 10 } }
           |let y = 100
           |x {
           |  a,
           |  plus: .a + .b,       // `this` is x
           |  plusplus: .a + y,    // `this` is x
           |  cplus: .c.d + .c.e,  // `this` is x
           |  cplusplus: .c.d + y, // `this` is x
           |  c {
           |    plus: .d + .e,     // `this` is x.c
           |    plusplus: .d + y   // `this` is x.c
           |  },
           |  cc: .c {
           |    plus: .d + .e,     // `this` is x.c
           |    plusplus: .d + y   // `this` is x.c
           |  }
           |}""".stripMargin,
        db
      )

      res shouldEqual JSObject(
        "a" -> 1,
        "plus" -> 11,
        "plusplus" -> 101,
        "cplus" -> 15,
        "cplusplus" -> 105,
        "c" -> JSObject("plus" -> 15, "plusplus" -> 105),
        "cc" -> JSObject("plus" -> 15, "plusplus" -> 105)
      )
    }

    test("returns nulls when requesting keys that aren't present") {
      val db = aDatabase.sample
      val res = queryOk(
        """|
        |let x = {
        |  "foo": "bar",
        |}
        |x {
        |  foo,
        |  hello
        |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      (res / "foo").as[String] shouldEqual "bar"
      (res / "hello") shouldBe a[JSNull]
    }
  }
  "array" - {
    test("supports projection on arrays") {
      val db = aDatabase.sample

      val res = queryOk(
        """|
         |let y = [
         |  {
         |    "hi": "low",
         |    "whoa": "now"
         |  },
         |  {
         |    "hi": "bye",
         |    "no": "sir"
         |  },
         |  {
         |    "hi": "howdy"
         |  }
         |]
         |y {
         |  hi
         |}
      |""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      ).as[JSArray].value

      res.size shouldEqual 3
      res foreach { v =>
        v.as[JSObject].keys shouldEqual Seq("hi")
      }

      res map { v => (v / "hi").as[String] } shouldEqual Seq("low", "bye", "howdy")
    }
  }
  "document" - {
    test("supports projection on documents") {
      val db = aDatabase.sample
      val collection = aCollection(db).sample
      val res = queryOk(
        s"""|
        |let author = $collection.create({
        |  "foo": "bar",
        |  "seattle": "wa",
        |})
        |let projectedId = author { id }
        |let readAuthor = $collection.byId(projectedId.id)
        |readAuthor {
        |  id,
        |  hello,
        |  nested {
        |    key
        |  },
        |  foo
        |}""".stripMargin,
        db
      )

      (res / "id") shouldBe a[JSString]
      (res / "foo").as[String] shouldEqual "bar"
      (res / "hello") shouldBe a[JSNull]
      (res / "nested") shouldBe a[JSNull]
      (res / "seattle").isEmpty shouldBe true
    }
  }
  "primitive types" - {
    test("ints are passed through") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|
        |3 {
        |  hello,
        |  nested {
        |    key
        |  }
        |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      res.as[Int] shouldEqual 3
    }
    test("strings are passed through") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|
        |"goodbye" {
        |  hello,
        |  nested {
        |    key
        |  }
        |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      res.as[String] shouldEqual "goodbye"
    }
    test("doubles are passed through") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|
        |1.1 {
        |  hello,
        |  nested {
        |    key
        |  }
        |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      res shouldBe a[JSDouble]
    }
    test("booleans are passed through") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|
        |true {
        |  hello,
        |  nested {
        |    key
        |  }
        |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      res.as[Boolean] shouldEqual true
    }
    test("nulls short-circuit") {
      val db = aDatabase.sample
      val res = queryOk(
        s"""|null {
            |  hello,
            |  nested {
            |    key
            |  }
            |}""".stripMargin,
        db,
        FQL2Params(typecheck = Some(false))
      )

      res shouldBe a[JSNull]
    }
  }
}
