package fauna.model.test

import fauna.repo.values.Value

class FQL2ComparisonSpec extends FQL2Spec {

  val equals = Seq("==" -> true, "!=" -> false)

  "works" in {
    val auth = newDB

    evalOk(auth, "2 == 2") shouldBe Value.True
    evalOk(auth, "2 != 2") shouldBe Value.False
    evalOk(auth, "2 > 2") shouldBe Value.False
    evalOk(auth, "2 < 2") shouldBe Value.False
    evalOk(auth, "2 >= 2") shouldBe Value.True
    evalOk(auth, "2 <= 2") shouldBe Value.True

    evalOk(auth, "2 == 3") shouldBe Value.False
    evalOk(auth, "2 != 3") shouldBe Value.True
    evalOk(auth, "2 > 3") shouldBe Value.False
    evalOk(auth, "2 < 3") shouldBe Value.True
    evalOk(auth, "2 >= 3") shouldBe Value.False
    evalOk(auth, "2 <= 3") shouldBe Value.True
  }

  "unrelated types behavior" in {
    val auth = newAuth

    evalOk(auth, "'a' == 2") shouldBe Value.False
    evalOk(auth, "'a' != 2") shouldBe Value.True

    // Unrelated types always return false
    evalOk(auth, "'a' > 2") shouldBe Value.False
    evalOk(auth, "'a' < 2") shouldBe Value.False
    evalOk(auth, "'a' >= 2") shouldBe Value.False
    evalOk(auth, "'a' <= 2") shouldBe Value.False

    // comparing contains of unrelated types is always allowed.
    evalOk(auth, "['a'] < [2]") shouldBe Value.False
  }

  "array behavior" in {
    val auth = newDB

    equals.foreach {
      case (cmp, expected) => {
        evalOk(auth, s"[] $cmp [2]") shouldBe Value.Boolean(!expected)
        evalOk(auth, s"['a'] $cmp [2]") shouldBe Value.Boolean(!expected)
        evalOk(auth, s"['a', 'b'] $cmp [2]") shouldBe Value.Boolean(!expected)

        evalOk(auth, s"[] $cmp []") shouldBe Value.Boolean(expected)
        evalOk(auth, s"[2] $cmp [2]") shouldBe Value.Boolean(expected)
        evalOk(auth, s"[2, 3] $cmp [2, 3]") shouldBe Value.Boolean(expected)
      }
    }

    // arrays compare like so:
    // - for each element
    //   - if [v] cmp [], returns -1
    //   - if [] cmp [v], returns 1
    //   - compare, if neq, return
    //   - proceed to the next element
    //
    // so effectively, length is compared _after_ the content in the arrays.
    evalOk(auth, "[2] < [2, 3]") shouldBe Value.True
    evalOk(auth, "[2] < [3, 3]") shouldBe Value.True
    evalOk(auth, "[2, 5] < [3, 3]") shouldBe Value.True
    evalOk(auth, "[2, 5] < [3]") shouldBe Value.True

    evalOk(auth, "[3, 5] > [2]") shouldBe Value.True
    evalOk(auth, "[3, 5] > [2, 5]") shouldBe Value.True
    evalOk(auth, "[3, 5] > [3, 4]") shouldBe Value.True
    evalOk(auth, "[4] > [3, 5]") shouldBe Value.True

    // arrays use total ordering
    evalOk(auth, "[1] < ['hi']") shouldBe Value.True
    evalOk(auth, "[1] > ['hi']") shouldBe Value.False
  }

  "object behavior" in {
    val auth = newDB

    equals.foreach {
      case (cmp, expected) => {
        val exp = Value.Boolean(expected)
        val notExp = Value.Boolean(!expected)

        evalOk(auth, s"{} $cmp { a: 2 }") shouldBe notExp
        evalOk(auth, s"{ a: 'foo' } $cmp { a: 2 }") shouldBe notExp
        evalOk(auth, s"{ a: 'foo' } $cmp { b: 'foo' }") shouldBe notExp
        evalOk(auth, s"{ a: 'foo', b: 'b' } $cmp { a: 2 }") shouldBe notExp

        evalOk(auth, s"{} $cmp {}") shouldBe exp
        evalOk(auth, s"{ a: 2 } $cmp { a: 2 }") shouldBe exp
        evalOk(auth, s"{ a: 2, b: 2 } $cmp { a: 2, b: 2 }") shouldBe exp
        evalOk(auth, s"{ a: 2, b: 2 } $cmp { b: 2, a: 2 }") shouldBe exp
      }
    }

    // objects compare like so:
    // - sort all pairs by order of key
    // - do array comparison on said sorted list of values
    evalOk(auth, "{ a: 2 } < { a: 2, b: 3 }") shouldBe Value.True
    evalOk(auth, "{ a: 2 } < { a: 3, b: 3 }") shouldBe Value.True
    evalOk(auth, "{ a: 2, b: 5 } < { a: 3, b: 3 }") shouldBe Value.True
    evalOk(auth, "{ a: 2, b: 5 } < { a: 3 }") shouldBe Value.True

    evalOk(auth, "{ a: 3, b: 5 } > { a: 2 }") shouldBe Value.True
    evalOk(auth, "{ a: 3, b: 5 } > { a: 2, b: 5 }") shouldBe Value.True
    evalOk(auth, "{ a: 3, b: 5 } > { a: 3, b: 4 }") shouldBe Value.True
    evalOk(auth, "{ a: 4 } > { a: 3, b: 5 }") shouldBe Value.True

    evalOk(auth, "{ a: 3 } < { b: 2 }") shouldBe Value.True
    evalOk(auth, "{ a: 2 } < { b: 3 }") shouldBe Value.True
  }

  "strange types" in {
    val auth = newDB

    // total ordering is weird
    evalOk(auth, "Time.now < Time.fromString") shouldBe Value.False
    evalOk(auth, "Time.now > Time.fromString") shouldBe Value.True

    // singletons compare by parent then name
    evalOk(auth, "Collection < Time") shouldBe Value.True
    evalOk(auth, "Collection > Time") shouldBe Value.False

    // lambdas are compared with listCmp(args)
    evalOk(auth, "(a => a) < (x => x)") shouldBe Value.True
    evalOk(auth, "(a => a) > (x => x)") shouldBe Value.False
    evalOk(auth, "((a, b) => a) < (a => a)") shouldBe Value.False
    evalOk(auth, "((a, b) => a) > (a => a)") shouldBe Value.True

    // sets compare by hash code
    evalOk(auth, "Set.sequence(0, 1) < [1].toSet()") shouldBe Value.True
    evalOk(auth, "Set.sequence(0, 1) > [1].toSet()") shouldBe Value.False
    evalOk(auth, "Set.sequence(0, 1) < [2].toSet()") shouldBe Value.False
    evalOk(auth, "Set.sequence(0, 1) > [2].toSet()") shouldBe Value.True

    // set cursors compare by their base64 value
    evalOk(
      auth,
      "Set.sequence(0, 2).paginate(1).after < [1, 2].toSet().paginate(1).after") shouldBe Value.True
    evalOk(
      auth,
      "Set.sequence(0, 2).paginate(1).after > [1, 2].toSet().paginate(1).after") shouldBe Value.False
  }

  "equals and hashCode works" in {
    val auth = newDB

    updateSchemaOk(auth, "main.fsl" -> "collection User {}")

    evalOk(auth, "User.create({ id: 0 })")
    evalOk(auth, "User.create({ id: 1 })")
    evalOk(auth, "User.create({ id: 2 })")

    val values = Seq(
      "null",
      "ID(0)", // An ID
      "1", // An Int
      "0x10ffffffff", // A Long
      "3.0",
      "'hi'",
      "true",
      "Time.now()",
      "Date.today()",
      // TODO: Bytes?
      // TODO: UUID?

      "User(0)", // An existing doc.
      "[1, 2]",
      "{ a: 2, b: 3 }",
      // NB: Lambdas are never equal, as they compare spans.
      // "((x) => x + 1)",
      "Time.now", // A NativeFunc
      "Collection", // A SingletonObject
      "TransactionTime()", // A TransactionTime
      "User.all()", // A Set
      "User.all().paginate(2).after", // A SetCursor
      "User.all().eventsOn(.name)" // An EventSource
    )

    for {
      (a, i) <- values.iterator.zipWithIndex
      (b, j) <- values.iterator.zipWithIndex
    } {
      def check(expr: String, expected: Value) = {
        val res = evalOk(auth, expr)
        if (res != expected) {
          fail(s"Expected $expected, got $res for $expr")
        }
      }

      // Need to eval both in the same query, so that `Time.now()` works.
      val values = evalOk(auth, s"[$a, $b]").asInstanceOf[Value.Array].elems
      val av = values(0)
      val bv = values(1)

      if (i == j) {
        // FQL equals uses `ValueCmp`.
        check(s"$a == $b", Value.True)
        check(s"$a != $b", Value.False)

        // Check that `Value.equals` (which doesn't go through query) gives
        // approximately the same result. This will miss `doc == null` comparisons,
        // which we are specifically not testing here.
        if (av != bv) {
          fail(
            s"Values differ between $a and $b.\n" +
              s"$a -> ${av}\n" +
              s"$b -> ${bv}\n")
        }

        // And check that the hash codes are equal too.
        if (av.hashCode() != bv.hashCode()) {
          fail(
            s"Hash codes differ between $a and $b.\n" +
              s"$a -> ${av}\n" +
              s"$b -> ${bv}\n")
        }
      } else {
        check(s"$a == $b", Value.False)
        check(s"$a != $b", Value.True)

        if (av == bv) {
          fail(
            s"Values are equal, when they should differ between $a and $b.\n" +
              s"$a -> ${av}\n" +
              s"$b -> ${bv}\n")
        }
      }
    }
  }
}
